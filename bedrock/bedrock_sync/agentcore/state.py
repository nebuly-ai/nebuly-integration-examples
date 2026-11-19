"""What the AgentCore sync remembers between runs, in one SQLite file.

A database belongs to one source (account, region, runtime endpoint) and one
set of log groups. It keeps:

- the CloudWatch coverage: the reading windows and the time range read in
  full for every log group;
- the raw records read, until their trace is delivered and the content
  retention has passed; then only a digest stays, so a record read again is
  recognized. After the history retention the digest and the trace are
  deleted too;
- the traces being assembled, with their status: ``open`` (spans may still
  arrive), ``blocked`` (no actor, no content or no single root), ``ready`` (staged for
  delivery) or ``dropped`` (expired before it could be sent);
- the outbox, shared with the runtime connector (``shared/outbox.py``), with a
  link from each interaction to its trace.

Callers persist every page of a window before completing it. No network here.
A lock file prevents two runs on the same database; a dry run works on an
in-memory copy (``Store`` in ``shared/store.py``).
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from bedrock_sync.shared import outbox
from bedrock_sync.shared.store import StateError, Store, validate_ms

from .assembler import BLOCKING
from .parser import explicitly_foreign
from .records import LogPage, RawLogRecord, SourceScope

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from pathlib import Path

    from bedrock_sync.shared.delivery import Delivery

TraceStatus = Literal["open", "blocked", "ready", "dropped"]
__all__ = ["AgentCoreState", "StateError", "TraceRow", "TraceStatus"]

_RECONCILIATION = "reconciliation"
_DELIVERABLE_TRACE = (
    "EXISTS (SELECT 1 FROM trace_outbox l JOIN traces t "
    "USING(trace_id) WHERE l.interaction_key=outbox.interaction_key "
    "AND t.status='ready' "
    "AND CAST(COALESCE(l.end_ns,json_extract(t.snapshot,'$.end_ns')) AS INTEGER) < ?)"
)
_SCHEMA = """
CREATE TABLE metadata(
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  source_key TEXT NOT NULL,
  log_groups TEXT NOT NULL,
  coverage_start INTEGER,
  coverage_end INTEGER);
CREATE TABLE windows(
  id INTEGER PRIMARY KEY,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER NOT NULL,
  completed INTEGER NOT NULL DEFAULT 0);
CREATE TABLE window_groups(
  window_id INTEGER NOT NULL REFERENCES windows(id),
  log_group TEXT NOT NULL,
  completed INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(window_id, log_group));
CREATE TABLE records(
  record_key TEXT PRIMARY KEY,
  log_group TEXT NOT NULL,
  log_stream TEXT NOT NULL,
  event_id TEXT NOT NULL,
  timestamp_ms INTEGER NOT NULL,
  ingestion_time_ms INTEGER NOT NULL,
  message TEXT NOT NULL,
  processed INTEGER NOT NULL DEFAULT 0,
  unsupported INTEGER NOT NULL DEFAULT 0,
  trace_id TEXT);
CREATE INDEX records_trace ON records(trace_id);
CREATE INDEX records_unprocessed ON records(timestamp_ms) WHERE processed=0;
CREATE TABLE record_receipts(
  record_key TEXT PRIMARY KEY,
  digest TEXT NOT NULL,
  timestamp_ms INTEGER NOT NULL);
CREATE INDEX record_receipts_time ON record_receipts(timestamp_ms);
CREATE TABLE traces(
  trace_id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK(status IN ('open','blocked','ready','dropped')),
  earliest_ms INTEGER NOT NULL,
  snapshot TEXT NOT NULL);
CREATE INDEX traces_status ON traces(status, earliest_ms);
CREATE TABLE trace_outbox(
  interaction_key TEXT PRIMARY KEY REFERENCES outbox(interaction_key),
  trace_id TEXT NOT NULL,
  root_span_id TEXT NOT NULL,
  end_ns INTEGER);
CREATE INDEX trace_outbox_trace ON trace_outbox(trace_id);
CREATE TABLE recovered_prefixes(
  trace_id TEXT PRIMARY KEY,
  start_ms INTEGER NOT NULL);
CREATE TABLE compacted_traces(trace_id TEXT PRIMARY KEY);
CREATE TABLE parser_settings(
  name TEXT PRIMARY KEY,
  value TEXT NOT NULL);
"""
_RECORD_COLUMNS = "log_group,log_stream,event_id,timestamp_ms,ingestion_time_ms,message"


@dataclass(frozen=True)
class TraceRow:
    trace_id: str
    status: TraceStatus
    earliest_ms: int
    snapshot: dict[str, object] = field(repr=False)


def _json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _blocking_reasons(status: str, snapshot: dict[str, object]) -> set[str]:
    """Why a trace still waits, from its snapshot, for the expiry warning."""
    if snapshot.get("conversion_error"):
        return {"conversion_failed"}
    if snapshot.get("outside_run_end"):
        return {"outside_run_end"}
    diagnostics = snapshot.get("diagnostics")
    codes = (
        {str(item.get("code")) for item in diagnostics if isinstance(item, dict)}
        if isinstance(diagnostics, list)
        else set()
    )
    if blocking := codes & BLOCKING:
        return blocking
    return {"agent_span_not_ended" if status == "open" else "not_assembled"}


def _receipt(values: tuple[object, ...]) -> str:
    return hashlib.sha256(_json(values).encode()).hexdigest()


class AgentCoreState(Store):
    LABEL = "AgentCore"
    APPLICATION_ID = 0x41474331
    SCHEMA_VERSION = 1

    def __init__(
        self,
        path: Path,
        source: SourceScope,
        log_groups: tuple[str, ...],
        *,
        dry_run: bool = False,
        now_ms: int | None = None,
    ) -> None:
        if not log_groups or any(not group.strip() for group in log_groups):
            raise ValueError("At least one nonblank log group is required")
        self.source = source
        self.log_groups = tuple(sorted(set(log_groups)))
        super().__init__(path, dry_run=dry_run, now_ms=now_ms)

    def _initialize(self) -> None:
        if self._schema_exists():
            row = self._db.execute(
                "SELECT source_key,log_groups FROM metadata WHERE singleton=1"
            ).fetchone()
            if row is None or tuple(row) != (self.source.key, _json(self.log_groups)):
                raise StateError("Database source or log-group configuration differs")
            return
        self._create_schema(
            _SCHEMA,
            initialize=(
                "INSERT INTO metadata VALUES (1,?,?,NULL,NULL)",
                (self.source.key, _json(self.log_groups)),
            ),
        )

    # Settings and bookkeeping ---------------------------------------------------

    def configure_parser(self, namespaces: tuple[str, ...]) -> None:
        """Remember the actor namespaces; when they change, records skipped as
        unsupported are parsed again, since they may now name the actor."""
        namespace = "\n".join(namespaces)
        row = self._db.execute(
            "SELECT value FROM parser_settings WHERE name='namespace'"
        ).fetchone()
        with self._transaction():
            if row is None or row[0] != namespace:
                self._db.execute(
                    "UPDATE records SET processed=0,unsupported=0 WHERE unsupported=1 "
                    "AND (trace_id IS NULL OR trace_id NOT IN "
                    "(SELECT trace_id FROM compacted_traces))"
                )
            self._db.execute(
                "INSERT INTO parser_settings VALUES ('namespace',?) ON CONFLICT(name) "
                "DO UPDATE SET value=excluded.value",
                (namespace,),
            )

    def last_reconciliation(self) -> int | None:
        row = self._db.execute(
            "SELECT value FROM operations WHERE name=?", (_RECONCILIATION,)
        ).fetchone()
        return None if row is None else int(row[0])

    def mark_reconciliation(self, end_ms: int) -> None:
        self._db.execute(
            "INSERT INTO operations VALUES (?,?) ON CONFLICT(name) "
            "DO UPDATE SET value=MAX(value,excluded.value)",
            (_RECONCILIATION, end_ms),
        )

    def recovered_start(self, trace_id: str) -> int | None:
        row = self._db.execute(
            "SELECT start_ms FROM recovered_prefixes WHERE trace_id=?", (trace_id,)
        ).fetchone()
        return None if row is None else int(row[0])

    def mark_recovered(self, trace_id: str, start_ms: int) -> None:
        validate_ms(start_ms)
        self._db.execute(
            "INSERT INTO recovered_prefixes VALUES (?,?) ON CONFLICT(trace_id) "
            "DO UPDATE SET start_ms=MIN(start_ms,excluded.start_ms)",
            (trace_id, start_ms),
        )

    def is_compacted(self, trace_id: str) -> bool:
        return (
            self._db.execute(
                "SELECT 1 FROM compacted_traces WHERE trace_id=?", (trace_id,)
            ).fetchone()
            is not None
        )

    # Coverage and records ---------------------------------------------------------

    @property
    def coverage(self) -> tuple[int, int] | None:
        row = self._db.execute(
            "SELECT coverage_start,coverage_end FROM metadata WHERE singleton=1"
        ).fetchone()
        return None if row[0] is None else (row[0], row[1])

    def begin_window(self, start_ms: int, end_ms: int) -> int:
        validate_ms(start_ms, end_ms)
        if start_ms >= end_ms:
            raise ValueError("A reading window must have positive duration")
        self._check_no_gap(start_ms, end_ms)
        with self._transaction():
            cursor = self._db.execute(
                "INSERT INTO windows(start_ms,end_ms) VALUES (?,?)", (start_ms, end_ms)
            )
            if cursor.lastrowid is None:
                raise StateError("SQLite did not return an inserted row ID")
            window_id = cursor.lastrowid
            self._db.executemany(
                "INSERT INTO window_groups(window_id,log_group) VALUES (?,?)",
                ((window_id, group) for group in self.log_groups),
            )
        return window_id

    def _check_no_gap(self, start_ms: int, end_ms: int) -> None:
        coverage = self.coverage
        if coverage is not None and (start_ms > coverage[1] or end_ms < coverage[0]):
            raise StateError("Reading window would create a coverage gap")

    def ingest_page(self, window_id: int, page: LogPage) -> int:
        if page.trace_id is not None:
            raise StateError("Filtered trace pages cannot establish coverage")
        validate_ms(page.start_ms, page.end_ms)
        with self._transaction():
            row = self._db.execute(
                "SELECT w.start_ms,w.end_ms,w.completed,g.completed FROM "
                "windows w JOIN window_groups g ON g.window_id=w.id WHERE "
                "w.id=? AND g.log_group=?",
                (window_id, page.log_group),
            ).fetchone()
            if row is None or (row[0], row[1]) != (page.start_ms, page.end_ms):
                raise StateError("Page does not belong to this reading window")
            if row[2] or row[3]:
                raise StateError("Reading window or log group is already complete")
            inserted = self._insert_records(page)
            if page.complete:
                self._db.execute(
                    "UPDATE window_groups SET completed=1 WHERE window_id=? "
                    "AND log_group=?",
                    (window_id, page.log_group),
                )
            return inserted

    def ingest_recovery_page(self, page: LogPage) -> int:
        """Save records read again for one trace, without extending coverage."""
        validate_ms(page.start_ms, page.end_ms)
        if page.log_group not in self.log_groups or page.start_ms >= page.end_ms:
            raise StateError("Recovery page has an invalid group or window")
        with self._transaction():
            return self._insert_records(page)

    def _insert_records(self, page: LogPage) -> int:
        inserted = 0
        for record in page.records:
            validate_ms(record.timestamp_ms, record.ingestion_time_ms)
            if (
                record.log_group != page.log_group
                or not page.start_ms <= record.timestamp_ms < page.end_ms
            ):
                raise StateError("Record lies outside its page group or window")
            if explicitly_foreign(record, self.source):
                continue
            key = self.source.record_key(record.log_group, record.event_id)
            values = (
                record.log_group,
                record.log_stream,
                record.event_id,
                record.timestamp_ms,
                record.ingestion_time_ms,
                record.message,
            )
            receipt = self._db.execute(
                "SELECT digest FROM record_receipts WHERE record_key=?", (key,)
            ).fetchone()
            if receipt is not None:
                if receipt[0] != _receipt(values):
                    raise StateError("Conflicting copies of one compacted record")
                continue
            existing = self._db.execute(
                f"SELECT {_RECORD_COLUMNS} FROM records WHERE record_key=?",  # noqa: S608
                (key,),
            ).fetchone()
            if existing is not None:
                if tuple(existing) != values:
                    raise StateError("Conflicting copies of one CloudWatch record")
                continue
            self._db.execute(
                f"INSERT INTO records(record_key,{_RECORD_COLUMNS}) "  # noqa: S608
                "VALUES (?,?,?,?,?,?,?)",
                (key, *values),
            )
            inserted += 1
        return inserted

    def complete_window(self, window_id: int) -> None:
        with self._transaction():
            row = self._db.execute(
                "SELECT start_ms,end_ms,completed FROM windows WHERE id=?", (window_id,)
            ).fetchone()
            if row is None:
                raise StateError("Unknown reading window")
            if row[2]:
                return
            if self._db.execute(
                "SELECT COUNT(*) FROM window_groups WHERE window_id=? AND completed=0",
                (window_id,),
            ).fetchone()[0]:
                raise StateError("Not every configured log group has completed")
            start, end = row[0], row[1]
            self._check_no_gap(start, end)
            if (coverage := self.coverage) is not None:
                start, end = min(start, coverage[0]), max(end, coverage[1])
            self._db.execute(
                "UPDATE metadata SET coverage_start=?,coverage_end=? WHERE singleton=1",
                (start, end),
            )
            self._db.execute("UPDATE windows SET completed=1 WHERE id=?", (window_id,))

    def records(
        self, *, trace_id: str | None = None, unprocessed_only: bool = False
    ) -> tuple[RawLogRecord, ...]:
        clauses = []
        parameters: list[str] = []
        if trace_id is not None:
            clauses.append("trace_id=?")
            parameters.append(trace_id)
        if unprocessed_only:
            clauses.append("processed=0")
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._db.execute(
            f"SELECT {_RECORD_COLUMNS} FROM records"  # noqa: S608
            + where
            + " ORDER BY timestamp_ms,log_group,event_id",
            parameters,
        )
        return tuple(RawLogRecord(*row) for row in rows)

    def unprocessed_records(self) -> Iterator[RawLogRecord]:
        """Read batches while the caller marks each returned record processed."""
        while rows := self._db.execute(
            f"SELECT {_RECORD_COLUMNS} FROM records WHERE processed=0 "  # noqa: S608
            "ORDER BY timestamp_ms,log_group,event_id LIMIT 512"
        ).fetchall():
            for row in rows:
                yield RawLogRecord(*row)

    def mark_processed(
        self,
        record: RawLogRecord,
        *,
        trace_id: str | None,
        unsupported: bool = False,
    ) -> None:
        """The record was parsed: link it to its trace. ``unsupported`` records
        are parsed again if the actor namespace changes."""
        with self._transaction():
            cursor = self._db.execute(
                "UPDATE records SET processed=1,unsupported=?,trace_id=? "
                "WHERE record_key=?",
                (
                    int(unsupported),
                    trace_id,
                    self.source.record_key(record.log_group, record.event_id),
                ),
            )
            if cursor.rowcount != 1:
                raise StateError("Cannot process a record that has not been ingested")

    # Traces and sessions ----------------------------------------------------------

    def save_trace(
        self,
        trace_id: str,
        status: TraceStatus,
        earliest_ms: int,
        snapshot: Mapping[str, object],
    ) -> None:
        validate_ms(earliest_ms)
        if status not in {"open", "blocked", "ready", "dropped"}:
            raise ValueError("Invalid trace status")
        with self._transaction():
            self._db.execute(
                "INSERT INTO traces VALUES (?,?,?,?) ON CONFLICT(trace_id) "
                "DO UPDATE SET status=excluded.status,"
                "earliest_ms=MIN(traces.earliest_ms,excluded.earliest_ms),"
                "snapshot=excluded.snapshot",
                (trace_id, status, earliest_ms, _json(dict(snapshot))),
            )

    def trace(self, trace_id: str) -> TraceRow | None:
        rows = self._traces("trace_id=?", (trace_id,))
        return rows[0] if rows else None

    def pending_traces(self) -> tuple[TraceRow, ...]:
        """Traces still to assemble: open or blocked. Dropped ones wait for new
        records, which mark them blocked again."""
        return self._traces("status IN ('open','blocked')")

    def ready_traces(self, *, unattempted_only: bool = False) -> tuple[TraceRow, ...]:
        """Ready snapshots, optionally only those no POST was attempted for.

        These are assembly results, not permission to send: the outbox stays
        the authority on what was sent, rejected or waits for a retry.
        """
        condition = "status='ready'"
        if unattempted_only:
            condition += (
                " AND NOT EXISTS (SELECT 1 FROM trace_outbox l JOIN outbox o "
                "USING(interaction_key) WHERE l.trace_id=traces.trace_id "
                "AND o.status!='pending')"
            )
        return self._traces(condition)

    def _traces(
        self, condition: str, parameters: tuple[object, ...] = ()
    ) -> tuple[TraceRow, ...]:
        rows = self._db.execute(
            "SELECT trace_id,status,earliest_ms,snapshot FROM traces WHERE "  # noqa: S608
            + condition
            + " ORDER BY earliest_ms,trace_id",
            parameters,
        )
        return tuple(
            TraceRow(row[0], row[1], row[2], json.loads(row[3])) for row in rows
        )

    # Outbox -----------------------------------------------------------------------

    def put_payload(
        self,
        trace_id: str,
        root_span_id: str,
        payload: Mapping[str, object],
        *,
        end_ns: int | None = None,
    ) -> str:
        key = self.source.interaction_key(trace_id, root_span_id)
        with self._transaction():
            outbox.put(self._db, key, payload)
            self._db.execute(
                "INSERT INTO trace_outbox VALUES (?,?,?,?) "
                "ON CONFLICT(interaction_key) DO UPDATE SET end_ns=excluded.end_ns",
                (key, trace_id, root_span_id, end_ns),
            )
        return key

    def supersede_roots(self, trace_id: str, root_span_id: str) -> int:
        """Another root of this trace was staged before: a sub-agent seen
        before its caller. Pending or given-up payloads are removed; returns
        how many Nebuly may already hold."""
        with self._transaction():
            rows = self._db.execute(
                "SELECT l.interaction_key,o.status,EXISTS(SELECT 1 FROM attempts a "
                "WHERE a.interaction_key=l.interaction_key), "
                "o.status='rejected' AND p.failing_since_ms IS NOT NULL "
                "FROM trace_outbox l JOIN outbox o USING(interaction_key) "
                "JOIN delivery_policy p USING(interaction_key) "
                "WHERE l.trace_id=? AND l.root_span_id!=?",
                (trace_id, root_span_id),
            ).fetchall()
            pending = [
                key
                for key, status, _, gave_up in rows
                if status == "pending" or gave_up
            ]
            self._db.executemany(
                "DELETE FROM trace_outbox WHERE interaction_key=?",
                [(key,) for key in pending],
            )
            outbox.delete(self._db, pending)
        return sum(
            1
            for _, status, tried, gave_up in rows
            if status == "sent" or gave_up or (status == "pending" and tried)
        )

    def requeue_payload(
        self, interaction_key: str, payload: Mapping[str, object], *, end_ns: int | None
    ) -> None:
        """Send a delivered trace again with a corrected payload."""
        with self._transaction():
            outbox.requeue(self._db, interaction_key, payload)
            self._db.execute(
                "UPDATE trace_outbox SET end_ns=? WHERE interaction_key=?",
                (end_ns, interaction_key),
            )

    def pending_deliveries(
        self, *, ended_before_ns: int | None = None
    ) -> tuple[Delivery, ...]:
        """Pending payloads; with ``ended_before_ns`` only those of ready traces
        that ended before it."""
        if ended_before_ns is None:
            return outbox.pending(self._db, self.now_ms)
        return outbox.pending(
            self._db,
            self.now_ms,
            where=_DELIVERABLE_TRACE,
            parameters=(ended_before_ns,),
        )

    def delivery_waiting_for_eligible_retry(self, *, ended_before_ns: int) -> bool:
        return outbox.waiting_for_retry(
            self._db,
            self.now_ms,
            where=_DELIVERABLE_TRACE,
            parameters=(ended_before_ns,),
        )

    # Retention --------------------------------------------------------------------

    def expire_traces(self, before_ms: int) -> tuple[int, Counter[str], int]:
        """Drop the open or blocked traces that started before ``before_ms``:
        no later read will complete them, so retention can erase them. Returns
        failures and reasons, and the count of unrelated rootless traces."""
        validate_ms(before_ms)
        reasons: Counter[str] = Counter()
        ignored = 0
        with self._transaction():
            rows = self._db.execute(
                "SELECT status,snapshot FROM traces "
                "WHERE status IN ('open','blocked') AND earliest_ms<?",
                (before_ms,),
            ).fetchall()
            for status, snapshot in rows:
                data = json.loads(snapshot)
                if data.get("unrelated_rootless"):
                    ignored += 1
                else:
                    reasons.update(_blocking_reasons(status, data))
            self._db.execute(
                "UPDATE traces SET status='dropped',snapshot='{\"expired\":true}' "
                "WHERE status IN ('open','blocked') AND earliest_ms<?",
                (before_ms,),
            )
        return len(rows) - ignored, reasons, ignored

    def prune_content(self, retention_seconds: int) -> int:
        """Erase the content of traces done before the retention: delivered
        (sent or rejected) or dropped. A digest of each record stays, so a
        record read again is recognized and not processed twice. Returns the
        number of records erased."""
        cutoff = self._retention_cutoff(retention_seconds)
        with self._transaction():
            delivered = self._db.execute(
                "SELECT l.trace_id FROM trace_outbox l "
                "JOIN traces t USING(trace_id) "
                "JOIN outbox o USING(interaction_key) "
                "JOIN delivery_policy p USING(interaction_key) "
                "WHERE t.status='ready' "
                "AND l.trace_id NOT IN (SELECT trace_id FROM compacted_traces) "
                "GROUP BY l.trace_id HAVING COUNT(*)=COUNT(CASE "
                "WHEN o.status IN ('sent','rejected') AND p.finalized_ms<=? "
                "THEN 1 END)",
                (cutoff,),
            ).fetchall()
            for (trace_id,) in delivered:
                self._compact_trace(trace_id, "ready")
                self._db.execute(
                    "UPDATE outbox SET payload='{}' WHERE interaction_key IN "
                    "(SELECT interaction_key FROM trace_outbox WHERE trace_id=?)",
                    (trace_id,),
                )
            dropped = self._db.execute(
                "SELECT trace_id FROM traces WHERE status='dropped' AND earliest_ms<=? "
                "AND trace_id NOT IN (SELECT trace_id FROM compacted_traces)",
                (cutoff,),
            ).fetchall()
            for (trace_id,) in dropped:
                self._compact_trace(trace_id, "dropped")
                # A payload staged before the trace was dropped is never sent.
                self._db.execute(
                    "UPDATE outbox SET payload='{}' WHERE status='pending' AND "
                    "interaction_key IN (SELECT interaction_key FROM trace_outbox "
                    "WHERE trace_id=?)",
                    (trace_id,),
                )
            # An expired correction can still have older finalized deliveries.
            # Keep those payloads only until their own content retention ends.
            self._db.execute(
                "UPDATE outbox SET payload='{}' WHERE payload!='{}' "
                "AND status IN ('sent','rejected') AND interaction_key IN ("
                "SELECT l.interaction_key FROM trace_outbox l "
                "JOIN traces t USING(trace_id) JOIN compacted_traces USING(trace_id) "
                "JOIN delivery_policy p USING(interaction_key) "
                "WHERE t.status='dropped' AND p.finalized_ms<=?)",
                (cutoff,),
            )
            return self._erase_records(cutoff)

    def prune_history(self, retention_seconds: int) -> int:
        """Forget compacted traces that started before the retention, with
        their outbox rows, the digests of their records and the old reading
        windows. A backfill over that time sends them again, and Nebuly
        updates the same interaction. Returns how many traces."""
        cutoff = self._retention_cutoff(retention_seconds)
        with self._transaction():
            traces = [
                trace_id
                for (trace_id,) in self._db.execute(
                    "SELECT t.trace_id FROM traces t JOIN compacted_traces "
                    "USING(trace_id) WHERE t.earliest_ms<=? AND NOT EXISTS "
                    "(SELECT 1 FROM trace_outbox l JOIN outbox o "
                    "USING(interaction_key) JOIN delivery_policy p "
                    "USING(interaction_key) WHERE l.trace_id=t.trace_id AND "
                    "(o.status='sending' "
                    "OR (o.status='pending' AND t.status!='dropped') "
                    "OR (o.status IN ('sent','rejected') AND p.finalized_ms>?)))",
                    (cutoff, cutoff),
                )
            ]
            for trace_id in traces:
                keys = [
                    key
                    for (key,) in self._db.execute(
                        "SELECT interaction_key FROM trace_outbox WHERE trace_id=?",
                        (trace_id,),
                    )
                ]
                self._db.execute(
                    "DELETE FROM trace_outbox WHERE trace_id=?", (trace_id,)
                )
                outbox.delete(self._db, keys)
                for table in (
                    "recovered_prefixes",
                    "compacted_traces",
                    "traces",
                ):
                    self._db.execute(
                        f"DELETE FROM {table} WHERE trace_id=?",  # noqa: S608
                        (trace_id,),
                    )
            self._db.execute(
                "DELETE FROM record_receipts WHERE timestamp_ms<=?", (cutoff,)
            )
            self._db.execute(
                "DELETE FROM window_groups WHERE window_id IN "
                "(SELECT id FROM windows WHERE completed=1 AND end_ms<=?)",
                (cutoff,),
            )
            self._db.execute(
                "DELETE FROM windows WHERE completed=1 AND end_ms<=?", (cutoff,)
            )
        return len(traces)

    def _compact_trace(self, trace_id: str, status: TraceStatus) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO compacted_traces VALUES (?)", (trace_id,)
        )
        self._db.execute(
            "UPDATE traces SET status=?,snapshot='{\"compacted\":true}' "
            "WHERE trace_id=?",
            (status, trace_id),
        )

    def _erase_records(self, cutoff: int) -> int:
        """Records of compacted traces, and old ones that belong to no trace."""
        rows = self._db.execute(
            f"SELECT record_key,{_RECORD_COLUMNS} FROM records "  # noqa: S608
            "WHERE processed=1 AND (trace_id IN "
            "(SELECT trace_id FROM compacted_traces) OR (timestamp_ms<=? AND "
            "NOT EXISTS (SELECT 1 FROM traces t "
            "WHERE t.trace_id=records.trace_id)))",
            (cutoff,),
        ).fetchall()
        for row in rows:
            self._db.execute(
                "INSERT OR IGNORE INTO record_receipts VALUES (?,?,?)",
                (row[0], _receipt(tuple(row)[1:]), row[4]),
            )
            self._db.execute("DELETE FROM records WHERE record_key=?", (row[0],))
        return len(rows)
