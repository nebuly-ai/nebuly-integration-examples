"""What the runtime sync remembers between runs, in one SQLite file.

One database per log source (bucket, prefix, account and region). It keeps:

- the S3 cursor: the covered range, the last key read and a bounded set of
  recently read keys so the overlapping listings do not download them again;
- the calls of interactions still open, as pointers only (S3 key and request
  id): they are read again from S3, which keeps logs without a time limit, so
  no conversation text sits on disk while an interaction is open;
- every closed turn (digest of conversation, user and interaction id) with
  the outcome: queued, or why it was skipped. Kept for the history retention,
  so late calls do not reopen a closed turn; a backfill over older time sends
  the interaction again and Nebuly updates it;
- the outbox, with the delivery rules shared with AgentCore. Payloads of
  sent or rejected interactions are compacted after the content retention
  and deleted after the history retention.

``checkpoint`` writes the cursor, the open pointers, the closed interactions
and their payloads in one transaction: after a crash the next run starts from
the last checkpoint and rebuilds the same interactions. A lock file prevents
two runs on the same database; a dry run works on an in-memory copy
(``Store`` in ``shared/store.py``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from bedrock_sync.shared import outbox
from bedrock_sync.shared.store import StateError, Store
from bedrock_sync.shared.time_utils import timestamp_str_to_datetime

from .grouping import CallRef

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

__all__ = [
    "CONVERSION_ERROR",
    "QUEUED",
    "Closure",
    "Cursor",
    "RuntimeState",
    "StateError",
]

QUEUED = "queued"
CONVERSION_ERROR = "conversion_error"
"""Closed until a backfill reads the original first request again."""

_SCHEMA = """
CREATE TABLE metadata(
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  log_source TEXT NOT NULL,
  coverage_from TEXT,
  coverage_until TEXT,
  last_key TEXT);
CREATE TABLE pending_call(
  request_id TEXT PRIMARY KEY,
  s3_key TEXT NOT NULL,
  turn_key TEXT NOT NULL);
CREATE TABLE read_object(
  s3_key TEXT PRIMARY KEY,
  delivered_at TEXT NOT NULL);
CREATE INDEX read_object_time ON read_object(delivered_at);
CREATE TABLE closed_interaction(
  turn_key TEXT PRIMARY KEY,
  closed_at TEXT NOT NULL,
  outcome TEXT NOT NULL,
  first_request_id TEXT,
  first_s3_key TEXT);
"""


@dataclass(frozen=True)
class Cursor:
    """How far the logs were read.

    ``coverage_until`` is the latest delivery time covered by this read, and
    ``last_key`` is the latest key read.
    """

    coverage_from: datetime
    coverage_until: datetime
    last_key: str | None


@dataclass(frozen=True)
class Closure:
    """A closed interaction: ``payload`` is None when it was skipped, and
    ``outcome`` then says why."""

    turn_key: str
    outcome: str
    payload: Mapping[str, object] | None = None
    first_call: CallRef | None = None


def _ts(value: datetime) -> str:
    """Fixed width, so text order is time order."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="microseconds")


class RuntimeState(Store):
    LABEL = "runtime"
    APPLICATION_ID = 0x42525431
    SCHEMA_VERSION = 1

    def __init__(
        self,
        path: Path,
        log_source: str,
        *,
        dry_run: bool = False,
        now_ms: int | None = None,
    ) -> None:
        self.log_source = log_source
        super().__init__(path, dry_run=dry_run, now_ms=now_ms)

    def _initialize(self) -> None:
        if self._schema_exists():
            row = self._db.execute(
                "SELECT log_source FROM metadata WHERE singleton=1"
            ).fetchone()
            if row is None or row[0] != self.log_source:
                raise StateError("The database belongs to another log source")
            return
        self._create_schema(
            _SCHEMA,
            initialize=(
                "INSERT INTO metadata VALUES (1,?,NULL,NULL,NULL)",
                (self.log_source,),
            ),
        )

    # Cursor and open calls -------------------------------------------------------

    def cursor(self) -> Cursor | None:
        row = self._db.execute(
            "SELECT coverage_from,coverage_until,last_key FROM metadata "
            "WHERE singleton=1"
        ).fetchone()
        if row[0] is None:
            return None
        return Cursor(
            timestamp_str_to_datetime(row[0]), timestamp_str_to_datetime(row[1]), row[2]
        )

    def pending_calls(self) -> list[CallRef]:
        """The calls of open interactions, in S3 key order."""
        return [
            CallRef(request_id=row[0], source_key=row[1], turn_key=row[2])
            for row in self._db.execute(
                "SELECT request_id,s3_key,turn_key FROM pending_call "
                "ORDER BY s3_key,request_id"
            )
        ]

    def is_closed(self, turn_key: str) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM closed_interaction WHERE turn_key=?", (turn_key,)
        ).fetchone()
        return row is not None

    def failed_turn_start(self, turn_key: str) -> CallRef | None:
        """Return the original first call needed to reopen a failed conversion."""
        row = self._db.execute(
            "SELECT first_request_id,first_s3_key FROM closed_interaction "
            "WHERE turn_key=? AND outcome=?",
            (turn_key, CONVERSION_ERROR),
        ).fetchone()
        return None if row is None else CallRef(row[0], row[1], turn_key)

    def read_object_keys(self, since: datetime, until: datetime) -> set[str]:
        """Files already processed in the overlapping part of a listing."""
        return {
            row[0]
            for row in self._db.execute(
                "SELECT s3_key FROM read_object WHERE delivered_at BETWEEN ? AND ?",
                (_ts(since), _ts(until)),
            )
        }

    def checkpoint(
        self,
        cursor: Cursor,
        open_calls: Iterable[CallRef],
        closures: Iterable[Closure],
        *,
        read_objects: Iterable[tuple[str, datetime]] = (),
        forget_objects_before: datetime | None = None,
        reopened_turns: Iterable[str] = (),
    ) -> None:
        """Save everything a restart needs, in one transaction."""
        with self._transaction():
            self._db.executemany(
                "INSERT OR IGNORE INTO read_object VALUES (?,?)",
                [(key, _ts(at)) for key, at in read_objects],
            )
            if forget_objects_before is not None:
                self._db.execute(
                    "DELETE FROM read_object WHERE delivered_at<?",
                    (_ts(forget_objects_before),),
                )
            self._db.execute("DELETE FROM pending_call")
            self._db.executemany(
                "INSERT OR IGNORE INTO pending_call VALUES (?,?,?)",
                [(ref.request_id, ref.source_key, ref.turn_key) for ref in open_calls],
            )
            self._db.executemany(
                "DELETE FROM closed_interaction WHERE turn_key=? AND outcome=?",
                [(key, CONVERSION_ERROR) for key in reopened_turns],
            )
            for closure in closures:
                self._db.execute(
                    "INSERT INTO closed_interaction VALUES (?,?,?,?,?)",
                    (
                        closure.turn_key,
                        _ts(datetime.fromtimestamp(self.now_ms / 1000, UTC)),
                        closure.outcome,
                        closure.first_call.request_id if closure.first_call else None,
                        closure.first_call.source_key if closure.first_call else None,
                    ),
                )
                if closure.payload is not None:
                    outbox.put(self._db, closure.turn_key, closure.payload)
            self._db.execute(
                "UPDATE metadata SET coverage_from=?,coverage_until=?,last_key=? "
                "WHERE singleton=1",
                (
                    _ts(cursor.coverage_from),
                    _ts(cursor.coverage_until),
                    cursor.last_key,
                ),
            )

    def reset(self) -> None:
        """Forget the cursor and the open calls, as the other integrations do
        when a run confirmed with ``--yes`` leaves a gap. Closed interactions
        and the outbox stay, so nothing is queued twice."""
        with self._transaction():
            self._db.execute("DELETE FROM pending_call")
            self._db.execute("DELETE FROM read_object")
            self._db.execute(
                "UPDATE metadata SET coverage_from=NULL,coverage_until=NULL,"
                "last_key=NULL WHERE singleton=1"
            )

    def prune_content(self, retention_seconds: int) -> int:
        """Compact payloads of interactions sent or rejected before the retention."""
        cutoff = self._retention_cutoff(retention_seconds)
        with self._transaction():
            return outbox.compact(self._db, cutoff)

    def prune_history(self, retention_seconds: int) -> int:
        """Forget interactions finished before the retention: their outbox rows
        and closed turns. A backfill over that time sends them again, and
        Nebuly updates the same interaction. Returns how many outbox rows.

        A closed turn is forgotten only once its outbox row is gone: the row
        is purged by delivery time, which may be days after the close (a
        retry), and a turn rebuilt while its sent row is still there would
        clash with it."""
        cutoff = self._retention_cutoff(retention_seconds)
        with self._transaction():
            purged = outbox.purge(self._db, cutoff)
            self._db.execute(
                "DELETE FROM closed_interaction WHERE closed_at<=? AND turn_key "
                "NOT IN (SELECT interaction_key FROM outbox)",
                (_ts(datetime.fromtimestamp(cutoff / 1000, UTC)),),
            )
            return purged
