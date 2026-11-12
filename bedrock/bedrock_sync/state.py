"""What the sync remembers between runs, in a SQLite file.

Same approach as the compliance integrations (``sync_state.db`` in
``--cache-dir``, WAL journal, an in-memory database in dry run so a test run
changes nothing). Three things are kept, per log source (bucket and prefix):

- how far the logs were read (``ingest_cursor``);
- what the ``Grouper`` left half done: calls not sent yet, conversation
  hashes, request ids already seen (the other tables but
  ``emitted_interaction``). Calls are kept as a pointer, the S3 key and the
  request id, and read again from S3: no conversation text is stored here;
- what was already sent (``emitted_interaction``), written right after each
  POST so a run that stops halfway never sends a turn twice. It is never
  pruned, as in the compliance integrations: a backfill can read files of
  any age again, and only this table keeps their turns from being sent twice.

The cursor and the ``Grouper`` state are saved together by
``save_checkpoint``, in one transaction: after a crash the next run starts
from the last checkpoint and rebuilds the same turns, and the registry skips
those already sent.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .config import timestamp_str_to_datetime
from .grouping import (
    CallRef,
    ConversationSource,
    GrouperSnapshot,
    OpenTurnState,
    TipState,
)

if TYPE_CHECKING:
    from pathlib import Path

DB_FILENAME = "sync_state.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingest_cursor (
  log_source     TEXT NOT NULL,
  coverage_from  TEXT NOT NULL,
  coverage_until TEXT NOT NULL,
  last_key       TEXT,
  latest_call    TEXT,
  updated_at     TEXT NOT NULL,
  PRIMARY KEY (log_source)
);

CREATE TABLE IF NOT EXISTS pending_call (
  log_source TEXT NOT NULL,
  request_id TEXT NOT NULL,
  s3_key     TEXT NOT NULL,
  call_time  TEXT NOT NULL,
  turn_id    INTEGER,
  position   INTEGER NOT NULL,
  PRIMARY KEY (log_source, request_id)
);

CREATE TABLE IF NOT EXISTS open_turn (
  log_source          TEXT NOT NULL,
  turn_id             INTEGER NOT NULL,
  conversation_id     TEXT NOT NULL,
  conversation_source TEXT NOT NULL,
  partial_start       INTEGER NOT NULL,
  PRIMARY KEY (log_source, turn_id)
);

CREATE TABLE IF NOT EXISTS conversation_tip (
  log_source      TEXT NOT NULL,
  digest          TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  seen_at         TEXT NOT NULL,
  ambiguous       INTEGER NOT NULL,
  PRIMARY KEY (log_source, digest)
);

CREATE TABLE IF NOT EXISTS seen_request (
  log_source TEXT NOT NULL,
  request_id TEXT NOT NULL,
  call_time  TEXT NOT NULL,
  PRIMARY KEY (log_source, request_id)
);

CREATE TABLE IF NOT EXISTS answered_tool (
  log_source TEXT NOT NULL,
  loop_key   TEXT NOT NULL,
  tool_id    TEXT NOT NULL,
  call_time  TEXT NOT NULL,
  PRIMARY KEY (log_source, loop_key, tool_id)
);

CREATE TABLE IF NOT EXISTS emitted_interaction (
  log_source      TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  interaction_key TEXT NOT NULL,
  status          TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  PRIMARY KEY (log_source, interaction_key)
);
"""

_GROUPER_TABLES = (
    "pending_call",
    "open_turn",
    "conversation_tip",
    "seen_request",
    "answered_tool",
)
"""Rewritten as a whole at each checkpoint."""


@dataclass(frozen=True)
class Cursor:
    """How far the logs were read.

    ``coverage_until`` is the delivery time of the last file read in full,
    ``last_key`` its S3 key: the next run lists the keys after it.
    """

    coverage_from: datetime
    coverage_until: datetime
    last_key: str | None


def _ts(value: datetime) -> str:
    """Always the same width (microseconds included even when zero), so the
    order of the text is the order in time, in ``ORDER BY`` and in ``<``."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _row_ts(value: str | None) -> datetime | None:
    return timestamp_str_to_datetime(value) if value else None


def _now_ts() -> str:
    return _ts(datetime.now(tz=UTC))


class SyncState:
    def __init__(self, db_path: Path, log_source: str, *, dry_run: bool) -> None:
        self._log_source = log_source
        if dry_run:
            # Work on a copy, so a dry run goes on from the real cursor
            # (like Gemini, which reads its coverage and does not save it).
            self._conn = sqlite3.connect(":memory:")
            if db_path.exists():
                disk = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                try:
                    disk.backup(self._conn)
                finally:
                    disk.close()
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    # Cursor and Grouper state ------------------------------------------------

    def get_cursor(self) -> Cursor | None:
        row = self._conn.execute(
            """
            SELECT coverage_from, coverage_until, last_key FROM ingest_cursor
            WHERE log_source = ?
            """,
            (self._log_source,),
        ).fetchone()
        if row is None:
            return None
        return Cursor(
            coverage_from=timestamp_str_to_datetime(row[0]),
            coverage_until=timestamp_str_to_datetime(row[1]),
            last_key=row[2],
        )

    def load_snapshot(self) -> GrouperSnapshot:
        """The ``Grouper`` state of the last checkpoint (empty on the first
        run)."""
        source = (self._log_source,)
        latest = self._conn.execute(
            "SELECT latest_call FROM ingest_cursor WHERE log_source = ?", source
        ).fetchone()
        snapshot = GrouperSnapshot(latest=_row_ts(latest[0]) if latest else None)

        calls: dict[int | None, list[CallRef]] = {}
        for request_id, s3_key, call_time, turn_id in self._conn.execute(
            """
            SELECT request_id, s3_key, call_time, turn_id FROM pending_call
            WHERE log_source = ? ORDER BY turn_id, position
            """,
            source,
        ):
            ref = CallRef(request_id, s3_key, timestamp_str_to_datetime(call_time))
            calls.setdefault(turn_id, []).append(ref)
        snapshot.buffered = calls.get(None, [])
        snapshot.open_turns = [
            OpenTurnState(
                conversation_id=conversation_id,
                conversation_source=ConversationSource(conversation_source),
                partial_start=bool(partial_start),
                calls=tuple(calls.get(turn_id, [])),
            )
            for turn_id, conversation_id, conversation_source, partial_start in (
                self._conn.execute(
                    """
                    SELECT turn_id, conversation_id, conversation_source,
                           partial_start
                    FROM open_turn WHERE log_source = ? ORDER BY turn_id
                    """,
                    source,
                )
            )
        ]
        snapshot.tips = [
            TipState(
                digest,
                conversation_id,
                timestamp_str_to_datetime(seen_at),
                bool(ambiguous),
            )
            for digest, conversation_id, seen_at, ambiguous in self._conn.execute(
                """
                SELECT digest, conversation_id, seen_at, ambiguous
                FROM conversation_tip WHERE log_source = ? ORDER BY seen_at, digest
                """,
                source,
            )
        ]
        snapshot.seen = {
            request_id: timestamp_str_to_datetime(call_time)
            for request_id, call_time in self._conn.execute(
                "SELECT request_id, call_time FROM seen_request WHERE log_source = ?",
                source,
            )
        }
        snapshot.answered = {
            (loop, tool_id): timestamp_str_to_datetime(call_time)
            for loop, tool_id, call_time in self._conn.execute(
                """
                SELECT loop_key, tool_id, call_time FROM answered_tool
                WHERE log_source = ?
                """,
                source,
            )
        }
        return snapshot

    def save_checkpoint(self, cursor: Cursor, snapshot: GrouperSnapshot) -> None:
        """Save the cursor and the ``Grouper`` state together, and commit.

        Call it only once every turn released up to ``cursor`` has been sent
        (or recorded as rejected): those turns are no longer in the state.
        """
        source = self._log_source
        with self._conn:
            for table in _GROUPER_TABLES:
                self._conn.execute(
                    f"DELETE FROM {table} WHERE log_source = ?",  # noqa: S608 - fixed names
                    (source,),
                )
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO pending_call (
                  log_source, request_id, s3_key, call_time, turn_id, position
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        source,
                        ref.request_id,
                        ref.source_key,
                        _ts(ref.timestamp),
                        None,
                        i,
                    )
                    for i, ref in enumerate(snapshot.buffered)
                ]
                + [
                    (source, ref.request_id, ref.source_key, _ts(ref.timestamp), t, i)
                    for t, turn in enumerate(snapshot.open_turns)
                    for i, ref in enumerate(turn.calls)
                ],
            )
            self._conn.executemany(
                """
                INSERT INTO open_turn (
                  log_source, turn_id, conversation_id, conversation_source,
                  partial_start
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        source,
                        t,
                        turn.conversation_id,
                        turn.conversation_source.value,
                        int(turn.partial_start),
                    )
                    for t, turn in enumerate(snapshot.open_turns)
                ],
            )
            self._conn.executemany(
                """
                INSERT INTO conversation_tip (
                  log_source, digest, conversation_id, seen_at, ambiguous
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        source,
                        tip.digest,
                        tip.conversation_id,
                        _ts(tip.seen_at),
                        int(tip.ambiguous),
                    )
                    for tip in snapshot.tips
                ],
            )
            self._conn.executemany(
                """
                INSERT INTO seen_request (log_source, request_id, call_time)
                VALUES (?, ?, ?)
                """,
                [(source, rid, _ts(at)) for rid, at in snapshot.seen.items()],
            )
            self._conn.executemany(
                """
                INSERT INTO answered_tool (log_source, loop_key, tool_id, call_time)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (source, loop, tool_id, _ts(at))
                    for (loop, tool_id), at in snapshot.answered.items()
                ],
            )
            self._conn.execute(
                """
                INSERT INTO ingest_cursor (
                  log_source, coverage_from, coverage_until, last_key,
                  latest_call, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (log_source) DO UPDATE SET
                  coverage_from = excluded.coverage_from,
                  coverage_until = excluded.coverage_until,
                  last_key = excluded.last_key,
                  latest_call = excluded.latest_call,
                  updated_at = excluded.updated_at
                """,
                (
                    source,
                    _ts(cursor.coverage_from),
                    _ts(cursor.coverage_until),
                    cursor.last_key,
                    _ts(snapshot.latest) if snapshot.latest else None,
                    _now_ts(),
                ),
            )

    def reset(self) -> None:
        """Forget everything about this log source but the sent registry, as
        the other integrations do when a run confirmed with ``--yes`` leaves
        a gap."""
        with self._conn:
            for table in (*_GROUPER_TABLES, "ingest_cursor"):
                self._conn.execute(
                    f"DELETE FROM {table} WHERE log_source = ?",  # noqa: S608 - fixed names
                    (self._log_source,),
                )

    # Sent registry -------------------------------------------------------------

    def is_emitted(self, interaction_key: str) -> bool:
        """``interaction_key`` is the request id of the first call of the
        turn, unique by itself. The conversation id is not part of the key: a
        hash-chain id can change when a run is repeated and sees a late file
        the first run did not, and the turn must still count as sent."""
        row = self._conn.execute(
            """
            SELECT 1 FROM emitted_interaction
            WHERE log_source = ? AND interaction_key = ?
            """,
            (self._log_source, interaction_key),
        ).fetchone()
        return row is not None

    def record_emitted(
        self, conversation_id: str, interaction_key: str, status: str
    ) -> None:
        """``status`` is ``sent`` or ``rejected`` (a permanent 4xx). The
        first status wins, as in the other integrations; the caller commits.
        ``conversation_id`` is kept only to look a row up by hand."""
        self._conn.execute(
            """
            INSERT INTO emitted_interaction (
              log_source, conversation_id, interaction_key, status, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (log_source, interaction_key) DO NOTHING
            """,
            (self._log_source, conversation_id, interaction_key, status, _now_ts()),
        )

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
