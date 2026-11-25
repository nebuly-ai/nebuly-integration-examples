from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .models import Call, UsageRecord

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS usage_record (
  event_id            TEXT PRIMARY KEY,
  timestamp_ms        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usage_record_time
  ON usage_record (timestamp_ms);

CREATE TABLE IF NOT EXISTS pairing_pending (
  github_request_id   TEXT PRIMARY KEY,
  event_id            TEXT NOT NULL,
  user_id             INTEGER NOT NULL,
  endpoint            TEXT NOT NULL,
  body                TEXT NOT NULL,
  timestamp_ms        INTEGER NOT NULL,
  truncated           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pairing_pending_response (
  github_request_id   TEXT PRIMARY KEY,
  event_id            TEXT NOT NULL,
  user_id             INTEGER NOT NULL,
  endpoint            TEXT NOT NULL,
  body                TEXT NOT NULL,
  timestamp_ms        INTEGER NOT NULL,
  truncated           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS turn_call (
  github_request_id       TEXT PRIMARY KEY,
  turn_key                TEXT NOT NULL,
  conversation_id         TEXT NOT NULL,
  closed                  INTEGER NOT NULL,
  user_id                 INTEGER NOT NULL,
  endpoint                TEXT NOT NULL,
  request_body            TEXT NOT NULL,
  response_body           TEXT NOT NULL,
  request_timestamp_ms    INTEGER NOT NULL,
  response_timestamp_ms   INTEGER NOT NULL,
  request_event_id        TEXT NOT NULL,
  response_event_id       TEXT NOT NULL,
  request_truncated       INTEGER NOT NULL,
  response_truncated      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_turn_call_turn
  ON turn_call (turn_key);

CREATE TABLE IF NOT EXISTS conversation_session (
  session_key         TEXT PRIMARY KEY,
  conversation_id     TEXT NOT NULL,
  user_id             INTEGER NOT NULL,
  last_activity_ms    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS source_cursor (
  source_name         TEXT PRIMARY KEY,
  cursor_value        TEXT NOT NULL,
  updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_login (
  user_id             INTEGER PRIMARY KEY,
  login               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS emitted_turn (
  turn_key            TEXT PRIMARY KEY,
  status              TEXT NOT NULL,
  emitted_at          TEXT NOT NULL
);
"""

USAGE_RECORD_RETENTION_MS = 7 * 24 * 3_600_000


@dataclass(frozen=True)
class PendingRecord:
    event_id: str
    user_id: int
    endpoint: str
    body: str
    timestamp_ms: int
    truncated: bool

    @classmethod
    def from_usage_record(cls, record: UsageRecord) -> PendingRecord:
        return cls(
            event_id=record.event_id,
            user_id=record.user_id,
            endpoint=record.endpoint,
            body=record.body,
            timestamp_ms=record.timestamp_ms,
            truncated=record.truncated,
        )


@dataclass(frozen=True)
class StoredTurnCall:
    turn_key: str
    conversation_id: str
    closed: bool
    call: Call


@dataclass(frozen=True)
class ConversationSession:
    session_key: str
    conversation_id: str
    user_id: int
    last_activity_ms: int


class Store:
    def __init__(self, path: Path, *, dry_run: bool = False) -> None:
        if dry_run:
            self._conn = sqlite3.connect(":memory:")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(path)
        self._conn.executescript(_SCHEMA)
        self._in_transaction = False
        self.prune_usage_records()

    def close(self) -> None:
        self._conn.close()

    def _commit(self) -> None:
        if not self._in_transaction:
            self._conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Defer commits until the block ends so its writes land atomically."""
        self._in_transaction = True
        try:
            yield
        except BaseException:
            self._in_transaction = False
            self._conn.rollback()
            raise
        self._in_transaction = False
        self._conn.commit()

    def insert_usage_record(self, record: UsageRecord) -> bool:
        """Return False if event_id already exists (dedupe)."""
        cursor = self._conn.execute(
            "INSERT OR IGNORE INTO usage_record (event_id, timestamp_ms) VALUES (?, ?)",
            (record.event_id, record.timestamp_ms),
        )
        self._commit()
        return cursor.rowcount == 1

    def prune_usage_records(
        self, retention_ms: int = USAGE_RECORD_RETENTION_MS
    ) -> int:
        """Forget event_ids older than `retention_ms` behind the newest one stored.

        The horizon follows the data, not the wall clock, so a quiet stream or a
        backfill of old records keeps its dedupe window. It has to outlast how
        far back a source can re-deliver: the 48 h REST window, the REST overlap
        and `OBJECT_LAG_SECONDS`.
        """
        cursor = self._conn.execute(
            """
            DELETE FROM usage_record
            WHERE timestamp_ms < (SELECT MAX(timestamp_ms) FROM usage_record) - ?
            """,
            (retention_ms,),
        )
        self._commit()
        return cursor.rowcount

    def save_pending_request(self, record: UsageRecord) -> None:
        self._save_pending("pairing_pending", record)

    def save_pending_response(self, record: UsageRecord) -> None:
        self._save_pending("pairing_pending_response", record)

    def take_pending_request(self, github_request_id: str) -> PendingRecord | None:
        return self._take_pending("pairing_pending", github_request_id)

    def take_pending_response(self, github_request_id: str) -> PendingRecord | None:
        return self._take_pending("pairing_pending_response", github_request_id)

    def expire_pending_requests_before(self, cutoff_ms: int) -> int:
        return self._expire_pending("pairing_pending", cutoff_ms)

    def expire_pending_responses_before(self, cutoff_ms: int) -> int:
        return self._expire_pending("pairing_pending_response", cutoff_ms)

    def _save_pending(self, table: str, record: UsageRecord) -> None:
        self._conn.execute(
            f"""
            INSERT INTO {table} (
              github_request_id, event_id, user_id, endpoint,
              body, timestamp_ms, truncated
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(github_request_id) DO UPDATE SET
              event_id = excluded.event_id,
              user_id = excluded.user_id,
              endpoint = excluded.endpoint,
              body = excluded.body,
              timestamp_ms = excluded.timestamp_ms,
              truncated = excluded.truncated
            """,
            (
                record.github_request_id,
                record.event_id,
                record.user_id,
                record.endpoint,
                record.body,
                record.timestamp_ms,
                int(record.truncated),
            ),
        )
        self._commit()

    def _take_pending(self, table: str, github_request_id: str) -> PendingRecord | None:
        cursor = self._conn.execute(
            f"""
            SELECT event_id, user_id, endpoint, body, timestamp_ms, truncated
            FROM {table}
            WHERE github_request_id = ?
            """,
            (github_request_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        self._conn.execute(
            f"DELETE FROM {table} WHERE github_request_id = ?",
            (github_request_id,),
        )
        self._commit()
        return PendingRecord(
            event_id=row[0],
            user_id=row[1],
            endpoint=row[2],
            body=row[3],
            timestamp_ms=row[4],
            truncated=bool(row[5]),
        )

    def _expire_pending(self, table: str, cutoff_ms: int) -> int:
        cursor = self._conn.execute(
            f"DELETE FROM {table} WHERE timestamp_ms <= ?",
            (cutoff_ms,),
        )
        self._commit()
        return cursor.rowcount

    def save_turn_call(self, turn_key: str, conversation_id: str, call: Call) -> None:
        self._conn.execute(
            """
            INSERT INTO turn_call (
              github_request_id, turn_key, conversation_id, closed, user_id,
              endpoint, request_body, response_body, request_timestamp_ms,
              response_timestamp_ms, request_event_id, response_event_id,
              request_truncated, response_truncated
            ) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(github_request_id) DO UPDATE SET
              turn_key = excluded.turn_key,
              conversation_id = excluded.conversation_id
            """,
            (
                call.github_request_id,
                turn_key,
                conversation_id,
                call.user_id,
                call.endpoint,
                call.request_body,
                call.response_body,
                call.request_timestamp_ms,
                call.response_timestamp_ms,
                call.request_event_id,
                call.response_event_id,
                int(call.request_truncated),
                int(call.response_truncated),
            ),
        )
        self._commit()

    def mark_turn_closed(self, turn_key: str) -> None:
        self._conn.execute(
            "UPDATE turn_call SET closed = 1 WHERE turn_key = ? AND closed = 0",
            (turn_key,),
        )
        self._commit()

    def release_closed_turn(self, turn_key: str) -> None:
        self._conn.execute(
            "DELETE FROM turn_call WHERE turn_key = ? AND closed = 1",
            (turn_key,),
        )
        self._commit()

    def load_turn_calls(self) -> list[StoredTurnCall]:
        cursor = self._conn.execute(
            """
            SELECT turn_key, conversation_id, closed, github_request_id, user_id,
                   endpoint, request_body, response_body, request_timestamp_ms,
                   response_timestamp_ms, request_event_id, response_event_id,
                   request_truncated, response_truncated
            FROM turn_call
            ORDER BY rowid
            """
        )
        return [
            StoredTurnCall(
                turn_key=row[0],
                conversation_id=row[1],
                closed=bool(row[2]),
                call=Call(
                    github_request_id=row[3],
                    user_id=row[4],
                    endpoint=row[5],
                    request_body=row[6],
                    response_body=row[7],
                    request_timestamp_ms=row[8],
                    response_timestamp_ms=row[9],
                    request_event_id=row[10],
                    response_event_id=row[11],
                    request_truncated=bool(row[12]),
                    response_truncated=bool(row[13]),
                ),
            )
            for row in cursor.fetchall()
        ]

    def get_conversation_session(self, session_key: str) -> ConversationSession | None:
        cursor = self._conn.execute(
            """
            SELECT session_key, conversation_id, user_id, last_activity_ms
            FROM conversation_session
            WHERE session_key = ?
            """,
            (session_key,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return ConversationSession(*row)

    def upsert_conversation_session(
        self,
        session_key: str,
        conversation_id: str,
        user_id: int,
        last_activity_ms: int,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO conversation_session (
              session_key, conversation_id, user_id, last_activity_ms
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
              conversation_id = excluded.conversation_id,
              last_activity_ms = excluded.last_activity_ms
            """,
            (session_key, conversation_id, user_id, last_activity_ms),
        )
        self._commit()

    def set_source_cursor(self, source_name: str, cursor_value: str) -> None:
        now = datetime.now(UTC).isoformat()
        self._conn.execute(
            """
            INSERT INTO source_cursor (source_name, cursor_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
              cursor_value = excluded.cursor_value,
              updated_at = excluded.updated_at
            """,
            (source_name, cursor_value, now),
        )
        self._commit()

    def get_source_cursor(self, source_name: str) -> str | None:
        cursor = self._conn.execute(
            "SELECT cursor_value FROM source_cursor WHERE source_name = ?",
            (source_name,),
        )
        row = cursor.fetchone()
        return None if row is None else str(row[0])

    def get_user_login(self, user_id: int) -> str | None:
        cursor = self._conn.execute(
            "SELECT login FROM user_login WHERE user_id = ?",
            (user_id,),
        )
        row = cursor.fetchone()
        return None if row is None else str(row[0])

    def set_user_login(self, user_id: int, login: str) -> None:
        self._conn.execute(
            """
            INSERT INTO user_login (user_id, login) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET login = excluded.login
            """,
            (user_id, login),
        )
        self._commit()

    def is_turn_emitted(self, turn_key: str) -> bool:
        cursor = self._conn.execute(
            "SELECT 1 FROM emitted_turn WHERE turn_key = ?",
            (turn_key,),
        )
        return cursor.fetchone() is not None

    def record_emitted_turn(self, turn_key: str, status: str) -> None:
        now = datetime.now(UTC).isoformat()
        self._conn.execute(
            """
            INSERT INTO emitted_turn (turn_key, status, emitted_at)
            VALUES (?, ?, ?)
            ON CONFLICT(turn_key) DO UPDATE SET
              status = excluded.status,
              emitted_at = excluded.emitted_at
            """,
            (turn_key, status, now),
        )
        self._commit()
