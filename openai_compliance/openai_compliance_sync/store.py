from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

if TYPE_CHECKING:
    from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingest_cursor (
  principal_id TEXT NOT NULL,
  event_type   TEXT NOT NULL,
  after_ts     TEXT,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (principal_id, event_type)
);

CREATE TABLE IF NOT EXISTS processed_file (
  principal_id TEXT NOT NULL,
  file_id      TEXT NOT NULL,
  event_type   TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  PRIMARY KEY (principal_id, file_id)
);

CREATE TABLE IF NOT EXISTS chat_message (
  principal_id          TEXT NOT NULL,
  message_id            TEXT NOT NULL,
  conversation_id       TEXT NOT NULL,
  author_type           TEXT NOT NULL,
  created_at            TEXT NOT NULL,
  previous_message_id   TEXT,
  record_json           TEXT NOT NULL,
  PRIMARY KEY (principal_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_chat_message_conversation
  ON chat_message (principal_id, conversation_id, created_at);

CREATE TABLE IF NOT EXISTS app_call (
  principal_id     TEXT NOT NULL,
  event_id         TEXT NOT NULL,
  conversation_id  TEXT NOT NULL,
  app_id           TEXT NOT NULL,
  record_kind      TEXT NOT NULL,
  timestamp        TEXT NOT NULL,
  record_json      TEXT NOT NULL,
  PRIMARY KEY (principal_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_app_call_window
  ON app_call (principal_id, conversation_id, app_id, timestamp);

CREATE TABLE IF NOT EXISTS emitted_interaction (
  principal_id      TEXT NOT NULL,
  source            TEXT NOT NULL,
  interaction_key   TEXT NOT NULL,
  status            TEXT NOT NULL,
  updated_at        TEXT NOT NULL,
  PRIMARY KEY (principal_id, source, interaction_key)
);
"""


@dataclass(frozen=True)
class StoredChatMessage:
    message_id: str
    conversation_id: str
    author_type: str
    created_at: datetime
    previous_message_id: str | None
    record_json: str


@dataclass(frozen=True)
class StoredAppCall:
    event_id: str
    conversation_id: str
    app_id: str
    record_kind: str
    timestamp: datetime
    record_json: str


def _now_ts() -> str:
    return datetime_to_timestamp_str(datetime.now(tz=UTC))


def _row_ts(value: str | None) -> datetime | None:
    if value is None:
        return None
    return timestamp_str_to_datetime(value)


class ComplianceStore:
    def __init__(self, db_path: Path, principal_id: str, *, dry_run: bool) -> None:
        self._principal_id = principal_id
        self._dry_run = dry_run
        if dry_run:
            self._conn = sqlite3.connect(":memory:")
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    def get_cursor(self, event_type: str) -> datetime | None:
        row = self._conn.execute(
            """
            SELECT after_ts FROM ingest_cursor
            WHERE principal_id = ? AND event_type = ?
            """,
            (self._principal_id, event_type),
        ).fetchone()
        if row is None:
            return None
        return _row_ts(row[0])

    def set_cursor(self, event_type: str, after: datetime | None) -> None:
        now = _now_ts()
        after_ts = datetime_to_timestamp_str(after) if after is not None else None
        self._conn.execute(
            """
            INSERT INTO ingest_cursor (
              principal_id, event_type, after_ts, updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT (principal_id, event_type) DO UPDATE SET
              after_ts = excluded.after_ts,
              updated_at = excluded.updated_at
            """,
            (self._principal_id, event_type, after_ts, now),
        )

    def is_file_processed(self, file_id: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM processed_file
            WHERE principal_id = ? AND file_id = ?
            """,
            (self._principal_id, file_id),
        ).fetchone()
        return row is not None

    def mark_file_processed(self, file_id: str, event_type: str) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO processed_file (
              principal_id, file_id, event_type, processed_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT (principal_id, file_id) DO NOTHING
            """,
            (self._principal_id, file_id, event_type, now),
        )

    def insert_chat_message(
        self,
        *,
        message_id: str,
        conversation_id: str,
        author_type: str,
        created_at: datetime,
        previous_message_id: str | None,
        record: dict[str, object],
    ) -> bool:
        cur = self._conn.execute(
            """
            INSERT INTO chat_message (
              principal_id, message_id, conversation_id, author_type,
              created_at, previous_message_id, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (principal_id, message_id) DO NOTHING
            """,
            (
                self._principal_id,
                message_id,
                conversation_id,
                author_type,
                datetime_to_timestamp_str(created_at),
                previous_message_id,
                json.dumps(record),
            ),
        )
        return cur.rowcount > 0

    def insert_app_call(
        self,
        *,
        event_id: str,
        conversation_id: str,
        app_id: str,
        record_kind: str,
        timestamp: datetime,
        record: dict[str, object],
    ) -> bool:
        cur = self._conn.execute(
            """
            INSERT INTO app_call (
              principal_id, event_id, conversation_id, app_id,
              record_kind, timestamp, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (principal_id, event_id) DO NOTHING
            """,
            (
                self._principal_id,
                event_id,
                conversation_id,
                app_id,
                record_kind,
                datetime_to_timestamp_str(timestamp),
                json.dumps(record),
            ),
        )
        return cur.rowcount > 0

    def unemitted_user_messages(self) -> list[StoredChatMessage]:
        rows = self._conn.execute(
            """
            SELECT m.message_id, m.conversation_id, m.author_type, m.created_at,
                   m.previous_message_id, m.record_json
            FROM chat_message m
            LEFT JOIN emitted_interaction e
              ON e.principal_id = m.principal_id
             AND e.source = 'chats'
             AND e.interaction_key = m.message_id
            WHERE m.principal_id = ?
              AND m.author_type = 'user'
              AND e.interaction_key IS NULL
            ORDER BY m.created_at
            """,
            (self._principal_id,),
        ).fetchall()
        return [_chat_row(row) for row in rows]

    def conversation_messages(self, conversation_id: str) -> list[StoredChatMessage]:
        rows = self._conn.execute(
            """
            SELECT message_id, conversation_id, author_type, created_at,
                   previous_message_id, record_json
            FROM chat_message
            WHERE principal_id = ? AND conversation_id = ?
            ORDER BY created_at
            """,
            (self._principal_id, conversation_id),
        ).fetchall()
        return [_chat_row(row) for row in rows]

    def app_calls_in_window(
        self,
        conversation_id: str,
        *,
        start: datetime,
        end: datetime,
    ) -> list[StoredAppCall]:
        rows = self._conn.execute(
            """
            SELECT event_id, conversation_id, app_id, record_kind, timestamp,
                   record_json
            FROM app_call
            WHERE principal_id = ?
              AND conversation_id = ?
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY timestamp
            """,
            (
                self._principal_id,
                conversation_id,
                datetime_to_timestamp_str(start),
                datetime_to_timestamp_str(end),
            ),
        ).fetchall()
        return [_app_row(row) for row in rows]

    def is_emitted(self, source: str, interaction_key: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM emitted_interaction
            WHERE principal_id = ? AND source = ? AND interaction_key = ?
            """,
            (self._principal_id, source, interaction_key),
        ).fetchone()
        return row is not None

    def record_emitted(self, source: str, interaction_key: str, status: str) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO emitted_interaction (
              principal_id, source, interaction_key, status, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (principal_id, source, interaction_key) DO NOTHING
            """,
            (self._principal_id, source, interaction_key, status, now),
        )

    def prune(self, *, retention_days: int = 35) -> None:
        cutoff = datetime.now(tz=UTC) - timedelta(days=retention_days)
        cutoff_ts = datetime_to_timestamp_str(cutoff)
        self._conn.execute(
            "DELETE FROM chat_message WHERE principal_id = ? AND created_at < ?",
            (self._principal_id, cutoff_ts),
        )
        self._conn.execute(
            "DELETE FROM app_call WHERE principal_id = ? AND timestamp < ?",
            (self._principal_id, cutoff_ts),
        )

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def _chat_row(row: tuple[str, ...]) -> StoredChatMessage:
    return StoredChatMessage(
        message_id=row[0],
        conversation_id=row[1],
        author_type=row[2],
        created_at=timestamp_str_to_datetime(row[3]),
        previous_message_id=row[4],
        record_json=row[5],
    )


def _app_row(row: tuple[str, ...]) -> StoredAppCall:
    return StoredAppCall(
        event_id=row[0],
        conversation_id=row[1],
        app_id=row[2],
        record_kind=row[3],
        timestamp=timestamp_str_to_datetime(row[4]),
        record_json=row[5],
    )
