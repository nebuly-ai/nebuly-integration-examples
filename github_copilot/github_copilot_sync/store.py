from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .models import UsageRecord

if TYPE_CHECKING:
    from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS usage_record (
  event_id            TEXT PRIMARY KEY,
  record_type         TEXT NOT NULL,
  user_id             INTEGER NOT NULL,
  enterprise_id       INTEGER NOT NULL,
  github_request_id   TEXT NOT NULL,
  endpoint            TEXT NOT NULL,
  body                TEXT NOT NULL,
  timestamp_ms        INTEGER NOT NULL,
  truncated           INTEGER NOT NULL,
  record_json         TEXT NOT NULL
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


@dataclass(frozen=True)
class PendingRequest:
    event_id: str
    user_id: int
    endpoint: str
    body: str
    timestamp_ms: int
    truncated: bool


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

    def close(self) -> None:
        self._conn.close()

    def insert_usage_record(self, record: UsageRecord) -> bool:
        """Return False if event_id already exists (dedupe)."""
        cursor = self._conn.execute(
            "SELECT 1 FROM usage_record WHERE event_id = ?",
            (record.event_id,),
        )
        if cursor.fetchone():
            return False
        self._conn.execute(
            """
            INSERT INTO usage_record (
              event_id, record_type, user_id, enterprise_id,
              github_request_id, endpoint, body, timestamp_ms,
              truncated, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.event_id,
                record.type,
                record.user_id,
                record.enterprise_id,
                record.github_request_id,
                record.endpoint,
                record.body,
                record.timestamp_ms,
                int(record.truncated),
                json.dumps(record.model_dump(by_alias=True)),
            ),
        )
        self._conn.commit()
        return True

    def save_pending_request(self, record: UsageRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO pairing_pending (
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
        self._conn.commit()

    def take_pending_request(self, github_request_id: str) -> PendingRequest | None:
        cursor = self._conn.execute(
            """
            SELECT event_id, user_id, endpoint, body, timestamp_ms, truncated
            FROM pairing_pending
            WHERE github_request_id = ?
            """,
            (github_request_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        self._conn.execute(
            "DELETE FROM pairing_pending WHERE github_request_id = ?",
            (github_request_id,),
        )
        self._conn.commit()
        return PendingRequest(
            event_id=row[0],
            user_id=row[1],
            endpoint=row[2],
            body=row[3],
            timestamp_ms=row[4],
            truncated=bool(row[5]),
        )

    def expire_pending_requests_before(self, cutoff_ms: int) -> int:
        cursor = self._conn.execute(
            """
            SELECT github_request_id FROM pairing_pending
            WHERE timestamp_ms <= ?
            """,
            (cutoff_ms,),
        )
        ids = [row[0] for row in cursor.fetchall()]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        self._conn.execute(
            f"DELETE FROM pairing_pending WHERE github_request_id IN ({placeholders})",
            ids,
        )
        self._conn.commit()
        return len(ids)

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
        self._conn.commit()

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
        self._conn.commit()

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
        self._conn.commit()

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
        self._conn.commit()
