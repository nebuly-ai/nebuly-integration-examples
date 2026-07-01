from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

if TYPE_CHECKING:
    from pathlib import Path

    from .models import Session


@dataclass(frozen=True)
class SessionState:
    engine_key: str
    session_id: str
    user_id: str | None
    last_seen_end_time: str | None
    last_sent_turn_time: str | None
    last_sent_query_id: str | None
    status: str
    last_error: str | None
    last_successful_run_at: str | None
    updated_at: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_session_state (
  engine_key            TEXT NOT NULL,
  session_id            TEXT NOT NULL,
  user_id               TEXT,
  last_seen_end_time    TEXT,
  last_sent_turn_time   TEXT,
  last_sent_query_id    TEXT,
  status                TEXT NOT NULL,
  last_error            TEXT,
  last_successful_run_at TEXT,
  updated_at            TEXT NOT NULL,
  PRIMARY KEY (engine_key, session_id)
);
"""


class SyncCache:
    def __init__(
        self, db_path: Path, engine_key: str, *, dry_run: bool = False
    ) -> None:
        self._engine_key = engine_key
        self._conn = sqlite3.connect(":memory:" if dry_run else db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def get(self, session_id: str) -> SessionState | None:
        row = self._conn.execute(
            """
            SELECT engine_key, session_id, user_id, last_seen_end_time,
                   last_sent_turn_time, last_sent_query_id, status,
                   last_error, last_successful_run_at, updated_at
            FROM sync_session_state
            WHERE engine_key = ? AND session_id = ?
            """,
            (self._engine_key, session_id),
        ).fetchone()
        if row is None:
            return None
        return SessionState(*row)

    def should_skip_fetch(self, session: Session) -> bool:
        state = self.get(session.session_id)
        if state is None:
            return False
        return (
            state.status == "complete" and state.last_seen_end_time == session.end_time
        )

    def checkpoint(
        self,
        session: Session,
        *,
        turn_time: datetime,
        query_id: str,
    ) -> None:
        now = datetime_to_timestamp_str(datetime.now(UTC))
        turn_time_str = datetime_to_timestamp_str(turn_time)
        self._conn.execute(
            """
            INSERT INTO sync_session_state (
              engine_key, session_id, user_id, last_seen_end_time,
              last_sent_turn_time, last_sent_query_id, status,
              last_error, last_successful_run_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'partial', NULL, NULL, ?)
            ON CONFLICT(engine_key, session_id) DO UPDATE SET
              user_id = excluded.user_id,
              last_sent_turn_time = excluded.last_sent_turn_time,
              last_sent_query_id = excluded.last_sent_query_id,
              status = 'partial',
              last_error = NULL,
              updated_at = excluded.updated_at
            """,
            (
                self._engine_key,
                session.session_id,
                session.user_id,
                session.end_time,
                turn_time_str,
                query_id,
                now,
            ),
        )
        self._conn.commit()

    def mark_complete(self, session: Session) -> None:
        now = datetime_to_timestamp_str(datetime.now(UTC))
        existing = self.get(session.session_id)
        self._conn.execute(
            """
            INSERT INTO sync_session_state (
              engine_key, session_id, user_id, last_seen_end_time,
              last_sent_turn_time, last_sent_query_id, status,
              last_error, last_successful_run_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, NULL, 'complete', NULL, ?, ?)
            ON CONFLICT(engine_key, session_id) DO UPDATE SET
              user_id = excluded.user_id,
              last_seen_end_time = excluded.last_seen_end_time,
              status = 'complete',
              last_error = NULL,
              last_successful_run_at = excluded.last_successful_run_at,
              updated_at = excluded.updated_at
            """,
            (
                self._engine_key,
                session.session_id,
                session.user_id,
                session.end_time,
                now,
                now,
            ),
        )
        if existing and existing.last_sent_turn_time:
            self._conn.execute(
                """
                UPDATE sync_session_state
                SET last_sent_turn_time = ?, last_sent_query_id = ?
                WHERE engine_key = ? AND session_id = ?
                """,
                (
                    existing.last_sent_turn_time,
                    existing.last_sent_query_id,
                    self._engine_key,
                    session.session_id,
                ),
            )
        self._conn.commit()

    def mark_partial(self, session: Session, *, error: str | None = None) -> None:
        now = datetime_to_timestamp_str(datetime.now(UTC))
        self._conn.execute(
            """
            INSERT INTO sync_session_state (
              engine_key, session_id, user_id, last_seen_end_time,
              last_sent_turn_time, last_sent_query_id, status,
              last_error, last_successful_run_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, NULL, 'partial', ?, NULL, ?)
            ON CONFLICT(engine_key, session_id) DO UPDATE SET
              user_id = excluded.user_id,
              last_seen_end_time = excluded.last_seen_end_time,
              status = 'partial',
              last_error = COALESCE(excluded.last_error, sync_session_state.last_error),
              updated_at = excluded.updated_at
            """,
            (
                self._engine_key,
                session.session_id,
                session.user_id,
                session.end_time,
                error,
                now,
            ),
        )
        self._conn.commit()

    def advance_watermark(
        self,
        session: Session,
        *,
        turn_time: datetime,
        query_id: str,
    ) -> None:
        now = datetime_to_timestamp_str(datetime.now(UTC))
        turn_time_str = datetime_to_timestamp_str(turn_time)
        self._conn.execute(
            """
            INSERT INTO sync_session_state (
              engine_key, session_id, user_id, last_seen_end_time,
              last_sent_turn_time, last_sent_query_id, status,
              last_error, last_successful_run_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'partial', NULL, NULL, ?)
            ON CONFLICT(engine_key, session_id) DO UPDATE SET
              user_id = excluded.user_id,
              last_sent_turn_time = excluded.last_sent_turn_time,
              last_sent_query_id = excluded.last_sent_query_id,
              updated_at = excluded.updated_at
            """,
            (
                self._engine_key,
                session.session_id,
                session.user_id,
                session.end_time,
                turn_time_str,
                query_id,
                now,
            ),
        )
        self._conn.commit()

    def last_sent_turn_datetime(self, session_id: str) -> datetime | None:
        state = self.get(session_id)
        if state is None or not state.last_sent_turn_time:
            return None
        return timestamp_str_to_datetime(state.last_sent_turn_time)

    def last_sent_query_id(self, session_id: str) -> str | None:
        state = self.get(session_id)
        if state is None:
            return None
        return state.last_sent_query_id

    def close(self) -> None:
        self._conn.close()
