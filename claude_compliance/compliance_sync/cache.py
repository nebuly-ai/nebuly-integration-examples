from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime
from .models import ChatSummary

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_user_state (
  user_id                            TEXT NOT NULL,
  organization_uuid                  TEXT NOT NULL,
  highest_completed_chat_updated_at  TEXT,
  coverage_from                      TEXT,
  coverage_until                     TEXT,
  last_successful_run_at             TEXT,
  updated_at                         TEXT NOT NULL,
  PRIMARY KEY (organization_uuid, user_id)
);

CREATE TABLE IF NOT EXISTS sync_chat_state (
  chat_id                        TEXT NOT NULL,
  user_id                        TEXT NOT NULL,
  organization_uuid              TEXT NOT NULL,
  chat_created_at                TEXT NOT NULL,
  last_seen_chat_updated_at      TEXT NOT NULL,
  last_exported_chat_updated_at  TEXT,
  coverage_from                  TEXT,
  coverage_until                 TEXT,
  status                         TEXT NOT NULL,
  last_error                     TEXT,
  updated_at                     TEXT NOT NULL,
  PRIMARY KEY (organization_uuid, chat_id)
);
CREATE INDEX IF NOT EXISTS idx_chat_user
  ON sync_chat_state (organization_uuid, user_id);
CREATE INDEX IF NOT EXISTS idx_chat_status
  ON sync_chat_state (organization_uuid, status);
"""


@dataclass(frozen=True)
class ChatState:
    chat_id: str
    user_id: str
    chat_created_at: datetime
    last_seen_chat_updated_at: datetime
    last_exported_chat_updated_at: datetime | None
    coverage_from: datetime | None
    coverage_until: datetime | None
    status: str
    last_error: str | None


@dataclass(frozen=True)
class UserState:
    user_id: str
    highest_completed_chat_updated_at: datetime | None
    coverage_from: datetime | None
    coverage_until: datetime | None
    last_successful_run_at: datetime | None


@dataclass(frozen=True)
class FetchInterval:
    created_at_gte: datetime | None
    created_at_lte: datetime


@dataclass(frozen=True)
class ChatWorkPlan:
    skip: bool
    intervals: tuple[FetchInterval, ...] = ()


def _row_ts(value: str | None) -> datetime | None:
    if value is None:
        return None
    return timestamp_str_to_datetime(value)


def _now_ts() -> str:
    return datetime_to_timestamp_str(datetime.now(tz=timezone.utc))


class SyncCache:
    def __init__(
        self, db_path: Path, organization_uuid: str, *, dry_run: bool
    ) -> None:
        self._organization_uuid = organization_uuid
        self._dry_run = dry_run
        if dry_run:
            self._conn = sqlite3.connect(":memory:")
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    def get_chat_state(self, chat_id: str) -> ChatState | None:
        row = self._conn.execute(
            """
            SELECT chat_id, user_id, chat_created_at, last_seen_chat_updated_at,
                   last_exported_chat_updated_at, coverage_from, coverage_until,
                   status, last_error
            FROM sync_chat_state
            WHERE organization_uuid = ? AND chat_id = ?
            """,
            (self._organization_uuid, chat_id),
        ).fetchone()
        if row is None:
            return None
        return ChatState(
            chat_id=row[0],
            user_id=row[1],
            chat_created_at=timestamp_str_to_datetime(row[2]),
            last_seen_chat_updated_at=timestamp_str_to_datetime(row[3]),
            last_exported_chat_updated_at=_row_ts(row[4]),
            coverage_from=_row_ts(row[5]),
            coverage_until=_row_ts(row[6]),
            status=row[7],
            last_error=row[8],
        )

    def plan_chat_work(
        self,
        chat: ChatSummary,
        requested_from: datetime | None,
        run_until: datetime,
    ) -> ChatWorkPlan:
        state = self.get_chat_state(chat.id)

        if state is not None and state.status == "deleted":
            return ChatWorkPlan(skip=True)

        if state is None:
            return ChatWorkPlan(
                skip=False,
                intervals=(FetchInterval(requested_from, run_until),),
            )

        changed = (
            state.last_exported_chat_updated_at is None
            or chat.updated_at > state.last_exported_chat_updated_at
        )
        backfill = (
            requested_from is not None
            and state.coverage_from is not None
            and requested_from < state.coverage_from
        )

        if not changed and not backfill:
            return ChatWorkPlan(skip=True)

        intervals: list[FetchInterval] = []
        if backfill and state.coverage_from is not None:
            intervals.append(FetchInterval(requested_from, state.coverage_from))
        if changed:
            tail_from = (
                state.coverage_until if state.coverage_until is not None else requested_from
            )
            intervals.append(FetchInterval(tail_from, run_until))

        return ChatWorkPlan(skip=False, intervals=tuple(intervals))

    def mark_chat_in_progress(
        self,
        chat: ChatSummary,
        requested_from: datetime | None,
        run_until: datetime,
    ) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO sync_chat_state (
              chat_id, user_id, organization_uuid, chat_created_at,
              last_seen_chat_updated_at, status, last_error, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'in_progress', NULL, ?)
            ON CONFLICT (organization_uuid, chat_id) DO UPDATE SET
              user_id = excluded.user_id,
              chat_created_at = excluded.chat_created_at,
              last_seen_chat_updated_at = excluded.last_seen_chat_updated_at,
              status = 'in_progress',
              last_error = NULL,
              updated_at = excluded.updated_at
            """,
            (
                chat.id,
                chat.user.id,
                self._organization_uuid,
                datetime_to_timestamp_str(chat.created_at),
                datetime_to_timestamp_str(chat.updated_at),
                now,
            ),
        )

    def mark_chat_completed(
        self,
        chat: ChatSummary,
        *,
        new_coverage_from: datetime | None,
        new_coverage_until: datetime,
    ) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            UPDATE sync_chat_state SET
              last_seen_chat_updated_at = ?,
              last_exported_chat_updated_at = ?,
              coverage_from = ?,
              coverage_until = ?,
              status = 'completed',
              last_error = NULL,
              updated_at = ?
            WHERE organization_uuid = ? AND chat_id = ?
            """,
            (
                datetime_to_timestamp_str(chat.updated_at),
                datetime_to_timestamp_str(chat.updated_at),
                datetime_to_timestamp_str(new_coverage_from)
                if new_coverage_from
                else None,
                datetime_to_timestamp_str(new_coverage_until),
                now,
                self._organization_uuid,
                chat.id,
            ),
        )

    def mark_chat_failed(
        self,
        chat_id: str,
        error: str,
        *,
        new_coverage_until: datetime | None = None,
    ) -> None:
        now = _now_ts()
        coverage_ts: str | None = None
        if new_coverage_until is not None:
            state = self.get_chat_state(chat_id)
            if state and state.coverage_until is not None:
                merged = max(state.coverage_until, new_coverage_until)
            else:
                merged = new_coverage_until
            coverage_ts = datetime_to_timestamp_str(merged)

        if coverage_ts is not None:
            self._conn.execute(
                """
                UPDATE sync_chat_state SET
                  status = 'failed',
                  last_error = ?,
                  coverage_until = ?,
                  updated_at = ?
                WHERE organization_uuid = ? AND chat_id = ?
                """,
                (error, coverage_ts, now, self._organization_uuid, chat_id),
            )
        else:
            self._conn.execute(
                """
                UPDATE sync_chat_state SET
                  status = 'failed',
                  last_error = ?,
                  updated_at = ?
                WHERE organization_uuid = ? AND chat_id = ?
                """,
                (error, now, self._organization_uuid, chat_id),
            )

    def mark_chat_deleted(self, chat_id: str, reason: str) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            UPDATE sync_chat_state SET
              status = 'deleted',
              last_error = ?,
              updated_at = ?
            WHERE organization_uuid = ? AND chat_id = ?
            """,
            (reason, now, self._organization_uuid, chat_id),
        )

    def mark_chat_skipped_extend(self, chat: ChatSummary, run_until: datetime) -> None:
        state = self.get_chat_state(chat.id)
        now = _now_ts()
        coverage_from = state.coverage_from if state else None
        coverage_until = run_until
        if state and state.coverage_until is not None:
            coverage_until = max(state.coverage_until, run_until)
        last_exported = (
            state.last_exported_chat_updated_at if state else chat.updated_at
        )
        if state is None or state.last_exported_chat_updated_at is None:
            last_exported = chat.updated_at
        self._conn.execute(
            """
            INSERT INTO sync_chat_state (
              chat_id, user_id, organization_uuid, chat_created_at,
              last_seen_chat_updated_at, last_exported_chat_updated_at,
              coverage_from, coverage_until, status, last_error, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', NULL, ?)
            ON CONFLICT (organization_uuid, chat_id) DO UPDATE SET
              last_seen_chat_updated_at = excluded.last_seen_chat_updated_at,
              coverage_until = excluded.coverage_until,
              updated_at = excluded.updated_at
            """,
            (
                chat.id,
                chat.user.id,
                self._organization_uuid,
                datetime_to_timestamp_str(chat.created_at),
                datetime_to_timestamp_str(chat.updated_at),
                datetime_to_timestamp_str(last_exported),
                datetime_to_timestamp_str(coverage_from) if coverage_from else None,
                datetime_to_timestamp_str(coverage_until),
                now,
            ),
        )

    def iter_unfinished_chats(self, user_id: str) -> list[str]:
        rows = self._conn.execute(
            """
            SELECT chat_id FROM sync_chat_state
            WHERE organization_uuid = ? AND user_id = ?
              AND status IN ('in_progress', 'failed')
            """,
            (self._organization_uuid, user_id),
        ).fetchall()
        return [row[0] for row in rows]

    def upsert_user_state(
        self,
        user_id: str,
        *,
        highest_completed_chat_updated_at: datetime | None,
        coverage_from: datetime | None,
        coverage_until: datetime | None,
        last_successful_run_at: datetime,
    ) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO sync_user_state (
              user_id, organization_uuid, highest_completed_chat_updated_at,
              coverage_from, coverage_until, last_successful_run_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (organization_uuid, user_id) DO UPDATE SET
              highest_completed_chat_updated_at = excluded.highest_completed_chat_updated_at,
              coverage_from = excluded.coverage_from,
              coverage_until = excluded.coverage_until,
              last_successful_run_at = excluded.last_successful_run_at,
              updated_at = excluded.updated_at
            """,
            (
                user_id,
                self._organization_uuid,
                datetime_to_timestamp_str(highest_completed_chat_updated_at)
                if highest_completed_chat_updated_at
                else None,
                datetime_to_timestamp_str(coverage_from) if coverage_from else None,
                datetime_to_timestamp_str(coverage_until) if coverage_until else None,
                datetime_to_timestamp_str(last_successful_run_at),
                now,
            ),
        )

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
