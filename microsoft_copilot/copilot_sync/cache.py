from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime
from .models import CopilotAuditRecord

if TYPE_CHECKING:
    from pathlib import Path


def _merge_coverage(
    state_from: datetime | None,
    state_until: datetime | None,
    requested_from: datetime | None,
    run_until: datetime,
) -> tuple[datetime | None, datetime]:
    from_candidates = [x for x in [state_from, requested_from] if x is not None]
    new_from = min(from_candidates) if from_candidates else None
    until_candidates = [x for x in [state_until, run_until] if x is not None]
    new_until = max(until_candidates) if until_candidates else run_until
    return new_from, new_until


def _merge_coverage_with_hold_back(
    state_from: datetime | None,
    requested_from: datetime,
    hold_back: datetime,
) -> tuple[datetime | None, datetime]:
    new_until = hold_back - timedelta(microseconds=1)
    if state_from is None or hold_back >= state_from:
        from_candidates = [x for x in [state_from, requested_from] if x is not None]
        new_from = min(from_candidates) if from_candidates else None
    else:
        new_from = state_from
    return new_from, new_until


_SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_user_coverage (
  tenant_id              TEXT NOT NULL,
  user_id                TEXT NOT NULL,
  coverage_from          TEXT,
  coverage_until         TEXT,
  last_successful_run_at TEXT,
  updated_at             TEXT NOT NULL,
  PRIMARY KEY (tenant_id, user_id)
);
CREATE TABLE IF NOT EXISTS copilot_audit_messages (
  tenant_id    TEXT NOT NULL,
  message_id   TEXT NOT NULL,
  thread_id    TEXT,
  created_at   TEXT NOT NULL,
  record_json  TEXT NOT NULL,
  PRIMARY KEY (tenant_id, message_id)
);
CREATE TABLE IF NOT EXISTS sync_audit_day_chunks (
  tenant_id    TEXT NOT NULL,
  chunk_start  TEXT NOT NULL,
  chunk_end    TEXT NOT NULL,
  PRIMARY KEY (tenant_id, chunk_start)
);
CREATE TABLE IF NOT EXISTS sync_user_interaction_denied (
  tenant_id   TEXT NOT NULL,
  user_id     TEXT NOT NULL,
  reason      TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  PRIMARY KEY (tenant_id, user_id)
);
"""


@dataclass(frozen=True)
class UserCoverage:
    user_id: str
    coverage_from: datetime | None
    coverage_until: datetime | None
    last_successful_run_at: datetime | None


@dataclass(frozen=True)
class FetchInterval:
    gte: datetime
    lte: datetime


def _row_ts(value: str | None) -> datetime | None:
    if value is None:
        return None
    return timestamp_str_to_datetime(value)


def _now_ts() -> str:
    return datetime_to_timestamp_str(datetime.now(tz=UTC))


class SyncCache:
    def __init__(self, db_path: Path, tenant_id: str, *, dry_run: bool) -> None:
        self._tenant_id = tenant_id
        self._dry_run = dry_run
        if dry_run:
            self._conn = sqlite3.connect(":memory:")
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    def get_user_coverage(self, user_id: str) -> UserCoverage | None:
        row = self._conn.execute(
            """
            SELECT user_id, coverage_from, coverage_until, last_successful_run_at
            FROM sync_user_coverage
            WHERE tenant_id = ? AND user_id = ?
            """,
            (self._tenant_id, user_id),
        ).fetchone()
        if row is None:
            return None
        return UserCoverage(
            user_id=row[0],
            coverage_from=_row_ts(row[1]),
            coverage_until=_row_ts(row[2]),
            last_successful_run_at=_row_ts(row[3]),
        )

    def min_coverage_from(self) -> datetime | None:
        row = self._conn.execute(
            """
            SELECT MIN(coverage_from) FROM sync_user_coverage
            WHERE tenant_id = ? AND coverage_from IS NOT NULL
            """,
            (self._tenant_id,),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return timestamp_str_to_datetime(row[0])

    def has_any_coverage(self) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM sync_user_coverage
            WHERE tenant_id = ? LIMIT 1
            """,
            (self._tenant_id,),
        ).fetchone()
        return row is not None

    def plan_intervals(
        self,
        coverage: UserCoverage | None,
        requested_from: datetime,
        run_until: datetime,
    ) -> tuple[FetchInterval, ...]:
        if coverage is None:
            return (FetchInterval(requested_from, run_until),)

        intervals: list[FetchInterval] = []
        if (
            coverage.coverage_from is not None
            and requested_from < coverage.coverage_from
        ):
            intervals.append(FetchInterval(requested_from, coverage.coverage_from))
        if coverage.coverage_until is not None and run_until > coverage.coverage_until:
            intervals.append(
                FetchInterval(
                    coverage.coverage_until + timedelta(microseconds=1), run_until
                )
            )

        return tuple(intervals)

    def save_user_coverage(
        self,
        user_id: str,
        requested_from: datetime,
        run_until: datetime,
        *,
        hold_back: datetime | None = None,
    ) -> None:
        existing = self.get_user_coverage(user_id)
        state_from = existing.coverage_from if existing else None
        state_until = existing.coverage_until if existing else None
        if hold_back is not None:
            new_from, new_until = _merge_coverage_with_hold_back(
                state_from,
                requested_from,
                hold_back,
            )
        else:
            new_from, new_until = _merge_coverage(
                state_from,
                state_until,
                requested_from,
                run_until,
            )
        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO sync_user_coverage (
              tenant_id, user_id, coverage_from, coverage_until,
              last_successful_run_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (tenant_id, user_id) DO UPDATE SET
              coverage_from = excluded.coverage_from,
              coverage_until = excluded.coverage_until,
              last_successful_run_at = excluded.last_successful_run_at,
              updated_at = excluded.updated_at
            """,
            (
                self._tenant_id,
                user_id,
                datetime_to_timestamp_str(new_from) if new_from else None,
                datetime_to_timestamp_str(new_until),
                now,
                now,
            ),
        )

    def is_interaction_denied(self, user_id: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM sync_user_interaction_denied
            WHERE tenant_id = ? AND user_id = ?
            """,
            (self._tenant_id, user_id),
        ).fetchone()
        return row is not None

    def mark_interaction_denied(self, user_id: str, *, reason: str) -> None:
        self._conn.execute(
            """
            INSERT INTO sync_user_interaction_denied (
              tenant_id, user_id, reason, updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT (tenant_id, user_id) DO UPDATE SET
              reason = excluded.reason,
              updated_at = excluded.updated_at
            """,
            (self._tenant_id, user_id, reason, _now_ts()),
        )

    def commit(self) -> None:
        self._conn.commit()

    def has_audit_day_chunk(self, chunk_start: datetime) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM sync_audit_day_chunks
            WHERE tenant_id = ? AND chunk_start = ?
            """,
            (self._tenant_id, datetime_to_timestamp_str(chunk_start)),
        ).fetchone()
        return row is not None

    def mark_audit_day_chunk(self, chunk_start: datetime, chunk_end: datetime) -> None:
        self._conn.execute(
            """
            INSERT INTO sync_audit_day_chunks (tenant_id, chunk_start, chunk_end)
            VALUES (?, ?, ?)
            ON CONFLICT (tenant_id, chunk_start) DO UPDATE SET
              chunk_end = excluded.chunk_end
            """,
            (
                self._tenant_id,
                datetime_to_timestamp_str(chunk_start),
                datetime_to_timestamp_str(chunk_end),
            ),
        )

    def upsert_audit_records(self, records: list[CopilotAuditRecord]) -> None:
        for record in records:
            record_json = json.dumps(record.model_dump(mode="json"))
            created_at = datetime_to_timestamp_str(record.created_datetime)
            thread_id = record.thread_id
            for message_id in record.message_ids:
                self._conn.execute(
                    """
                    INSERT INTO copilot_audit_messages (
                      tenant_id, message_id, thread_id, created_at, record_json
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (tenant_id, message_id) DO UPDATE SET
                      thread_id = excluded.thread_id,
                      created_at = excluded.created_at,
                      record_json = excluded.record_json
                    """,
                    (
                        self._tenant_id,
                        message_id,
                        thread_id,
                        created_at,
                        record_json,
                    ),
                )

    def find_audit_record(self, message_ids: list[str]) -> CopilotAuditRecord | None:
        for message_id in message_ids:
            if not message_id:
                continue
            row = self._conn.execute(
                """
                SELECT record_json FROM copilot_audit_messages
                WHERE tenant_id = ? AND message_id = ?
                """,
                (self._tenant_id, message_id),
            ).fetchone()
            if row is None:
                continue
            return CopilotAuditRecord.model_validate(json.loads(row[0]))
        return None

    def close(self) -> None:
        self._conn.close()
