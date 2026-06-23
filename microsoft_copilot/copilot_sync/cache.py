from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

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
            intervals.append(FetchInterval(coverage.coverage_until, run_until))

        return tuple(intervals)

    def save_user_coverage(
        self,
        user_id: str,
        requested_from: datetime,
        run_until: datetime,
    ) -> None:
        existing = self.get_user_coverage(user_id)
        state_from = existing.coverage_from if existing else None
        state_until = existing.coverage_until if existing else None
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

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
