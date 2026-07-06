from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

if TYPE_CHECKING:
    from pathlib import Path

_COVERAGE_VERSION = 2
_CURSOR_FILENAME = "cursor.json"


@dataclass(frozen=True)
class CoverageState:
    coverage_from: datetime | None
    coverage_until: datetime | None


def _merge_coverage(
    state_from: datetime | None,
    state_until: datetime | None,
    new_from: datetime | None,
    new_until: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    from_candidates = [x for x in [state_from, new_from] if x is not None]
    merged_from = min(from_candidates) if from_candidates else None
    until_candidates = [x for x in [state_until, new_until] if x is not None]
    merged_until = max(until_candidates) if until_candidates else new_until
    return merged_from, merged_until


def plan_run(
    coverage: CoverageState | None,
    requested_from: datetime,
    requested_until: datetime,
) -> tuple[list[tuple[datetime, datetime]], bool]:
    if coverage is None or (
        coverage.coverage_from is None and coverage.coverage_until is None
    ):
        return [(requested_from, requested_until)], False

    cov_from = coverage.coverage_from
    cov_until = coverage.coverage_until

    if cov_until is not None and requested_from > cov_until:
        return [], True
    if cov_from is not None and requested_until < cov_from:
        return [], True

    intervals: list[tuple[datetime, datetime]] = []

    if cov_from is not None and requested_from < cov_from:
        intervals.append((requested_from, cov_from))
    elif cov_from is None and cov_until is not None and requested_from < cov_until:
        if requested_until <= cov_until:
            intervals.append((requested_from, requested_until))
        else:
            intervals.append((requested_from, cov_until))

    if cov_until is not None and requested_until > cov_until:
        intervals.append((cov_until, requested_until))

    return intervals, False


class Coverage:
    def __init__(self, cache_dir: Path, *, dry_run: bool = False) -> None:
        self._path = cache_dir / "coverage.json"
        self._cursor_path = cache_dir / _CURSOR_FILENAME
        self._dry_run = dry_run
        self._state = CoverageState(coverage_from=None, coverage_until=None)

    @property
    def state(self) -> CoverageState:
        return self._state

    def has_coverage(self) -> bool:
        return (
            self._state.coverage_from is not None
            or self._state.coverage_until is not None
        )

    def load(self) -> CoverageState:
        if self._path.exists():
            data = json.loads(self._path.read_text(encoding="utf-8"))
            cov_from = data.get("coverage_from")
            cov_until = data.get("coverage_until")
            self._state = CoverageState(
                coverage_from=(
                    timestamp_str_to_datetime(cov_from) if cov_from else None
                ),
                coverage_until=(
                    timestamp_str_to_datetime(cov_until) if cov_until else None
                ),
            )
            return self._state

        if self._cursor_path.exists():
            data = json.loads(self._cursor_path.read_text(encoding="utf-8"))
            last_ts = data.get("last_timestamp")
            self._state = CoverageState(
                coverage_from=None,
                coverage_until=(
                    timestamp_str_to_datetime(last_ts) if last_ts else None
                ),
            )
            return self._state

        self._state = CoverageState(coverage_from=None, coverage_until=None)
        return self._state

    def invalidate(self) -> None:
        self._state = CoverageState(coverage_from=None, coverage_until=None)
        if self._dry_run:
            return
        if self._path.exists():
            self._path.unlink()
        if self._cursor_path.exists():
            self._cursor_path.unlink()

    def save(
        self,
        *,
        coverage_from: datetime | None = None,
        coverage_until: datetime | None = None,
    ) -> None:
        merged_from, merged_until = _merge_coverage(
            self._state.coverage_from,
            self._state.coverage_until,
            coverage_from,
            coverage_until,
        )
        self._state = CoverageState(
            coverage_from=merged_from,
            coverage_until=merged_until,
        )
        if self._dry_run:
            return
        self._write()

    def advance_until(self, until: datetime) -> None:
        if until.tzinfo is None:
            until = until.replace(tzinfo=UTC)
        else:
            until = until.astimezone(UTC)
        self.save(coverage_until=until)

    def _write(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": _COVERAGE_VERSION,
            "coverage_from": (
                datetime_to_timestamp_str(self._state.coverage_from)
                if self._state.coverage_from is not None
                else None
            ),
            "coverage_until": (
                datetime_to_timestamp_str(self._state.coverage_until)
                if self._state.coverage_until is not None
                else None
            ),
            "updated_at": datetime_to_timestamp_str(datetime.now(UTC)),
        }
        tmp_path = self._path.with_suffix(".json.tmp")
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            os.close(fd)
            raise
        os.replace(tmp_path, self._path)
