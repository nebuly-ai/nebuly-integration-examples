from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from copilot_sync.cache import SyncCache

if TYPE_CHECKING:
    from pathlib import Path


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _cache(
    tmp_path: Path,
    *,
    tenant: str = "tenant_1",
    dry_run: bool = False,
) -> SyncCache:
    return SyncCache(tmp_path / "sync_state.db", tenant, dry_run=dry_run)


def test_no_coverage_plans_full_interval(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    intervals = cache.plan_intervals(None, _ts(8), _ts(12))

    assert len(intervals) == 1
    assert intervals[0].gte == _ts(8)
    assert intervals[0].lte == _ts(12)


def test_covered_window_plans_nothing(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.commit()

    coverage = cache.get_user_coverage("user_1")
    assert coverage is not None
    intervals = cache.plan_intervals(coverage, _ts(8), _ts(12))
    assert intervals == ()


def test_backfill_interval(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.commit()

    coverage = cache.get_user_coverage("user_1")
    assert coverage is not None
    intervals = cache.plan_intervals(coverage, _ts(6), _ts(12))

    assert len(intervals) == 1
    assert intervals[0].gte == _ts(6)
    assert intervals[0].lte == _ts(8)


def test_tail_interval(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.commit()

    coverage = cache.get_user_coverage("user_1")
    assert coverage is not None
    intervals = cache.plan_intervals(coverage, _ts(8), _ts(14))

    assert len(intervals) == 1
    assert intervals[0].gte == _ts(12) + timedelta(microseconds=1)
    assert intervals[0].lte == _ts(14)


def test_backfill_and_tail_produce_two_intervals(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.commit()

    coverage = cache.get_user_coverage("user_1")
    assert coverage is not None
    intervals = cache.plan_intervals(coverage, _ts(6), _ts(14))

    assert len(intervals) == 2
    assert intervals[0].gte == _ts(6)
    assert intervals[0].lte == _ts(8)
    assert intervals[1].gte == _ts(12) + timedelta(microseconds=1)
    assert intervals[1].lte == _ts(14)


def test_save_merges_coverage_window(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.save_user_coverage("user_1", _ts(6), _ts(14))
    cache.commit()

    coverage = cache.get_user_coverage("user_1")
    assert coverage is not None
    assert coverage.coverage_from == _ts(6)
    assert coverage.coverage_until == _ts(14)


def test_min_coverage_from_across_users(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.save_user_coverage("user_2", _ts(10), _ts(14))
    cache.commit()

    assert cache.min_coverage_from() == _ts(8)


def test_per_user_coverage_isolated(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.commit()

    assert cache.get_user_coverage("user_2") is None
    assert cache.get_user_coverage("user_1") is not None


def test_dry_run_uses_memory_and_persists_nothing(tmp_path: Path) -> None:
    cache = _cache(tmp_path, dry_run=True)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.commit()
    cache.close()

    assert not (tmp_path / "sync_state.db").exists()


def test_has_any_coverage(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    assert not cache.has_any_coverage()

    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.commit()
    assert cache.has_any_coverage()


def test_save_with_hold_back_reopens_gap_below_existing_until(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.save_user_coverage("user_1", _ts(6), _ts(14), hold_back=_ts(6, 30))
    cache.commit()

    coverage = cache.get_user_coverage("user_1")
    assert coverage is not None
    assert coverage.coverage_from == _ts(8)
    assert coverage.coverage_until == _ts(6, 30) - timedelta(microseconds=1)


def test_save_with_hold_back_advances_from_when_hold_back_after_existing_from(
    tmp_path: Path,
) -> None:
    cache = _cache(tmp_path)
    cache.save_user_coverage("user_1", _ts(8), _ts(12))
    cache.save_user_coverage("user_1", _ts(6), _ts(14), hold_back=_ts(13))
    cache.commit()

    coverage = cache.get_user_coverage("user_1")
    assert coverage is not None
    assert coverage.coverage_from == _ts(6)
    assert coverage.coverage_until == _ts(13) - timedelta(microseconds=1)
