from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gemini_enterprise_sync.coverage import Coverage, CoverageState, plan_run

if TYPE_CHECKING:
    from pathlib import Path


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 7, 2, hour, minute, tzinfo=UTC)


def test_no_coverage_plans_full_interval() -> None:
    intervals, gap = plan_run(None, _ts(8), _ts(12))
    assert not gap
    assert intervals == [(_ts(8), _ts(12))]


def test_covered_window_plans_nothing() -> None:
    coverage = CoverageState(coverage_from=_ts(8), coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(8), _ts(12))
    assert not gap
    assert intervals == []


def test_backfill_interval() -> None:
    coverage = CoverageState(coverage_from=_ts(8), coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(6), _ts(12))
    assert not gap
    assert intervals == [(_ts(6), _ts(8))]


def test_tail_interval() -> None:
    coverage = CoverageState(coverage_from=_ts(8), coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(8), _ts(14))
    assert not gap
    assert intervals == [(_ts(12), _ts(14))]


def test_backfill_and_tail_produce_two_intervals() -> None:
    coverage = CoverageState(coverage_from=_ts(8), coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(6), _ts(14))
    assert not gap
    assert intervals == [(_ts(6), _ts(8)), (_ts(12), _ts(14))]


def test_forward_gap_detected() -> None:
    coverage = CoverageState(coverage_from=_ts(8), coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(14), _ts(16))
    assert gap
    assert intervals == []


def test_backward_gap_detected() -> None:
    coverage = CoverageState(coverage_from=_ts(8), coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(4), _ts(6))
    assert gap
    assert intervals == []


def test_migrated_cursor_backfill_without_coverage_from() -> None:
    coverage = CoverageState(coverage_from=None, coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(6), _ts(10))
    assert not gap
    assert intervals == [(_ts(6), _ts(10))]


def test_save_and_load_round_trip(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.save(coverage_from=_ts(8), coverage_until=_ts(12))
    loaded = Coverage(tmp_path).load()
    assert loaded.coverage_from == _ts(8)
    assert loaded.coverage_until == _ts(12)


def test_migrates_legacy_cursor_json(tmp_path: Path) -> None:
    cursor_path = tmp_path / "cursor.json"
    cursor_path.write_text(
        json.dumps(
            {
                "version": 1,
                "last_timestamp": "2026-07-02T12:00:00Z",
                "last_insert_id": "abc",
            }
        ),
        encoding="utf-8",
    )
    loaded = Coverage(tmp_path).load()
    assert loaded.coverage_from is None
    assert loaded.coverage_until == _ts(12)
    assert loaded.coverage_until_ids == frozenset({"abc"})


def test_invalidate_clears_state_and_files(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.save(coverage_from=_ts(8), coverage_until=_ts(12))
    coverage.invalidate()
    assert not coverage.has_coverage()
    assert not (tmp_path / "coverage.json").exists()


def test_coverage_until_ids_round_trip(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.save(
        coverage_from=_ts(8),
        coverage_until=_ts(12),
        coverage_until_ids=["a", "b"],
    )
    loaded = Coverage(tmp_path).load()
    assert loaded.coverage_until_ids == frozenset({"a", "b"})


def test_advance_until_unions_ids_at_equal_timestamp(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.advance_until(_ts(12), "a")
    coverage.advance_until(_ts(12), "b")
    assert coverage.state.coverage_until == _ts(12)
    assert coverage.state.coverage_until_ids == frozenset({"a", "b"})


def test_advance_until_resets_ids_at_greater_timestamp(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.advance_until(_ts(12), "a")
    coverage.advance_until(_ts(12), "b")
    coverage.advance_until(_ts(13), "c")
    assert coverage.state.coverage_until == _ts(13)
    assert coverage.state.coverage_until_ids == frozenset({"c"})
