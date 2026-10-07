from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from langfuse_sync.config import datetime_to_timestamp_str
from langfuse_sync.coverage import Coverage, CoverageState, plan_run

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


def test_forward_gap_detected() -> None:
    coverage = CoverageState(coverage_from=_ts(8), coverage_until=_ts(12))
    intervals, gap = plan_run(coverage, _ts(14), _ts(16))
    assert gap
    assert intervals == []


def test_save_and_load_round_trip(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.save(coverage_from=_ts(8), coverage_until=_ts(12))
    loaded = Coverage(tmp_path).load()
    assert loaded.coverage_from == _ts(8)
    assert loaded.coverage_until == _ts(12)


def test_dry_run_does_not_persist(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path, dry_run=True)
    coverage.save(coverage_from=_ts(8), coverage_until=_ts(12))
    assert not (tmp_path / "coverage.json").exists()


def test_advance_until_unions_ids_at_equal_timestamp(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.advance_until(_ts(12), "a")
    coverage.advance_until(_ts(12), "b")
    assert coverage.state.coverage_until == _ts(12)
    assert coverage.state.coverage_until_ids == frozenset({"a", "b"})


def test_advance_until_resets_ids_at_greater_timestamp(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.advance_until(_ts(12), "a")
    coverage.advance_until(_ts(13), "c")
    assert coverage.state.coverage_until == _ts(13)
    assert coverage.state.coverage_until_ids == frozenset({"c"})


def test_advance_until_persists_after_each_forward_step(tmp_path: Path) -> None:
    coverage = Coverage(tmp_path)
    coverage.advance_until(_ts(12), "trace-a")
    first = json.loads((tmp_path / "coverage.json").read_text(encoding="utf-8"))
    assert first["coverage_until_ids"] == ["trace-a"]

    coverage.advance_until(_ts(13), "trace-b")
    second = json.loads((tmp_path / "coverage.json").read_text(encoding="utf-8"))
    assert second["coverage_until"] == datetime_to_timestamp_str(_ts(13))
    assert second["coverage_until_ids"] == ["trace-b"]
    assert second["updated_at"] != first["updated_at"]


def test_advance_until_does_not_regress_watermark_for_older_traces(
    tmp_path: Path,
) -> None:
    coverage = Coverage(tmp_path)
    coverage.advance_until(_ts(12), "tail-done")
    snapshot = (tmp_path / "coverage.json").read_text(encoding="utf-8")

    coverage.advance_until(_ts(8), "backfill-trace")
    assert (tmp_path / "coverage.json").read_text(encoding="utf-8") == snapshot
