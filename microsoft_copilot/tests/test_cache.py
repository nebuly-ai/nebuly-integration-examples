from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from copilot_sync.cache import SyncCache
from copilot_sync.models import CopilotAuditRecord

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


def _audit_record(
    *,
    message_ids: tuple[str, ...],
    thread_id: str = "19:thread@thread.v2",
) -> CopilotAuditRecord:
    return CopilotAuditRecord.model_validate(
        {
            "id": "audit-cache-1",
            "createdDateTime": "2025-06-15T10:00:00Z",
            "auditData": {
                "ThreadId": thread_id,
                "Messages": [
                    {"Id": mid, "isPrompt": i == 0} for i, mid in enumerate(message_ids)
                ],
            },
        },
    )


def test_upsert_audit_record_lookup_by_response_and_prompt_id(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    record = _audit_record(message_ids=("prompt-id", "response-id"))
    cache.upsert_audit_records([record])
    cache.commit()

    by_response = cache.find_audit_record(["response-id"])
    assert cache.find_audit_record(["prompt-id"]) is not None
    assert by_response is not None
    assert by_response.thread_id == "19:thread@thread.v2"


def test_audit_record_tenant_isolation(tmp_path: Path) -> None:
    cache_a = _cache(tmp_path, tenant="tenant_a")
    cache_b = _cache(tmp_path, tenant="tenant_b")
    record = _audit_record(message_ids=("msg-1",))
    cache_a.upsert_audit_records([record])
    cache_a.commit()
    cache_b.commit()

    assert cache_a.find_audit_record(["msg-1"]) is not None
    assert cache_b.find_audit_record(["msg-1"]) is None
    cache_a.close()
    cache_b.close()


def test_upsert_audit_record_idempotent(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    record = _audit_record(message_ids=("msg-1",))
    cache.upsert_audit_records([record])
    cache.upsert_audit_records([record])
    cache.commit()

    assert cache.find_audit_record(["msg-1"]) is not None


def test_audit_day_chunk_tracking(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    start = datetime(2025, 6, 15, 8, 0, tzinfo=UTC)
    end = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)
    assert cache.has_audit_day_chunk(start) is False
    cache.mark_audit_day_chunk(start, end)
    cache.commit()
    assert cache.has_audit_day_chunk(start) is True


def test_interaction_denied_persists(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    assert cache.is_interaction_denied("user-x") is False
    cache.mark_interaction_denied("user-x", reason="interaction_403")
    cache.commit()
    assert cache.is_interaction_denied("user-x") is True
