from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from compliance_sync.cache import SyncCache
from compliance_sync.models import ChatSummary, ChatUser

if TYPE_CHECKING:
    from pathlib import Path


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _chat(
    chat_id: str,
    *,
    updated_at: datetime,
    created_at: datetime | None = None,
) -> ChatSummary:
    return ChatSummary(
        id=chat_id,
        name=f"Chat {chat_id}",
        created_at=created_at or updated_at,
        updated_at=updated_at,
        href=f"https://example.com/chats/{chat_id}",
        model="claude-3-5-sonnet",
        organization_id="org_1",
        organization_uuid="org_demo",
        project_id="proj_1",
        user=ChatUser(id="user_1", email_address="user@example.com"),
    )


def _cache(
    tmp_path: Path, *, org: str = "org_demo", dry_run: bool = False
) -> SyncCache:
    return SyncCache(tmp_path / "sync_state.db", org, dry_run=dry_run)


def test_new_chat_plans_full_fetch(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    plan = cache.plan_chat_work(chat, _ts(8), run_until)

    assert not plan.skip
    assert len(plan.intervals) == 1
    assert plan.intervals[0].created_at_gte == _ts(8)
    assert plan.intervals[0].created_at_lte == run_until


def test_unchanged_chat_is_skipped(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=run_until,
    )
    cache.commit()

    plan = cache.plan_chat_work(chat, _ts(8), _ts(14))
    assert plan.skip


def test_skipped_extend_bumps_coverage_until(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=run_until,
    )
    cache.commit()

    cache.mark_chat_skipped_extend(chat, _ts(14))
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.coverage_until == _ts(14)
    assert state.status == "completed"


def test_changed_chat_fetches_tail_only(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=run_until,
    )
    cache.commit()

    updated = _chat("chat_1", updated_at=_ts(13))
    plan = cache.plan_chat_work(updated, _ts(8), _ts(14))

    assert not plan.skip
    assert len(plan.intervals) == 1
    assert plan.intervals[0].created_at_gte == run_until
    assert plan.intervals[0].created_at_lte == _ts(14)


def test_earlier_requested_from_triggers_backfill(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=run_until,
    )
    cache.commit()

    plan = cache.plan_chat_work(chat, _ts(6), _ts(14))

    assert not plan.skip
    assert len(plan.intervals) == 1
    assert plan.intervals[0].created_at_gte == _ts(6)
    assert plan.intervals[0].created_at_lte == _ts(8)


def test_backfill_and_changed_produce_two_intervals(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=run_until,
    )
    cache.commit()

    updated = _chat("chat_1", updated_at=_ts(13))
    plan = cache.plan_chat_work(updated, _ts(6), _ts(14))

    assert not plan.skip
    assert len(plan.intervals) == 2
    assert plan.intervals[0].created_at_gte == _ts(6)
    assert plan.intervals[0].created_at_lte == _ts(8)
    assert plan.intervals[1].created_at_gte == run_until
    assert plan.intervals[1].created_at_lte == _ts(14)


def test_status_transitions(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "in_progress"

    cache.mark_chat_failed("chat_1", "boom")
    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "failed"
    assert state.last_error == "boom"

    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=_ts(12),
    )
    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "completed"
    assert state.last_error is None


def test_iter_unfinished_chats(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat_a = _chat("chat_a", updated_at=_ts(10))
    chat_b = _chat("chat_b", updated_at=_ts(11))
    chat_c = _chat("chat_c", updated_at=_ts(12))

    cache.mark_chat_in_progress(chat_a)
    cache.mark_chat_in_progress(chat_b)
    cache.mark_chat_completed(
        chat_b,
        new_coverage_from=_ts(8),
        new_coverage_until=_ts(12),
    )
    cache.mark_chat_in_progress(chat_c)
    cache.mark_chat_failed("chat_c", "err")
    cache.commit()

    unfinished = cache.iter_unfinished_chats("user_1")
    assert set(unfinished) == {"chat_a", "chat_c"}


def test_org_isolation(tmp_path: Path) -> None:
    cache_a = _cache(tmp_path, org="org_a")
    cache_b = _cache(tmp_path, org="org_b")
    chat = _chat("chat_1", updated_at=_ts(10))

    cache_a.mark_chat_in_progress(chat)
    cache_a.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=_ts(12),
    )
    cache_a.commit()

    assert cache_b.get_chat_state("chat_1") is None
    plan = cache_b.plan_chat_work(chat, _ts(8), _ts(12))
    assert not plan.skip


def test_dry_run_uses_memory_and_persists_nothing(tmp_path: Path) -> None:
    cache = _cache(tmp_path, dry_run=True)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=_ts(12),
    )
    cache.commit()
    cache.close()

    assert not (tmp_path / "sync_state.db").exists()


def test_mark_chat_failed_persists_coverage_without_regressing(
    tmp_path: Path,
) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_failed("chat_1", "boom", new_coverage_until=_ts(9))
    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "failed"
    assert state.coverage_until == _ts(9)

    cache.mark_chat_failed("chat_1", "again", new_coverage_until=_ts(8))
    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.coverage_until == _ts(9)


def test_mark_chat_deleted_excluded_from_unfinished(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_deleted("chat_1", "gone")
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "deleted"
    assert state.last_error == "gone"
    assert cache.iter_unfinished_chats("user_1") == []


def test_skipped_extend_preserves_failed_status(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=run_until,
    )
    cache.mark_chat_failed("chat_1", "backfill failed")
    cache.commit()

    cache.mark_chat_skipped_extend(chat, _ts(14))
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "failed"
    assert state.last_error == "backfill failed"
    assert state.coverage_until == _ts(14)
    assert cache.iter_unfinished_chats("user_1") == ["chat_1"]


def test_skipped_extend_preserves_in_progress_status(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    run_until = _ts(12)

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=run_until,
    )
    cache.commit()

    cache.mark_chat_in_progress(chat)
    cache.commit()

    cache.mark_chat_skipped_extend(chat, _ts(14))
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "in_progress"
    assert cache.iter_unfinished_chats("user_1") == ["chat_1"]


def test_skipped_extend_preserves_deleted_status(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    cache.mark_chat_deleted("chat_1", "gone")
    cache.commit()

    cache.mark_chat_skipped_extend(chat, _ts(14))
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "deleted"
    assert state.last_error == "gone"


def test_chat_state_round_trips_composite_watermarks(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    cache.checkpoint_chat_coverage_until("chat_1", _ts(10), "a1")
    cache.checkpoint_chat_coverage_from("chat_1", _ts(7), "a0")
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.coverage_until == _ts(10)
    assert state.coverage_until_msg_id == "a1"
    assert state.coverage_from == _ts(7)
    assert state.coverage_from_msg_id == "a0"


def test_checkpoint_coverage_until_is_monotonic(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    cache.checkpoint_chat_coverage_until("chat_1", _ts(9), "a0")
    cache.checkpoint_chat_coverage_until("chat_1", _ts(10), "a1")
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.coverage_until == _ts(10)
    assert state.coverage_until_msg_id == "a1"


def test_mark_chat_completed_never_regresses_coverage_until(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))

    cache.mark_chat_in_progress(chat)
    cache.checkpoint_chat_coverage_until("chat_1", _ts(10), "a1")
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(8),
        new_coverage_until=_ts(7),
        new_coverage_until_msg_id="a0",
    )
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.coverage_until == _ts(10)
    assert state.coverage_until_msg_id == "a1"
    assert state.coverage_from_msg_id is None


def test_mark_chat_completed_preserves_coverage_from_msg_id(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    tie_ts = _ts(7)

    cache.mark_chat_in_progress(chat)
    cache.checkpoint_chat_coverage_from("chat_1", tie_ts, "a_high")
    cache.mark_chat_completed(
        chat,
        new_coverage_from=tie_ts,
        new_coverage_until=_ts(10),
        new_coverage_until_msg_id="a1",
    )
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.coverage_from == tie_ts
    assert state.coverage_from_msg_id == "a_high"


def test_mark_chat_completed_clears_coverage_from_msg_id_when_earlier_boundary(
    tmp_path: Path,
) -> None:
    cache = _cache(tmp_path)
    chat = _chat("chat_1", updated_at=_ts(10))
    tie_ts = _ts(7)

    cache.mark_chat_in_progress(chat)
    cache.checkpoint_chat_coverage_from("chat_1", tie_ts, "a_high")
    cache.mark_chat_completed(
        chat,
        new_coverage_from=_ts(6),
        new_coverage_until=_ts(10),
        new_coverage_until_msg_id="a1",
    )
    cache.commit()

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.coverage_from == _ts(6)
    assert state.coverage_from_msg_id is None
