from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from compliance_sync.cache import SyncCache

if TYPE_CHECKING:
    from pathlib import Path


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _cache(
    tmp_path: Path, *, org: str = "org_demo", dry_run: bool = False
) -> SyncCache:
    return SyncCache(tmp_path / "sync_state.db", org, dry_run=dry_run)


def test_new_conversation_should_process(tmp_path: Path) -> None:
    cache = _cache(tmp_path)

    assert cache.should_process("chats", "c1", _ts(10), _ts(8)) is True


def test_unchanged_completed_conversation_is_skipped(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(8),
        status="completed",
    )

    assert cache.should_process("chats", "c1", _ts(10), _ts(8)) is False


def test_updated_at_change_should_process(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=None,
        status="completed",
    )

    assert cache.should_process("chats", "c1", _ts(12), None) is True


def test_pending_and_failed_should_process(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "pending_chat",
        updated_at_seen=_ts(10),
        coverage_from=None,
        status="pending",
    )
    cache.upsert_conversation(
        "chats",
        "failed_chat",
        updated_at_seen=_ts(10),
        coverage_from=_ts(8),
        status="failed",
        last_error="boom",
    )

    assert cache.should_process("chats", "pending_chat", _ts(10), None) is True
    assert cache.should_process("chats", "failed_chat", _ts(10), _ts(8)) is True


def test_gone_is_never_processed(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.mark_gone("chats", "c1", updated_at_seen=_ts(10), reason="deleted")

    assert cache.should_process("chats", "c1", _ts(10), None) is False
    assert cache.should_process("chats", "c1", _ts(14), _ts(1)) is False
    state = cache.get_conversation("chats", "c1")
    assert state is not None
    assert state.status == "gone"
    assert state.last_error == "deleted"


def test_backfill_when_requested_from_is_earlier_than_coverage(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(8),
        status="completed",
    )

    assert cache.should_process("chats", "c1", _ts(10), _ts(6)) is True


def test_full_coverage_does_not_backfill(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=None,
        status="completed",
    )

    assert cache.should_process("chats", "c1", _ts(10), _ts(1)) is False


def test_requesting_full_history_backfills_partial_coverage(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(8),
        status="completed",
    )

    assert cache.should_process("chats", "c1", _ts(10), None) is True


def test_coverage_from_none_means_full(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(8),
        status="completed",
    )
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(6),
        status="completed",
    )
    state = cache.get_conversation("chats", "c1")
    assert state is not None
    assert state.coverage_from == _ts(6)

    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(9),
        status="completed",
    )
    state = cache.get_conversation("chats", "c1")
    assert state is not None
    assert state.coverage_from == _ts(6)

    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=None,
        status="completed",
    )
    state = cache.get_conversation("chats", "c1")
    assert state is not None
    assert state.coverage_from is None

    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(1),
        status="completed",
    )
    state = cache.get_conversation("chats", "c1")
    assert state is not None
    assert state.coverage_from is None


def test_unclaimed_coverage_does_not_mean_full(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=None,
        status="failed",
        claim_coverage=False,
    )

    state = cache.get_conversation("chats", "c1")
    assert state is not None
    assert state.status == "failed"
    assert state.coverage_from is not None


def test_emitted_keys_are_idempotent_and_scoped(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    assert cache.is_emitted("chats", "c1", "k1") is False

    cache.record_emitted("chats", "c1", "k1", "sent")
    cache.record_emitted("chats", "c1", "k1", "rejected")
    cache.record_emitted("chats", "c1", "k2", "rejected")

    assert cache.is_emitted("chats", "c1", "k1") is True
    assert cache.is_emitted("chats", "c1", "k2") is True
    assert cache.is_emitted("chats", "c1", "k3") is False
    assert cache.is_emitted("local_sessions", "c1", "k1") is False
    assert cache.is_emitted("chats", "c2", "k1") is False

    count = cache._conn.execute(
        """
        SELECT COUNT(*) FROM sync_emitted_interaction
        WHERE organization_uuid = ? AND conversation_id = ? AND interaction_key = ?
        """,
        ("org_demo", "c1", "k1"),
    ).fetchone()
    assert count is not None
    assert count[0] == 1


def test_watermarks_are_per_source_and_org(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    other = _cache(tmp_path, org="org_b")

    assert cache.get_watermark("chats") is None
    cache.set_watermark("chats", _ts(10))
    cache.set_watermark("local_sessions", _ts(12))
    cache.commit()

    assert cache.get_watermark("chats") == _ts(10)
    assert cache.get_watermark("local_sessions") == _ts(12)
    assert cache.get_watermark("remote_sessions") is None
    assert other.get_watermark("chats") is None


def test_org_isolation(tmp_path: Path) -> None:
    cache_a = _cache(tmp_path, org="org_a")
    cache_b = _cache(tmp_path, org="org_b")
    cache_a.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(8),
        status="completed",
    )
    cache_a.record_emitted("chats", "c1", "k1", "sent")
    cache_a.commit()

    assert cache_b.get_conversation("chats", "c1") is None
    assert cache_b.is_emitted("chats", "c1", "k1") is False
    assert cache_b.should_process("chats", "c1", _ts(10), _ts(8)) is True


def test_iter_unfinished_returns_pending_and_failed_only(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    cache.upsert_conversation(
        "chats",
        "pending_one",
        updated_at_seen=_ts(10),
        coverage_from=None,
        status="pending",
    )
    cache.upsert_conversation(
        "chats",
        "failed_one",
        updated_at_seen=_ts(11),
        coverage_from=_ts(8),
        status="failed",
    )
    cache.upsert_conversation(
        "chats",
        "done",
        updated_at_seen=_ts(9),
        coverage_from=None,
        status="completed",
    )
    cache.mark_gone("chats", "gone_one", updated_at_seen=_ts(8), reason="deleted")
    cache.upsert_conversation(
        "local_sessions",
        "other_source",
        updated_at_seen=_ts(10),
        coverage_from=None,
        status="pending",
    )

    unfinished = cache.iter_unfinished("chats")
    assert {state.conversation_id for state in unfinished} == {
        "pending_one",
        "failed_one",
    }


def test_dry_run_uses_memory_and_persists_nothing(tmp_path: Path) -> None:
    cache = _cache(tmp_path, dry_run=True)
    cache.upsert_conversation(
        "chats",
        "c1",
        updated_at_seen=_ts(10),
        coverage_from=_ts(8),
        status="completed",
    )
    cache.set_watermark("chats", _ts(12))
    cache.commit()
    cache.close()

    assert not (tmp_path / "sync_state.db").exists()
