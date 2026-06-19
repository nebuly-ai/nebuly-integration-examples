from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from compliance_sync.cache import SyncCache
from compliance_sync.config import Config, timestamp_str_to_datetime
from compliance_sync.models import (
    ChatMessage,
    ChatMessagesResponse,
    ChatSummary,
    ChatUser,
    PaginatedChatsResponse,
    TextContent,
)
from compliance_sync.sync import _sync_user

UTC = timezone.utc


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _chat_summary(
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


def _message(msg_id: str, role: str, created_at: datetime, text: str) -> ChatMessage:
    return ChatMessage(
        id=msg_id,
        role=role,  # type: ignore[arg-type]
        created_at=created_at,
        content=[TextContent(type="text", text=text)],
    )


def _chat_messages_response(
    chat: ChatSummary,
    messages: list[ChatMessage],
) -> ChatMessagesResponse:
    return ChatMessagesResponse(
        id=chat.id,
        name=chat.name,
        created_at=chat.created_at,
        updated_at=chat.updated_at,
        href=chat.href,
        model=chat.model,
        organization_id=chat.organization_id,
        organization_uuid=chat.organization_uuid,
        project_id=chat.project_id,
        user=chat.user,
        chat_messages=messages,
        has_more=False,
    )


class FakeComplianceClient:
    def __init__(
        self,
        chats: list[ChatSummary],
        messages_by_chat: dict[str, list[ChatMessage]],
        *,
        chat_page_size: int = 100,
    ) -> None:
        self._chats = chats
        self._messages_by_chat = messages_by_chat
        self._chat_page_size = chat_page_size
        self.message_fetch_count = 0
        self.messages_404_for: set[str] = set()

    def list_chats(
        self,
        user_ids: list[str],  # noqa: ARG002
        *,
        updated_at_gte: str | None = None,
        updated_at_lte: str | None = None,
        after_id: str | None = None,
        limit: int = 100,
    ) -> PaginatedChatsResponse:
        chats = sorted(self._chats, key=lambda c: c.id)
        if updated_at_gte is not None:
            gte = timestamp_str_to_datetime(updated_at_gte)
            chats = [c for c in chats if c.updated_at >= gte]
        if updated_at_lte is not None:
            lte = timestamp_str_to_datetime(updated_at_lte)
            chats = [c for c in chats if c.updated_at <= lte]
        if after_id is not None:
            ids = [c.id for c in chats]
            try:
                start = ids.index(after_id) + 1
                chats = chats[start:]
            except ValueError:
                chats = []
        page_size = min(limit, self._chat_page_size, 100)
        page = chats[:page_size]
        has_more = len(chats) > page_size
        return PaginatedChatsResponse(
            data=page,
            has_more=has_more,
            first_id=page[0].id if page else None,
            last_id=page[-1].id if page else None,
        )

    def list_chat_messages(
        self,
        chat_id: str,
        *,
        created_at_gte: str | None = None,
        created_at_lte: str | None = None,
        after_id: str | None = None,  # noqa: ARG002
        order: str = "asc",  # noqa: ARG002
        limit: int = 1000,  # noqa: ARG002
    ) -> ChatMessagesResponse:
        if chat_id in self.messages_404_for:
            import httpx

            request = httpx.Request("GET", f"/chats/{chat_id}/messages")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("Not found", request=request, response=response)
        self.message_fetch_count += 1
        chat = next(c for c in self._chats if c.id == chat_id)
        messages = list(self._messages_by_chat[chat_id])
        if created_at_gte is not None:
            gte = timestamp_str_to_datetime(created_at_gte)
            messages = [m for m in messages if m.created_at >= gte]
        if created_at_lte is not None:
            lte = timestamp_str_to_datetime(created_at_lte)
            messages = [m for m in messages if m.created_at <= lte]
        return _chat_messages_response(chat, messages)


class FakeNebulyClient:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.sent: list[dict[str, Any]] = []
        self._fail_after = fail_after

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if self._fail_after is not None and len(self.sent) >= self._fail_after:
            import httpx

            request = httpx.Request("POST", "/events")
            response = httpx.Response(500, request=request)
            raise httpx.HTTPStatusError(
                "Server error", request=request, response=response
            )
        self.sent.append(payload)


class CrashNebulyClient:
    """Raises a non-HTTPStatusError after K successful sends (simulates SIGKILL)."""

    def __init__(self, *, crash_after: int) -> None:
        self.sent: list[dict[str, Any]] = []
        self._crash_after = crash_after

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if len(self.sent) >= self._crash_after:
            raise RuntimeError("simulated process crash")
        self.sent.append(payload)


def _config(tmp_path: Path, *, from_date: datetime | None = None) -> Config:
    return Config(
        nebuly_api_key="key",
        nebuly_endpoint="https://example.com/events",
        compliance_api_key="key",
        compliance_base_url="https://example.com",
        organization_uuid="org_demo",
        compliance_max_requests_per_minute=600,
        anonymize=False,
        from_date=from_date,
        to_date=_ts(16),
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
        safety_lag_minutes=5,
    )


def test_overlapping_chats_do_not_false_positive_skip(tmp_path: Path) -> None:
    chat_a = _chat_summary("chat_a", updated_at=_ts(14, 31), created_at=_ts(14, 0))
    chat_b = _chat_summary("chat_b", updated_at=_ts(15, 1), created_at=_ts(14, 10))

    messages_by_chat = {
        "chat_a": [
            _message("a_u1", "user", _ts(14, 0), "hello A"),
            _message("a_a1", "assistant", _ts(14, 1), "hi A"),
            _message("a_u2", "user", _ts(14, 30), "more A"),
            _message("a_a2", "assistant", _ts(14, 31), "reply A"),
        ],
        "chat_b": [
            _message("b_u1", "user", _ts(14, 10), "hello B"),
            _message("b_a1", "assistant", _ts(14, 11), "hi B"),
            _message("b_u2", "user", _ts(15, 0), "more B"),
            _message("b_a2", "assistant", _ts(15, 1), "reply B"),
        ],
    }

    compliance = FakeComplianceClient([chat_a, chat_b], messages_by_chat)
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path)

    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )

    assert counts.fetched == 4
    assert counts.skipped == 0
    assert counts.chats_processed == 2
    assert counts.chats_skipped == 0
    assert counts.sent == 4
    assert counts.failed == 0
    assert len(nebuly.sent) == 4


def test_second_run_with_no_changes_sends_and_fetches_nothing(
    tmp_path: Path,
) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages_by_chat = {
        "chat_1": [
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }

    compliance = FakeComplianceClient([chat], messages_by_chat)
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=_ts(8))

    _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )

    compliance.message_fetch_count = 0
    nebuly.sent.clear()

    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=_ts(17),
    )

    assert counts.sent == 0
    assert counts.fetched == 0
    assert counts.chats_processed == 0
    assert counts.chats_skipped == 1
    assert counts.skipped == 0
    assert compliance.message_fetch_count == 0
    assert len(nebuly.sent) == 0

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "completed"


def test_updated_chat_sends_only_new_pair(tmp_path: Path) -> None:
    chat_v1 = _chat_summary("chat_1", updated_at=_ts(10))
    messages_v1 = {
        "chat_1": [
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }

    compliance = FakeComplianceClient([chat_v1], messages_v1)
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=_ts(8))

    _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )
    assert len(nebuly.sent) == 1

    chat_v2 = _chat_summary("chat_1", updated_at=_ts(12))
    compliance._chats = [chat_v2]
    compliance._messages_by_chat = {
        "chat_1": messages_v1["chat_1"]
        + [
            _message("u2", "user", _ts(11), "more"),
            _message("a2", "assistant", _ts(12), "again"),
        ],
    }
    compliance.message_fetch_count = 0
    nebuly.sent.clear()

    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=_ts(17),
    )

    assert counts.sent == 1
    assert counts.fetched == 1
    assert compliance.message_fetch_count == 1
    assert len(nebuly.sent) == 1


def test_backfill_fetches_only_earlier_window(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages_by_chat = {
        "chat_1": [
            _message("u0", "user", _ts(7), "early"),
            _message("a0", "assistant", _ts(7, 30), "early reply"),
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }

    compliance = FakeComplianceClient([chat], messages_by_chat)
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=_ts(8))

    _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )
    assert len(nebuly.sent) == 1

    nebuly.sent.clear()
    compliance.message_fetch_count = 0
    config_backfill = _config(tmp_path, from_date=_ts(6))

    counts = _sync_user(
        user_id="user_1",
        config=config_backfill,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=_ts(17),
    )

    assert counts.sent == 1
    assert counts.fetched == 1
    assert compliance.message_fetch_count == 1


def test_unfinished_chat_on_recovery_page_two_is_processed(tmp_path: Path) -> None:
    chat_in_window = _chat_summary("chat_a", updated_at=_ts(10))
    chat_stale = _chat_summary("chat_b", updated_at=_ts(5))
    messages_by_chat = {
        "chat_a": [
            _message("a_u1", "user", _ts(9), "hello A"),
            _message("a_a1", "assistant", _ts(10), "hi A"),
        ],
        "chat_b": [
            _message("b_u1", "user", _ts(9), "hello B"),
            _message("b_a1", "assistant", _ts(10), "hi B"),
        ],
    }

    compliance = FakeComplianceClient(
        [chat_in_window, chat_stale],
        messages_by_chat,
        chat_page_size=1,
    )
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    cache.mark_chat_in_progress(chat_stale)
    cache.commit()

    config = _config(tmp_path, from_date=_ts(8))
    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )

    assert counts.chats_processed == 2
    assert counts.sent == 2
    state = cache.get_chat_state("chat_b")
    assert state is not None
    assert state.status == "completed"


def test_404_on_messages_marks_deleted_and_skips_retry(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages_by_chat = {
        "chat_1": [
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }

    compliance = FakeComplianceClient([chat], messages_by_chat)
    compliance.messages_404_for.add("chat_1")
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=_ts(8))

    _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )

    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "deleted"
    assert "404" in (state.last_error or "")

    compliance.message_fetch_count = 0
    nebuly.sent.clear()
    counts_retry = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=_ts(17),
    )

    assert counts_retry.chats_processed == 0
    assert compliance.message_fetch_count == 0
    assert len(nebuly.sent) == 0


def test_send_failure_persists_coverage_and_retries_tail_only(
    tmp_path: Path,
) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(12))
    messages_by_chat = {
        "chat_1": [
            _message("u1", "user", _ts(9), "one"),
            _message("a1", "assistant", _ts(10), "reply one"),
            _message("u2", "user", _ts(11), "two"),
            _message("a2", "assistant", _ts(12), "reply two"),
        ],
    }

    compliance = FakeComplianceClient([chat], messages_by_chat)
    nebuly = FakeNebulyClient(fail_after=1)
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=_ts(8))

    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )

    assert counts.sent == 1
    assert counts.failed == 1
    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "failed"
    assert state.coverage_until == _ts(10)

    nebuly_ok = FakeNebulyClient()
    compliance.message_fetch_count = 0
    counts_retry = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly_ok,  # type: ignore[arg-type]
        cache=cache,
        run_until=_ts(17),
    )

    assert counts_retry.sent == 1
    assert counts_retry.fetched == 1
    assert compliance.message_fetch_count == 1
    state = cache.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "completed"


def test_crash_mid_chat_does_not_replay_sent_interactions(
    tmp_path: Path,
) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(13))
    messages_by_chat = {
        "chat_1": [
            _message("u1", "user", _ts(9), "one"),
            _message("a1", "assistant", _ts(10), "reply one"),
            _message("u2", "user", _ts(11), "two"),
            _message("a2", "assistant", _ts(12), "reply two"),
            _message("u3", "user", _ts(12, 30), "three"),
            _message("a3", "assistant", _ts(13), "reply three"),
        ],
    }
    db_path = tmp_path / "sync_state.db"
    compliance = FakeComplianceClient([chat], messages_by_chat)
    nebuly_crash = CrashNebulyClient(crash_after=2)
    cache1 = SyncCache(db_path, "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=_ts(8))

    import pytest

    with pytest.raises(RuntimeError):
        _sync_user(
            user_id="user_1",
            config=config,
            compliance=compliance,  # type: ignore[arg-type]
            nebuly=nebuly_crash,  # type: ignore[arg-type]
            cache=cache1,
            run_until=config.to_date,  # type: ignore[arg-type]
        )

    assert len(nebuly_crash.sent) == 2
    cache1.close()

    nebuly_resume = FakeNebulyClient()
    cache2 = SyncCache(db_path, "org_demo", dry_run=False)
    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly_resume,  # type: ignore[arg-type]
        cache=cache2,
        run_until=_ts(17),
    )

    assert counts.sent == 1
    assert counts.fetched == 1

    all_sent = nebuly_crash.sent + nebuly_resume.sent
    assert len(all_sent) == 3
    conversation_ids = [p["interaction"]["conversation_id"] for p in all_sent]
    assert conversation_ids == ["chat_1", "chat_1", "chat_1"]
    inputs = [p["interaction"]["input"] for p in all_sent]
    assert inputs == ["one", "two", "three"]

    state = cache2.get_chat_state("chat_1")
    assert state is not None
    assert state.status == "completed"


def test_tail_boundary_pair_not_duplicated(tmp_path: Path) -> None:
    watermark = _ts(10)
    chat_v1 = _chat_summary("chat_1", updated_at=watermark)
    messages_v1 = {
        "chat_1": [
            _message("u1", "user", watermark, "hello"),
            _message("a1", "assistant", watermark, "hi"),
        ],
    }

    compliance = FakeComplianceClient([chat_v1], messages_v1)
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=_ts(8))

    _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )
    assert len(nebuly.sent) == 1

    chat_v2 = _chat_summary("chat_1", updated_at=_ts(12))
    compliance._chats = [chat_v2]
    compliance._messages_by_chat = {
        "chat_1": messages_v1["chat_1"]
        + [
            _message("u2", "user", _ts(11), "more"),
            _message("a2", "assistant", _ts(12), "again"),
        ],
    }
    nebuly.sent.clear()
    compliance.message_fetch_count = 0

    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=_ts(17),
    )

    assert counts.sent == 1
    assert len(nebuly.sent) == 1
    assert nebuly.sent[0]["interaction"]["input"] == "more"


def test_backfill_boundary_pair_not_duplicated(tmp_path: Path) -> None:
    boundary = _ts(8)
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages_by_chat = {
        "chat_1": [
            _message("u0", "user", boundary, "early"),
            _message("a0", "assistant", boundary, "early reply"),
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }

    compliance = FakeComplianceClient([chat], messages_by_chat)
    nebuly = FakeNebulyClient()
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    config = _config(tmp_path, from_date=boundary)

    _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=config.to_date,  # type: ignore[arg-type]
    )
    assert len(nebuly.sent) == 2

    nebuly.sent.clear()
    compliance.message_fetch_count = 0
    config_backfill = _config(tmp_path, from_date=_ts(6))

    counts = _sync_user(
        user_id="user_1",
        config=config_backfill,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        cache=cache,
        run_until=_ts(17),
    )

    assert counts.sent == 0
    assert len(nebuly.sent) == 0
