from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Never, Protocol

import httpx
import pytest
from compliance_sync.cache import SyncCache
from compliance_sync.compliance_client import SourceUnavailableError
from compliance_sync.config import Config, timestamp_str_to_datetime
from compliance_sync.models import (
    ChatMessage,
    ChatMessagesResponse,
    ChatSummary,
    ChatUser,
    PaginatedChatsResponse,
    TextContent,
)
from compliance_sync.nebuly_client import PermanentRejectionError
from compliance_sync.sources import ChatSource, FetchRequest
from compliance_sync.sync import SourceCounts, _run_source


class NebulySender(Protocol):
    def send_interaction(self, payload: dict[str, Any]) -> None: ...


if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _chat_summary(
    chat_id: str,
    *,
    updated_at: datetime,
    created_at: datetime | None = None,
    user: ChatUser | None | object = ...,
    deleted_at: datetime | None = None,
) -> ChatSummary:
    if user is ...:
        resolved_user: ChatUser | None = ChatUser(
            id="user_1", email_address="user@example.com"
        )
    else:
        resolved_user = user  # type: ignore[assignment]
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
        user=resolved_user,
        deleted_at=deleted_at,
    )


def _message(msg_id: str, role: str, created_at: datetime, text: str) -> ChatMessage:
    return ChatMessage(
        id=msg_id,
        role=role,
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
        deleted_at=chat.deleted_at,
    )


class FakeComplianceClient:
    def __init__(
        self,
        chats: list[ChatSummary],
        messages_by_chat: dict[str, list[ChatMessage]],
    ) -> None:
        self._chats = chats
        self._messages_by_chat = messages_by_chat
        self.message_fetch_count = 0
        self.messages_404_for: set[str] = set()

    def iter_chats(
        self,
        organization_ids: list[str],
        *,
        updated_at_gte: str | None = None,
        updated_at_lte: str | None = None,
        limit: int = 100,
    ) -> Iterator[PaginatedChatsResponse]:
        del organization_ids, limit
        chats = sorted(self._chats, key=lambda c: c.id)
        if updated_at_gte is not None:
            gte = timestamp_str_to_datetime(updated_at_gte)
            chats = [c for c in chats if c.updated_at >= gte]
        if updated_at_lte is not None:
            lte = timestamp_str_to_datetime(updated_at_lte)
            chats = [c for c in chats if c.updated_at <= lte]
        after_id: str | None = None
        while True:
            page_chats = chats
            if after_id is not None:
                ids = [c.id for c in page_chats]
                try:
                    start = ids.index(after_id) + 1
                    page_chats = page_chats[start:]
                except ValueError:
                    page_chats = []
            page = page_chats[:100]
            has_more = len(page_chats) > 100
            yield PaginatedChatsResponse(
                data=page,
                has_more=has_more,
                first_id=page[0].id if page else None,
                last_id=page[-1].id if page else None,
            )
            if not has_more:
                break
            after_id = page[-1].id

    def list_chat_messages(
        self,
        chat_id: str,
        *,
        created_at_gte: str | None = None,
        created_at_lte: str | None = None,
        after_id: str | None = None,
        order: str = "asc",
        limit: int = 1000,
    ) -> ChatMessagesResponse:
        del created_at_lte, after_id, order, limit
        if chat_id in self.messages_404_for:
            request = httpx.Request("GET", f"/chats/{chat_id}/messages")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("Not found", request=request, response=response)
        self.message_fetch_count += 1
        chat = next(c for c in self._chats if c.id == chat_id)
        messages = list(self._messages_by_chat[chat_id])
        if created_at_gte is not None:
            gte = timestamp_str_to_datetime(created_at_gte)
            messages = [m for m in messages if m.created_at >= gte]
        return _chat_messages_response(chat, messages)


class FakeNebulyClient:
    def __init__(
        self,
        *,
        fail_after: int | None = None,
        reject_after: int | None = None,
        reject_status: int = 400,
    ) -> None:
        self.sent: list[dict[str, Any]] = []
        self._fail_after = fail_after
        self._reject_after = reject_after
        self._reject_status = reject_status

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if self._reject_after is not None and len(self.sent) >= self._reject_after:
            raise PermanentRejectionError(self._reject_status)
        if self._fail_after is not None and len(self.sent) >= self._fail_after:
            request = httpx.Request("POST", "/events")
            response = httpx.Response(500, request=request)
            raise httpx.HTTPStatusError(
                "Server error", request=request, response=response
            )
        self.sent.append(payload)


class CrashNebulyClient:
    def __init__(self, *, crash_after: int) -> None:
        self.sent: list[dict[str, Any]] = []
        self._crash_after = crash_after

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if len(self.sent) >= self._crash_after:
            raise RuntimeError("simulated crash")
        self.sent.append(payload)


def _config(
    tmp_path: Path,
    *,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
) -> Config:
    return Config(
        nebuly_api_key="key",
        nebuly_endpoint="https://example.com/events",
        compliance_api_key="key",
        compliance_base_url="https://example.com",
        organization_uuid="org_demo",
        compliance_max_requests_per_minute=600,
        anonymize=False,
        from_date=from_date,
        to_date=to_date or _ts(16),
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
        sources=("chats",),
    )


def _run_chats(
    tmp_path: Path,
    compliance: FakeComplianceClient,
    nebuly: NebulySender,
    config: Config,
) -> tuple[SourceCounts, bool]:
    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    adapter = ChatSource(compliance, "org_demo")  # type: ignore[arg-type]
    counts, stopped = _run_source(
        adapter=adapter,
        cache=cache,
        nebuly=nebuly,  # type: ignore[arg-type]
        config=config,
        run_until=config.run_until(),
        run_started=_ts(15),
    )
    cache.close()
    return counts, stopped


def test_second_run_sends_nothing(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages = {
        "chat_1": [
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }
    compliance = FakeComplianceClient([chat], messages)
    nebuly = FakeNebulyClient()
    config = _config(tmp_path, from_date=_ts(8))

    _run_chats(tmp_path, compliance, nebuly, config)
    assert len(nebuly.sent) == 1
    compliance.message_fetch_count = 0
    nebuly.sent.clear()

    counts, _ = _run_chats(tmp_path, compliance, nebuly, config)
    assert counts.sent == 0
    assert compliance.message_fetch_count == 0


def test_updated_chat_sends_only_new_pair(tmp_path: Path) -> None:
    chat_v1 = _chat_summary("chat_1", updated_at=_ts(10))
    messages = {
        "chat_1": [
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }
    compliance = FakeComplianceClient([chat_v1], messages)
    nebuly = FakeNebulyClient()
    config = _config(tmp_path, from_date=_ts(8))
    _run_chats(tmp_path, compliance, nebuly, config)

    chat_v2 = _chat_summary("chat_1", updated_at=_ts(12))
    compliance._chats = [chat_v2]
    compliance._messages_by_chat["chat_1"].extend(
        [
            _message("u2", "user", _ts(11), "more"),
            _message("a2", "assistant", _ts(12), "again"),
        ]
    )
    nebuly.sent.clear()
    compliance.message_fetch_count = 0

    counts, _ = _run_chats(tmp_path, compliance, nebuly, config)
    assert counts.sent == 1
    assert compliance.message_fetch_count == 1


def test_crash_mid_conversation_does_not_replay(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(13))
    messages = {
        "chat_1": [
            _message("u1", "user", _ts(9), "one"),
            _message("a1", "assistant", _ts(10), "reply one"),
            _message("u2", "user", _ts(11), "two"),
            _message("a2", "assistant", _ts(12), "reply two"),
            _message("u3", "user", _ts(12, 30), "three"),
            _message("a3", "assistant", _ts(13), "reply three"),
        ],
    }
    compliance = FakeComplianceClient([chat], messages)
    nebuly_crash = CrashNebulyClient(crash_after=2)
    config = _config(tmp_path, from_date=_ts(8))

    with pytest.raises(RuntimeError):
        _run_chats(tmp_path, compliance, nebuly_crash, config)

    nebuly_resume = FakeNebulyClient()
    counts, _ = _run_chats(tmp_path, compliance, nebuly_resume, config)
    assert counts.sent == 1
    all_inputs = [
        p["interaction"]["input"] for p in nebuly_crash.sent + nebuly_resume.sent
    ]
    assert all_inputs == ["one", "two", "three"]


def test_send_failure_resumes(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(12))
    messages = {
        "chat_1": [
            _message("u1", "user", _ts(9), "one"),
            _message("a1", "assistant", _ts(10), "reply one"),
            _message("u2", "user", _ts(11), "two"),
            _message("a2", "assistant", _ts(12), "reply two"),
        ],
    }
    compliance = FakeComplianceClient([chat], messages)
    nebuly = FakeNebulyClient(fail_after=1)
    config = _config(tmp_path, from_date=_ts(8))

    counts, stopped = _run_chats(tmp_path, compliance, nebuly, config)
    assert counts.sent == 1
    assert stopped is True

    counts_retry, _ = _run_chats(tmp_path, compliance, FakeNebulyClient(), config)
    assert counts_retry.sent == 1


def test_messages_404_marks_gone(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages = {
        "chat_1": [
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }
    compliance = FakeComplianceClient([chat], messages)
    compliance.messages_404_for.add("chat_1")
    config = _config(tmp_path, from_date=_ts(8))
    _run_chats(tmp_path, compliance, FakeNebulyClient(), config)

    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    state = cache.get_conversation("chats", "chat_1")
    cache.close()
    assert state is not None
    assert state.status == "gone"


def test_deleted_chat_skipped_without_fetch(tmp_path: Path) -> None:
    chat = _chat_summary(
        "chat_1",
        updated_at=_ts(10),
        deleted_at=_ts(10),
    )
    compliance = FakeComplianceClient([chat], {})
    config = _config(tmp_path, from_date=_ts(8))
    counts, _ = _run_chats(tmp_path, compliance, FakeNebulyClient(), config)
    assert counts.skipped_deleted == 1
    assert compliance.message_fetch_count == 0


def test_null_user_chat_skipped(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10), user=None)
    compliance = FakeComplianceClient([chat], {})
    config = _config(tmp_path, from_date=_ts(8))
    counts, _ = _run_chats(tmp_path, compliance, FakeNebulyClient(), config)
    assert counts.skipped_no_user == 1
    assert compliance.message_fetch_count == 0


def test_nebuly_rejection_recorded_and_run_continues(tmp_path: Path) -> None:
    chat_a = _chat_summary("chat_a", updated_at=_ts(10))
    chat_b = _chat_summary("chat_b", updated_at=_ts(11))
    messages = {
        "chat_a": [
            _message("a_u1", "user", _ts(9), "a"),
            _message("a_a1", "assistant", _ts(10), "a reply"),
        ],
        "chat_b": [
            _message("b_u1", "user", _ts(10), "b"),
            _message("b_a1", "assistant", _ts(11), "b reply"),
        ],
    }
    compliance = FakeComplianceClient([chat_a, chat_b], messages)
    config = _config(tmp_path, from_date=_ts(8))

    class RejectFirstNebuly:
        def __init__(self) -> None:
            self.sent: list[dict[str, Any]] = []
            self.rejected = 0

        def send_interaction(self, payload: dict[str, Any]) -> None:
            if self.rejected == 0:
                self.rejected += 1
                raise PermanentRejectionError(413)
            self.sent.append(payload)

    nebuly_mixed = RejectFirstNebuly()
    counts, stopped = _run_chats(tmp_path, compliance, nebuly_mixed, config)
    assert counts.rejected == 1
    assert counts.sent == 1
    assert stopped is False


def test_transient_failure_does_not_advance_watermark(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(12))
    messages = {
        "chat_1": [
            _message("u1", "user", _ts(9), "one"),
            _message("a1", "assistant", _ts(10), "reply one"),
            _message("u2", "user", _ts(11), "two"),
            _message("a2", "assistant", _ts(12), "reply two"),
        ],
    }
    compliance = FakeComplianceClient([chat], messages)
    config = _config(tmp_path, from_date=_ts(8))
    run_started = _ts(14)

    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    adapter = ChatSource(compliance, "org_demo")  # type: ignore[arg-type]
    counts, stopped = _run_source(
        adapter=adapter,
        cache=cache,
        nebuly=FakeNebulyClient(fail_after=1),  # type: ignore[arg-type]
        config=config,
        run_until=config.run_until(),
        run_started=run_started,
    )
    assert stopped is True
    assert counts.sent == 1
    assert cache.get_watermark("chats") is None
    cache.close()


def test_backfill_reprocesses_completed_chat(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages = {
        "chat_1": [
            _message("u0", "user", _ts(7), "early"),
            _message("a0", "assistant", _ts(7, 30), "early reply"),
            _message("u1", "user", _ts(9), "hello"),
            _message("a1", "assistant", _ts(10), "hi"),
        ],
    }
    compliance = FakeComplianceClient([chat], messages)
    config = _config(tmp_path, from_date=_ts(8))
    _run_chats(tmp_path, compliance, FakeNebulyClient(), config)

    config_backfill = _config(tmp_path, from_date=_ts(6))
    counts, _ = _run_chats(tmp_path, compliance, FakeNebulyClient(), config_backfill)
    assert counts.sent == 1


def test_to_date_holds_interaction_and_leaves_pending(tmp_path: Path) -> None:
    chat = _chat_summary("chat_1", updated_at=_ts(10))
    messages = {
        "chat_1": [
            _message("u1", "user", _ts(9), "one"),
            _message("a1", "assistant", _ts(10), "reply one"),
            _message("u2", "user", _ts(11), "two"),
            _message("a2", "assistant", _ts(12), "reply two"),
        ],
    }
    compliance = FakeComplianceClient([chat], messages)
    config = _config(tmp_path, from_date=_ts(8), to_date=_ts(10, 30))
    counts, _ = _run_chats(tmp_path, compliance, FakeNebulyClient(), config)
    assert counts.sent == 1

    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=False)
    state = cache.get_conversation("chats", "chat_1")
    cache.close()
    assert state is not None
    assert state.status == "pending"


def test_local_source_unavailable_skipped(tmp_path: Path) -> None:
    class UnavailableLocal:
        name = "local_sessions"

        def list_changed(
            self,
            listing_from: datetime | None,
            to_date: datetime | None,
        ) -> Never:
            raise SourceUnavailableError("off")

        def fetch(self, request: FetchRequest) -> Never:
            raise AssertionError("fetch should not run")

    cache = SyncCache(tmp_path / "sync_state.db", "org_demo", dry_run=True)
    counts, stopped = _run_source(
        adapter=UnavailableLocal(),
        cache=cache,
        nebuly=FakeNebulyClient(),  # type: ignore[arg-type]
        config=_config(tmp_path, from_date=_ts(8)),
        run_until=_ts(16),
        run_started=_ts(15),
    )
    cache.close()
    assert counts.listed == 0
    assert stopped is False
