from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from compliance_sync.checkpoint import Checkpoint
from compliance_sync.config import Config
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
    ) -> None:
        self._chats = chats
        self._messages_by_chat = messages_by_chat

    def list_chats(
        self,
        user_ids: list[str],
        *,
        updated_at_gte: str | None = None,
        updated_at_lte: str | None = None,
        after_id: str | None = None,
        limit: int = 100,
    ) -> PaginatedChatsResponse:
        return PaginatedChatsResponse(data=list(self._chats), has_more=False)

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
        chat = next(c for c in self._chats if c.id == chat_id)
        return _chat_messages_response(chat, self._messages_by_chat[chat_id])


class FakeNebulyClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_interaction(self, payload: dict[str, Any]) -> None:
        self.sent.append(payload)


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
    checkpoint = Checkpoint(tmp_path / "checkpoint.json", "org_demo")

    config = Config(
        nebuly_api_key="key",
        nebuly_endpoint="https://example.com/events",
        compliance_api_key="key",
        compliance_base_url="https://example.com",
        organization_uuid="org_demo",
        compliance_max_requests_per_minute=600,
        anonymize=False,
        from_date=None,
        to_date=None,
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
    )

    counts = _sync_user(
        user_id="user_1",
        config=config,
        compliance=compliance,  # type: ignore[arg-type]
        nebuly=nebuly,  # type: ignore[arg-type]
        checkpoint=checkpoint,
    )

    assert counts.fetched == 4
    assert counts.skipped == 0
    assert counts.sent == 4
    assert counts.failed == 0
    assert len(nebuly.sent) == 4
