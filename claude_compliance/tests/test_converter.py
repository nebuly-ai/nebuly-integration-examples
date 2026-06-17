from __future__ import annotations

from datetime import datetime, timezone

from compliance_sync.converter import (
    build_message_pairs,
    dedup_key,
    extract_text_content,
    pair_to_payload,
)
from compliance_sync.models import (
    ChatMessage,
    ChatSummary,
    ChatUser,
    TextContent,
    ToolUseContent,
)


def _chat(chat_id: str = "chat_01", user_id: str = "user_01") -> ChatSummary:
    now = datetime(2025, 2, 1, 10, 0, tzinfo=timezone.utc)
    return ChatSummary(
        id=chat_id,
        name="Test chat",
        created_at=now,
        updated_at=now,
        href="https://claude.ai/chat/test",
        model="claude-opus-4-8",
        organization_id="org_1",
        organization_uuid="org_demo",
        project_id="proj_01",
        user=ChatUser(id=user_id, email_address="u@example.com"),
    )


def _msg(msg_id: str, role: str, text: str) -> ChatMessage:
    return ChatMessage(
        id=msg_id,
        role=role,
        created_at=datetime(2025, 2, 1, 10, 1, tzinfo=timezone.utc),
        content=[TextContent(type="text", text=text)],
    )


def test_pairs_user_with_assistant() -> None:
    chat = _chat()
    pairs = build_message_pairs(
        [_msg("u1", "user", "hello"), _msg("a1", "assistant", "hi")], chat
    )
    assert len(pairs) == 1


def test_tool_blocks_excluded() -> None:
    chat = _chat()
    assistant = ChatMessage(
        id="a1",
        role="assistant",
        created_at=datetime(2025, 2, 1, 10, 2, tzinfo=timezone.utc),
        content=[
            TextContent(type="text", text="done"),
            ToolUseContent(type="tool_use", id="t1", name="tool", input="{}"),
        ],
    )
    pairs = build_message_pairs([_msg("u1", "user", "run"), assistant], chat)
    assert extract_text_content(pairs[0].assistant_message) == "done"


def test_dedup_key() -> None:
    chat = _chat(chat_id="c1")
    pair = build_message_pairs(
        [_msg("u1", "user", "hello"), _msg("a1", "assistant", "hi")], chat
    )[0]
    assert dedup_key(pair) == "c1:a1"


def test_payload_shape() -> None:
    chat = _chat()
    pair = build_message_pairs(
        [_msg("u1", "user", "hello"), _msg("a1", "assistant", "hi")], chat
    )[0]
    payload = pair_to_payload(pair, anonymize=False, include_minimal_trace=False)
    assert payload is not None
    assert payload["interaction"]["end_user"] == "user_01"
    assert payload["traces"] == []
    assert payload["user_feedback"] == []


def test_minimal_trace_flag() -> None:
    chat = _chat()
    pair = build_message_pairs(
        [_msg("u1", "user", "hello"), _msg("a1", "assistant", "hi")], chat
    )[0]
    payload = pair_to_payload(pair, anonymize=False, include_minimal_trace=True)
    assert payload is not None
    assert len(payload["traces"]) == 1
    assert payload["traces"][0]["type"] == "llm"
