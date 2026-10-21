from __future__ import annotations

from datetime import UTC, datetime

from compliance_sync.models import (
    ChatSummary,
    ChatUser,
    ProvenanceSyntheticMarker,
    SessionMessage,
    TextContent,
    ToolResultContent,
    ToolResultTextContent,
    ToolUseContent,
)


def test_chat_summary_nullable_fields() -> None:
    chat = ChatSummary.model_validate(
        {
            "id": "chat_1",
            "name": "",
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "deleted_at": "2025-01-02T00:00:00Z",
            "href": "https://example.com",
            "model": None,
            "organization_uuid": "org_demo",
            "project_id": None,
            "user": None,
        }
    )
    assert chat.deleted_at is not None
    assert chat.project_id is None
    assert chat.user is None
    assert chat.model is None


def test_chat_summary_without_deprecated_organization_id() -> None:
    now = datetime(2025, 1, 1, tzinfo=UTC)
    chat = ChatSummary(
        id="chat_1",
        name="n",
        created_at=now,
        updated_at=now,
        href="https://example.com",
        organization_uuid="org_demo",
        user=ChatUser(id="u1"),
    )
    assert chat.organization_id is None


def test_nullable_tool_ids() -> None:
    msg = SessionMessage(
        id="m1",
        role="assistant",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        content=[
            ToolUseContent(type="tool_use", id=None, name="bash", input="{}"),
            ToolResultContent(
                type="tool_result",
                tool_use_id=None,
                is_error=False,
                content=[ToolResultTextContent(type="text", text="ok")],
                name="bash",
            ),
        ],
    )
    first = msg.content[0]
    second = msg.content[1]
    assert isinstance(first, ToolUseContent)
    assert isinstance(second, ToolResultContent)
    assert first.id is None
    assert second.tool_use_id is None


def test_synthetic_marker_any_role() -> None:
    marker = SessionMessage(
        id="m0",
        role="assistant",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        content=[TextContent(type="text", text="[system prompt content not shown]")],
        provenance=ProvenanceSyntheticMarker(type="synthetic_marker"),
    )
    assert marker.provenance is not None
    assert marker.provenance.type == "synthetic_marker"
