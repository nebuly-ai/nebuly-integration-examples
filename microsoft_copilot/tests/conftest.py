from __future__ import annotations

from typing import Any, Literal

import pytest

_COWORK_APP_CLASS = "IPM.SkypeTeams.Message.Copilot.CoworkChat"
_COWORK_USER_FROM: dict[str, Any] = {
    "user": {
        "id": "00000000-0000-4000-8000-000000000001",
        "displayName": "8:orgid:synthetic-user",
        "userIdentityType": "aadUser",
    },
}
_COWORK_BOT_FROM: dict[str, Any] = {
    "application": {
        "id": "00000000-0000-4000-8000-000000000099",
        "displayName": "28:cowork-bot-synthetic",
        "applicationIdentityType": "bot",
    },
}
_PLACEHOLDER_ATTACHMENT: dict[str, Any] = {
    "attachmentId": None,
    "contentType": "reference",
    "contentUrl": "file:///unknown-url",
    "content": None,
    "name": "unknown-file-name",
}


def _cowork_record(
    record_id: str,
    session_id: str,
    *,
    interaction_type: Literal["userPrompt", "aiResponse"],
    created: str,
    content_type: Literal["text", "html"],
    content: str,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "id": record_id,
        "sessionId": session_id,
        "requestId": None,
        "appClass": _COWORK_APP_CLASS,
        "interactionType": interaction_type,
        "conversationType": "coworkchat",
        "createdDateTime": created,
        "locale": "en-us",
        "contexts": [],
        "from": _COWORK_USER_FROM
        if interaction_type == "userPrompt"
        else _COWORK_BOT_FROM,
        "body": {"contentType": content_type, "content": content},
        "attachments": attachments or [],
        "links": [],
        "mentions": [],
    }


@pytest.fixture
def synthetic_cowork_interactions() -> list[dict[str, Any]]:
    """Graph-shaped Cowork export rows with null requestId (synthetic data)."""
    return [
        _cowork_record(
            "1000000000001",
            "19:synthetic-cowork-a@thread.v2",
            interaction_type="userPrompt",
            created="2026-01-15T10:00:00.000Z",
            content_type="text",
            content="Hello",
        ),
        _cowork_record(
            "1000000000002",
            "19:synthetic-cowork-a@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:00:01.000Z",
            content_type="html",
            content="Hello back from Cowork.",
        ),
        _cowork_record(
            "1000000000003",
            "19:synthetic-cowork-b@thread.v2",
            interaction_type="userPrompt",
            created="2026-01-15T10:01:00.000Z",
            content_type="text",
            content="What can you do?",
        ),
        _cowork_record(
            "1000000000004",
            "19:synthetic-cowork-b@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:01:01.000Z",
            content_type="html",
            content="I can help with calendars, documents, and Teams messages.",
        ),
        _cowork_record(
            "1000000000005",
            "19:synthetic-cowork-c@thread.v2",
            interaction_type="userPrompt",
            created="2026-01-15T10:02:00.000Z",
            content_type="text",
            content="Help me organize my week. Please review my Outlook calendar.",
        ),
        _cowork_record(
            "1000000000006",
            "19:synthetic-cowork-c@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:02:01.000Z",
            content_type="html",
            content="**Finding an efficient solution** internal planning text only.",
        ),
        _cowork_record(
            "1000000000007",
            "19:synthetic-cowork-c@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:02:02.000Z",
            content_type="html",
            content=(
                "I'll review your calendar for next week and summarize meeting load."
            ),
        ),
        _cowork_record(
            "1000000000008",
            "19:synthetic-cowork-d@thread.v2",
            interaction_type="userPrompt",
            created="2026-01-15T10:03:00.000Z",
            content_type="text",
            content="Create a personal skill and save SKILL.md to OneDrive.",
        ),
        _cowork_record(
            "1000000000009",
            "19:synthetic-cowork-d@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:03:01.000Z",
            content_type="html",
            content="",
        ),
        _cowork_record(
            "1000000000010",
            "19:synthetic-cowork-d@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:03:02.000Z",
            content_type="html",
            content="",
        ),
        _cowork_record(
            "1000000000011",
            "19:synthetic-cowork-e@thread.v2",
            interaction_type="userPrompt",
            created="2026-01-15T10:04:00.000Z",
            content_type="text",
            content="How do I add an MCP to Copilot Cowork?",
        ),
        _cowork_record(
            "1000000000012",
            "19:synthetic-cowork-e@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:04:01.000Z",
            content_type="html",
            content="You add an MCP server through a Cowork plugin package.",
        ),
        _cowork_record(
            "1000000000013",
            "19:synthetic-cowork-f@thread.v2",
            interaction_type="userPrompt",
            created="2026-01-15T10:05:00.000Z",
            content_type="text",
            content='```\n{"url": "https://example.invalid/mcp"'
            ', "transport": "http"}\n```',
        ),
        _cowork_record(
            "1000000000014",
            "19:synthetic-cowork-f@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:05:01.000Z",
            content_type="html",
            content="",
        ),
        _cowork_record(
            "1000000000015",
            "19:synthetic-cowork-f@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:05:02.000Z",
            content_type="html",
            content="**Addressing schema issues** internal reasoning only.",
        ),
        _cowork_record(
            "1000000000016",
            "19:synthetic-cowork-f@thread.v2",
            interaction_type="aiResponse",
            created="2026-01-15T10:05:03.000Z",
            content_type="html",
            content="Created **example-connector.zip** for your MCP endpoint.",
            attachments=[_PLACEHOLDER_ATTACHMENT],
        ),
    ]
