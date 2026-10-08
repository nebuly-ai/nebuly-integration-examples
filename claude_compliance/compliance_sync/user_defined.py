from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .models import LocalSession, RemoteSession, RemoteSessionListMetadata

if TYPE_CHECKING:
    from .converter import Interaction
    from .session_converter import SessionCutInteraction


INJECTED_CONTEXT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"This session is being continued from a previous conversation.*",
        re.DOTALL,
    ),
]


def _drop_none_tags(tags: dict[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in tags.items() if value is not None}


def build_tags(pair: Interaction) -> dict[str, str | None]:
    chat = pair.chat
    return {
        "claude source": "chat",
        "claude chat-id": chat.id,
        "claude project-id": chat.project_id,
        "model": str(chat.model or "unknown"),
        "chat name": chat.name,
        "href": chat.href,
    }


def build_traces(pair: Interaction) -> list[dict[str, Any]]:
    from .converter import tool_retrieval_traces_from_messages  # noqa: PLC0415

    return tool_retrieval_traces_from_messages(
        [pair.user_message, pair.assistant_message]
    )


def build_session_tags(
    session: LocalSession | RemoteSession,
    cut: SessionCutInteraction,
    *,
    source: str,
    list_metadata: RemoteSessionListMetadata | None = None,
) -> dict[str, str]:
    tags: dict[str, str | None] = {
        "claude source": source,
        "claude session-id": session.id,
        "product surface": getattr(session, "product_surface", None),
    }
    if isinstance(session, LocalSession):
        tags["workspace id"] = session.workspace_id
        tags["session truncated"] = str(session.truncated).lower()
    else:
        tags["session status"] = session.status
        tags["agent id"] = session.agent_id
        project_id = session.claude_project_id
        if list_metadata is not None and list_metadata.claude_project_id is not None:
            project_id = list_metadata.claude_project_id
        tags["claude project-id"] = project_id
    for tag in cut.content_unavailable_tags:
        tags["content unavailable"] = tag
    return _drop_none_tags(tags)


def build_session_traces(
    cut: SessionCutInteraction,
    _session: LocalSession | RemoteSession,
) -> list[dict[str, Any]]:
    from .converter import (  # noqa: PLC0415
        tool_retrieval_traces_from_messages,
        truncate_text,
    )
    from .models import ChatMessage  # noqa: PLC0415

    chat_messages = [
        ChatMessage.model_validate(message.model_dump()) for message in cut.messages
    ]

    traces: list[dict[str, Any]] = list(
        tool_retrieval_traces_from_messages(chat_messages)
    )

    model = "unknown"
    for message in reversed(cut.messages):
        if message.role == "assistant" and message.model:
            model = message.model
            break

    traces.append(
        {
            "messages": [{"role": "user", "content": truncate_text(cut.input_text)}],
            "model": model,
            "output": truncate_text(cut.output_text),
        }
    )
    return traces


def build_user_feedback(pair: Interaction) -> list[dict[str, Any]]:  # noqa: ARG001
    return []
