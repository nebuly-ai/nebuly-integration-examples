from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from typing import Any

from . import user_defined
from .config import datetime_to_timestamp_str
from .models import (
    ChatMessage,
    ChatMessagesResponse,
    ChatSummary,
    ToolResultContent,
    ToolUseContent,
)

MAX_CONTENT_CHARS = 8000


@dataclass(frozen=True)
class Interaction:
    user_message: ChatMessage
    assistant_message: ChatMessage
    chat: ChatSummary


def truncate_text(text: str) -> str:
    if len(text) <= MAX_CONTENT_CHARS:
        return text
    return text[:MAX_CONTENT_CHARS]


def _canonical_json(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _tool_result_text(block: ToolResultContent) -> str:
    return "\n".join(part.text for part in block.content if part.text)


def tool_retrieval_traces_from_messages(  # noqa: C901, PLR0912
    messages: list[ChatMessage],
) -> list[dict[str, Any]]:
    tool_uses: list[ToolUseContent] = []
    for message in messages:
        tool_uses.extend(block for block in message.content if block.type == "tool_use")

    results_by_id: dict[str, str] = {}
    for message in messages:
        for block in message.content:
            if block.type != "tool_result":
                continue
            tool_use_id = block.tool_use_id
            if tool_use_id:
                results_by_id[tool_use_id] = _tool_result_text(block)

    results_by_name: dict[str, deque[str]] = {}
    for message in messages:
        for block in message.content:
            if block.type != "tool_result":
                continue
            name = block.name or "tool"
            results_by_name.setdefault(name, deque()).append(_tool_result_text(block))

    traces: list[dict[str, Any]] = []

    for tool_use in tool_uses:
        tool_id = tool_use.id
        result_text: str | None = None
        if tool_id and tool_id in results_by_id:
            result_text = results_by_id[tool_id]
        elif tool_id is None:
            queue = results_by_name.get(tool_use.name)
            if queue:
                result_text = queue.popleft()
        if result_text is None:
            continue
        traces.append(
            {
                "source": tool_use.name,
                "input": _canonical_json(tool_use.input),
                "outputs": [result_text],
            }
        )
    return traces


def extract_text_content(message: ChatMessage) -> str:
    parts = [
        block.text for block in message.content if block.type == "text" and block.text
    ]
    return "\n".join(parts)


def build_message_pairs(
    chat_messages: list[ChatMessage], chat: ChatSummary
) -> list[Interaction]:
    enumerated = list(enumerate(chat_messages))
    sorted_messages = [
        msg
        for _, msg in sorted(
            enumerated,
            key=lambda item: (item[1].created_at, item[0]),
        )
    ]

    pairs: list[Interaction] = []
    pending_user: ChatMessage | None = None
    for message in sorted_messages:
        if message.role == "user":
            pending_user = message
        elif message.role == "assistant":
            if pending_user is None:
                continue
            pairs.append(
                Interaction(
                    user_message=pending_user,
                    assistant_message=message,
                    chat=chat,
                )
            )
            pending_user = None

    return sorted(
        pairs,
        key=lambda p: (p.assistant_message.created_at, p.assistant_message.id),
    )


def pair_to_payload(pair: Interaction, *, anonymize: bool) -> dict[str, Any] | None:
    user_input = extract_text_content(pair.user_message)
    # No user text means no Nebuly input; the pair is permanently non-exportable.
    if not user_input:
        return None

    assistant_output = extract_text_content(pair.assistant_message)
    chat = pair.chat

    if chat.user is None:
        return None

    tags = {
        key: value
        for key, value in user_defined.build_tags(pair).items()
        if value is not None
    }

    interaction = {
        "conversation_id": chat.id,
        "input": truncate_text(user_input),
        "output": truncate_text(assistant_output),
        "time_start": datetime_to_timestamp_str(pair.user_message.created_at),
        "time_end": datetime_to_timestamp_str(pair.assistant_message.created_at),
        "end_user": chat.user.id,
        "hide_content": False,
        "tags": tags,
    }

    return {
        "interaction": interaction,
        "traces": user_defined.build_traces(pair),
        "user_feedback": user_defined.build_user_feedback(pair),
        "anonymize": anonymize,
    }


def pairs_from_chat_response(response: ChatMessagesResponse) -> list[Interaction]:
    chat = ChatSummary.model_validate(
        response.model_dump(
            exclude={"chat_messages", "has_more", "first_id", "last_id"}
        )
    )
    return build_message_pairs(response.chat_messages, chat)
