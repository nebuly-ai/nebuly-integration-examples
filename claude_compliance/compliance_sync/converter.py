from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import user_defined
from .config import datetime_to_timestamp_str
from .models import ChatMessage, ChatMessagesResponse, ChatSummary


@dataclass(frozen=True)
class MessagePair:
    user_message: ChatMessage
    assistant_message: ChatMessage
    chat: ChatSummary


def extract_text_content(message: ChatMessage) -> str:
    parts: list[str] = []
    for block in message.content:
        if block.type == "text" and block.text:
            parts.append(block.text)
    return "\n".join(parts)


def build_message_pairs(
    chat_messages: list[ChatMessage], chat: ChatSummary
) -> list[MessagePair]:
    enumerated = list(enumerate(chat_messages))
    sorted_messages = [
        msg
        for _, msg in sorted(
            enumerated,
            key=lambda item: (item[1].created_at, item[0]),
        )
    ]

    pairs: list[MessagePair] = []
    pending_user: ChatMessage | None = None
    for message in sorted_messages:
        if message.role == "user":
            pending_user = message
        elif message.role == "assistant":
            if pending_user is None:
                continue
            pairs.append(
                MessagePair(
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


def pair_to_payload(pair: MessagePair, *, anonymize: bool) -> dict[str, Any] | None:
    user_input = extract_text_content(pair.user_message)
    # No user text means no Nebuly input; the pair is permanently non-exportable.
    if not user_input:
        return None

    assistant_output = extract_text_content(pair.assistant_message)
    chat = pair.chat

    interaction = {
        "conversation_id": chat.id,
        "input": user_input,
        "output": assistant_output,
        "time_start": datetime_to_timestamp_str(pair.user_message.created_at),
        "time_end": datetime_to_timestamp_str(pair.assistant_message.created_at),
        "end_user": chat.user.id,
        "hide_content": False,
        "tags": user_defined.build_tags(pair),
    }

    return {
        "interaction": interaction,
        "traces": user_defined.build_traces(pair),
        "user_feedback": user_defined.build_user_feedback(pair),
        "anonymize": anonymize,
    }


def pairs_from_chat_response(response: ChatMessagesResponse) -> list[MessagePair]:
    chat = ChatSummary.model_validate(
        response.model_dump(
            exclude={"chat_messages", "has_more", "first_id", "last_id"}
        )
    )
    return build_message_pairs(response.chat_messages, chat)
