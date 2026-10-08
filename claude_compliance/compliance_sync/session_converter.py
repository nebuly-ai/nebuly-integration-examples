from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal

from . import user_defined
from .config import datetime_to_timestamp_str
from .converter import truncate_text
from .models import (
    LocalSession,
    RemoteSession,
    RemoteSessionListMetadata,
    SessionMessage,
    content_unavailable_reason,
)

_SYSTEM_REMINDER_RE = re.compile(
    r"<system-reminder>.*?</system-reminder>\s*",
    re.DOTALL,
)


@dataclass
class SessionCutInteraction:
    key: str
    time_start: datetime
    time_end: datetime
    input_text: str
    output_text: str
    closed: bool
    content_unavailable_tags: list[str] = field(default_factory=list)
    messages: list[SessionMessage] = field(default_factory=list)


def _strip_injected_spans(text: str) -> str:
    cleaned = _SYSTEM_REMINDER_RE.sub("", text)
    for pattern in user_defined.INJECTED_CONTEXT_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    return cleaned.strip()


def _message_text(message: SessionMessage) -> str:
    parts = [
        block.text for block in message.content if block.type == "text" and block.text
    ]
    return "\n".join(parts)


def _has_tool_result(message: SessionMessage) -> bool:
    return any(block.type == "tool_result" for block in message.content)


def _is_synthetic_marker(message: SessionMessage) -> bool:
    return (
        message.provenance is not None and message.provenance.type == "synthetic_marker"
    )


def _is_client_asserted(message: SessionMessage) -> bool:
    return (
        message.provenance is not None and message.provenance.type == "client_asserted"
    )


def _is_retention_elapsed(message: SessionMessage) -> bool:
    return content_unavailable_reason(message.provenance) == "retention_elapsed"


def _remote_content_unavailable(message: SessionMessage) -> bool:
    return message.content_unavailable is True


def _session_idle_closed(
    session_updated_at: datetime,
    now: datetime,
    idle_minutes: int,
) -> bool:
    return now - session_updated_at >= timedelta(minutes=idle_minutes)


def _remote_terminal_closed(status: str | None) -> bool:
    return status in {"archived", "failed"}


def _verified_assistant_output(messages: list[SessionMessage]) -> str:
    output = ""
    for message in reversed(messages):
        if message.role != "assistant":
            continue
        if _is_client_asserted(message):
            continue
        text = _message_text(message)
        if text:
            output = text
            break
    return output


def _max_created_at(messages: list[SessionMessage]) -> datetime:
    return max(message.created_at for message in messages)


def cut_session_interactions(  # noqa: C901, PLR0912, PLR0915
    messages: list[SessionMessage],
    *,
    remote: bool,
    session_updated_at: datetime,
    session_status: str | None,
    now: datetime,
    idle_minutes: int = 30,
) -> list[SessionCutInteraction]:
    interactions: list[SessionCutInteraction] = []
    current_messages: list[SessionMessage] = []
    current_prompt_id: str | None = None
    current_prompt_at: datetime | None = None
    current_input_parts: list[str] = []
    unavailable_tags: list[str] = []
    saw_first_prompt = False
    saw_assistant_since_prompt = False

    def close_current(*, closed: bool) -> None:
        nonlocal current_prompt_id, current_prompt_at, current_input_parts
        nonlocal current_messages, unavailable_tags
        if current_prompt_id is None or not saw_first_prompt:
            current_messages = []
            current_input_parts = []
            unavailable_tags = []
            return
        input_text = "\n".join(current_input_parts)
        if not input_text:
            current_messages = []
            current_prompt_id = None
            current_prompt_at = None
            current_input_parts = []
            unavailable_tags = []
            return
        time_end = _max_created_at(current_messages)
        interactions.append(
            SessionCutInteraction(
                key=current_prompt_id,
                time_start=current_prompt_at or time_end,
                time_end=time_end,
                input_text=input_text,
                output_text=_verified_assistant_output(current_messages),
                closed=closed,
                content_unavailable_tags=list(unavailable_tags),
                messages=list(current_messages),
            )
        )
        current_messages = []
        current_prompt_id = None
        current_prompt_at = None
        current_input_parts = []
        unavailable_tags = []

    for message in messages:
        if not remote and _is_synthetic_marker(message):
            continue
        if not remote and _is_retention_elapsed(message):
            continue

        if remote and _remote_content_unavailable(message):
            unavailable_tags.append("content_unavailable")
            current_messages.append(message)
            continue

        if not remote:
            reason = content_unavailable_reason(message.provenance)
            if reason and reason != "retention_elapsed":
                unavailable_tags.append(f"content_unavailable={reason}")
                current_messages.append(message)
                continue

        if message.role == "user" and _has_tool_result(message):
            current_messages.append(message)
            continue

        if message.role == "user":
            stripped = _strip_injected_spans(_message_text(message))
            if not stripped:
                current_messages.append(message)
                continue
            if (
                current_prompt_id is not None
                and saw_first_prompt
                and not saw_assistant_since_prompt
            ):
                current_input_parts.append(stripped)
                current_messages.append(message)
                continue
            close_current(closed=True)
            saw_first_prompt = True
            saw_assistant_since_prompt = False
            current_prompt_id = message.id
            current_prompt_at = message.created_at
            current_input_parts = [stripped]
            current_messages = [message]
            continue

        if message.role == "assistant":
            if not saw_first_prompt:
                continue
            saw_assistant_since_prompt = True
            current_messages.append(message)
            continue

        current_messages.append(message)

    session_closed = _session_idle_closed(session_updated_at, now, idle_minutes)
    if remote and _remote_terminal_closed(session_status):
        session_closed = True
    close_current(closed=session_closed)
    return interactions


def resolve_session_end_user(
    session: LocalSession | RemoteSession,
    list_metadata: RemoteSessionListMetadata | None,
) -> str | None:
    if isinstance(session, LocalSession):
        if session.user is not None:
            return session.user.id
        return None
    if session.user is not None:
        return session.user.id
    if list_metadata and list_metadata.started_by_user is not None:
        return list_metadata.started_by_user.id
    if session.started_by_user is not None:
        return session.started_by_user.id
    return None


def session_interaction_to_payload(
    cut: SessionCutInteraction,
    session: LocalSession | RemoteSession,
    *,
    source: Literal["local_session", "remote_session"],
    list_metadata: RemoteSessionListMetadata | None,
    anonymize: bool,
) -> dict[str, Any] | None:
    end_user = resolve_session_end_user(session, list_metadata)
    if end_user is None:
        return None
    if not cut.input_text:
        return None

    tags = user_defined.build_session_tags(
        session,
        cut,
        source=source,
        list_metadata=list_metadata,
    )
    traces = user_defined.build_session_traces(cut, session)

    interaction = {
        "conversation_id": session.id,
        "input": truncate_text(cut.input_text),
        "output": truncate_text(cut.output_text),
        "time_start": datetime_to_timestamp_str(cut.time_start),
        "time_end": datetime_to_timestamp_str(cut.time_end),
        "end_user": end_user,
        "hide_content": False,
        "tags": tags,
    }
    return {
        "interaction": interaction,
        "traces": traces,
        "user_feedback": [],
        "anonymize": anonymize,
    }
