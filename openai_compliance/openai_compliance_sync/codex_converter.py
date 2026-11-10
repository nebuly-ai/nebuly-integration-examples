from __future__ import annotations

from typing import TYPE_CHECKING, Any

from . import user_defined
from .chat_converter import truncate_text
from .config import datetime_to_timestamp_str
from .entra import EntraDirectory, apply_directory_segments, apply_directory_tags

if TYPE_CHECKING:
    from .models import CodexEventDetails, CodexLogRecord


def prompt_text(details: CodexEventDetails) -> str:
    return (details.prompt_text or "").strip()


def response_text(details: CodexEventDetails) -> str:
    return (details.response_text or "").strip()


def codex_record_to_payload(
    record: CodexLogRecord,
    *,
    anonymize: bool,
    directory: EntraDirectory | None = None,
) -> dict[str, Any] | None:
    details = record.event_details
    if details is None or details.detail_type != "PROMPT_RESPONSE_RECEIVED":
        return None
    status = (details.status or "").casefold()
    if status in {"error", "failed"}:
        return None
    prompt = prompt_text(details)
    response = response_text(details)
    if not prompt or not response:
        return None
    if details.session_id is None:
        return None
    if record.timestamp is None:
        return None

    tags = apply_directory_tags(
        user_defined.build_codex_tags(record),
        directory,
        None,
    )
    ts = datetime_to_timestamp_str(record.timestamp)
    interaction = {
        "conversation_id": details.session_id,
        "input": truncate_text(prompt),
        "output": truncate_text(response),
        "time_start": ts,
        "time_end": ts,
        "end_user": record.client_id or "",
        "hide_content": False,
        "tags": tags,
    }
    apply_directory_segments(interaction, directory, None)

    return {
        "interaction": interaction,
        "traces": user_defined.build_codex_traces(record),
        "user_feedback": user_defined.build_codex_user_feedback(record),
        "anonymize": anonymize,
    }
