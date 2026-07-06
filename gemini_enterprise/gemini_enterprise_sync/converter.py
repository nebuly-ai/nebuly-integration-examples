from __future__ import annotations

import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

from . import user_defined
from .config import datetime_to_timestamp_str
from .models import LogPayload

if TYPE_CHECKING:
    from .logging_client import LogRecord
    from .trace_client import TraceData

logger = logging.getLogger(__name__)


class SkipReason(Enum):
    EMPTY_INPUT = "empty_input"
    EMPTY_OUTPUT = "empty_output"


def _path_segment(name: str, key: str) -> str | None:
    parts = name.split("/")
    for index, part in enumerate(parts[:-1]):
        if part == key:
            return parts[index + 1]
    return None


def _parse_answer_path(
    name: str | None,
) -> tuple[str | None, str | None, str | None]:
    if not name:
        return None, None, None
    return (
        _path_segment(name, "engines"),
        _path_segment(name, "sessions"),
        _path_segment(name, "assistAnswers"),
    )


def turn_to_payload(
    record: LogRecord,
    trace_data: TraceData | None,
    *,
    engine_id: str,
    anonymize: bool,
) -> dict[str, Any] | SkipReason:
    payload = LogPayload.model_validate(record.payload)

    user_input = ""
    if payload.request and payload.request.query and payload.request.query.parts:
        user_input = (payload.request.query.parts[0].text or "").strip()
    if not user_input:
        return SkipReason.EMPTY_INPUT

    assistant_output = (payload.service_text_reply or "").strip()
    if not assistant_output:
        return SkipReason.EMPTY_OUTPUT

    answer_name = (
        payload.response.answer.name
        if payload.response and payload.response.answer
        else None
    )
    _, session_id, answer_id = _parse_answer_path(answer_name)

    if session_id:
        conversation_id = f"{engine_id}-{session_id}"
    elif record.trace_id:
        conversation_id = f"{engine_id}-{record.trace_id}"
    else:
        conversation_id = f"{engine_id}-unknown"

    if trace_data and trace_data.time_start and trace_data.time_end:
        time_start = datetime_to_timestamp_str(trace_data.time_start)
        time_end = datetime_to_timestamp_str(trace_data.time_end)
    else:
        logger.warning("Trace data is missing start and end times")
        timestamp_str = datetime_to_timestamp_str(record.timestamp)
        time_start = time_end = timestamp_str

    return {
        "interaction": {
            "conversation_id": conversation_id,
            "input": user_input,
            "output": assistant_output,
            "time_start": time_start,
            "time_end": time_end,
            "end_user": payload.user_iam_principal,
            "hide_content": False,
            "tags": user_defined.build_tags(
                record,
                engine_id=engine_id,
                session_id=session_id,
                answer_id=answer_id,
            ),
        },
        "traces": user_defined.build_traces(record, trace_data),
        "user_feedback": user_defined.build_user_feedback(record),
        "anonymize": anonymize,
    }
