from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .converter import Turn
    from .models import Event, Session

logger = logging.getLogger(__name__)


def _usage_metadata(event: Event) -> dict[str, Any]:
    if not event.event_metadata or not event.event_metadata.custom_metadata:
        return {}
    raw = event.event_metadata.custom_metadata.get("_usage_metadata", {})
    return raw if isinstance(raw, dict) else {}


def _model_version(event: Event) -> str:
    if event.raw_event and event.raw_event.model_version:
        return event.raw_event.model_version
    return "gemini"


def _node_path(event: Event) -> str:
    if event.raw_event and event.raw_event.node_info:
        return str(event.raw_event.node_info.get("path", ""))
    return ""


def build_tags(turn: Turn, session: Session) -> dict[str, str]:
    final = turn.final_response
    usage = _usage_metadata(final) if final else {}
    return {
        "session_id": session.session_id,
        "invocation_id": turn.invocation_id,
        "final_author": final.author if final else "",
        "node_path": _node_path(final) if final else "",
        "model_version": _model_version(final) if final else "",
        "traffic_type": str(usage.get("traffic_type", "")),
        "total_tokens": str(usage.get("total_token_count", "")),
    }


def _model_events(turn: Turn) -> list[Event]:
    return [
        event
        for event in turn.events
        if event.content and event.content.role == "model"
    ]


def _messages_prefix(turn: Turn, model_event: Event) -> list[dict[str, str]]:
    # Circular import
    from .converter import event_to_message  # noqa: PLC0415

    prefix: list[dict[str, str]] = []
    for event in turn.events:
        if event is model_event:
            break
        prefix.append(event_to_message(event))
    if prefix and prefix[-1]["role"] != "user":
        logger.warning(
            "Consecutive model events in invocation %s; appending synthetic "
            "user message",
            turn.invocation_id,
        )
        prefix.append({"role": "user", "content": ""})
    return prefix


def build_traces(turn: Turn) -> list[dict[str, Any]]:
    # Circular import
    from .converter import extract_text_from_event  # noqa: PLC0415

    traces: list[dict[str, Any]] = []
    for model_event in _model_events(turn):
        usage = _usage_metadata(model_event)
        traces.append(
            {
                "model": _model_version(model_event),
                "messages": _messages_prefix(turn, model_event),
                "output": extract_text_from_event(model_event),
                "input_tokens": usage.get("prompt_token_count", 0),
                "output_tokens": usage.get("candidates_token_count", 0),
            }
        )
    return traces


def build_user_feedback(turn: Turn) -> list[dict[str, Any]]:  # noqa: ARG001
    return []
