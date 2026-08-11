"""Convert Langfuse traces/observations into Nebuly interactions."""

from __future__ import annotations

import json
from typing import cast

from langfuse_sync.models import (
    ChatMessage,
    Interaction,
    JsonValue,
    LangfuseObservation,
    LangfuseTrace,
    LLMTrace,
    RetrievalTrace,
)

_VALID_ROLES = frozenset({"system", "user", "assistant", "tool"})
_ROLE_ALIASES: dict[str, str] = {
    "system_message": "system",
    "user_message": "user",
    "assistant_message": "assistant",
    "human": "user",
    "ai": "assistant",
}


def _content_to_str(value: JsonValue | None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
                else:
                    parts.append(json.dumps(item))
            else:
                parts.append(str(item))
        return "".join(parts) if parts else json.dumps(value)
    return json.dumps(value)


def _message_from_role_content(raw: dict[str, JsonValue]) -> ChatMessage | None:
    role_raw = raw.get("role")
    if not isinstance(role_raw, str):
        return None
    role = _ROLE_ALIASES.get(role_raw, role_raw)
    if role not in _VALID_ROLES:
        return None
    if "content" not in raw:
        return None
    return {"role": role, "content": _content_to_str(raw.get("content"))}


def _messages_from_keyed_dict(raw: dict[str, JsonValue]) -> list[ChatMessage] | None:
    """Expand Langfuse-style {system_message, user_message, ...} into chat messages."""
    ordered_keys = (
        "system_message",
        "system",
        "user_message",
        "user",
        "human",
        "assistant_message",
        "assistant",
        "ai",
    )
    messages: list[ChatMessage] = []
    for key in ordered_keys:
        if key not in raw:
            continue
        role = _ROLE_ALIASES.get(key, key)
        if role not in _VALID_ROLES:
            continue
        messages.append({"role": role, "content": _content_to_str(raw.get(key))})
    return messages or None


def _normalize_message(raw: dict[str, JsonValue]) -> list[ChatMessage]:
    openai_msg = _message_from_role_content(raw)
    if openai_msg is not None:
        return [openai_msg]

    keyed = _messages_from_keyed_dict(raw)
    if keyed is not None:
        return keyed

    return [{"role": "user", "content": _content_to_str(cast(JsonValue, raw))}]


def _ensure_ends_with_user(messages: list[ChatMessage]) -> list[ChatMessage]:
    """Ensure message prefix ends with a user turn (output is separate)."""
    while messages and messages[-1].get("role") == "assistant":
        messages = messages[:-1]
    if not messages:
        return [{"role": "user", "content": ""}]
    if messages[-1].get("role") != "user":
        messages = [*messages, {"role": "user", "content": ""}]
    return messages


def _parse_messages_from_string(raw: str) -> list[ChatMessage]:
    try:
        parsed: object = json.loads(raw)
    except json.JSONDecodeError:
        return _ensure_ends_with_user([{"role": "user", "content": raw}])
    if isinstance(parsed, (dict, list, str, int, float, bool)) or parsed is None:
        return _parse_messages(cast(JsonValue, parsed))
    return _ensure_ends_with_user([{"role": "user", "content": str(parsed)}])


def _parse_messages_from_list(raw: list[JsonValue]) -> list[ChatMessage]:
    messages: list[ChatMessage] = []
    for item in raw:
        if isinstance(item, dict):
            messages.extend(_normalize_message(item))
        elif isinstance(item, str):
            messages.append({"role": "user", "content": item})
    return _ensure_ends_with_user(messages)


def _parse_messages_from_dict(raw: dict[str, JsonValue]) -> list[ChatMessage]:
    nested = raw.get("messages")
    if isinstance(nested, list):
        return _parse_messages(nested)
    return _ensure_ends_with_user(_normalize_message(raw))


def _parse_messages(raw: JsonValue | None) -> list[ChatMessage]:
    if raw is None:
        return _ensure_ends_with_user([])
    if isinstance(raw, str):
        return _parse_messages_from_string(raw)
    if isinstance(raw, list):
        return _parse_messages_from_list(raw)
    if isinstance(raw, dict):
        return _parse_messages_from_dict(raw)
    return _ensure_ends_with_user([{"role": "user", "content": str(raw)}])


def _stringify(value: JsonValue | None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], str):
        return value[0]
    return json.dumps(value)


_USER_INPUT_KEYS = ("user_message", "user", "human", "input", "query", "text")


def _extract_user_input_from_string(value: str) -> str:
    try:
        parsed: object = json.loads(value)
    except json.JSONDecodeError:
        return value
    if isinstance(parsed, (dict, list, str, int, float, bool)) or parsed is None:
        return _extract_user_input(cast(JsonValue, parsed))
    return value


def _user_input_from_dict(value: dict[str, JsonValue]) -> str:
    for key in _USER_INPUT_KEYS:
        if key in value:
            return _content_to_str(value.get(key)).strip()
    nested = value.get("messages")
    if isinstance(nested, list):
        return _extract_user_input(nested)
    return _stringify(value)


def _user_input_from_list(value: list[JsonValue]) -> str:
    for item in reversed(value):
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        if role in ("user", "human") or "user_message" in item or "user" in item:
            msgs = _normalize_message(item)
            for msg in reversed(msgs):
                if msg.get("role") == "user":
                    return str(msg.get("content") or "").strip()
    if len(value) == 1 and isinstance(value[0], str):
        return value[0]
    return _stringify(value)


def _extract_user_input(value: JsonValue | None) -> str:
    """Prefer the user turn for interaction input, not the full prompt dict."""
    if value is None:
        return ""
    if isinstance(value, str):
        return _extract_user_input_from_string(value)
    if isinstance(value, dict):
        return _user_input_from_dict(value)
    if isinstance(value, list):
        return _user_input_from_list(value)
    return _stringify(value)


def _langfuse_tags_to_dict(tags: list[str] | dict[str, str] | None) -> dict[str, str]:
    if not tags:
        return {}
    if isinstance(tags, dict):
        return {str(k): str(v) for k, v in tags.items()}
    return {str(tag): "true" for tag in tags if tag is not None}


def convert_observations_to_traces(
    observations: list[LangfuseObservation],
) -> list[RetrievalTrace | LLMTrace]:
    traces: list[RetrievalTrace | LLMTrace] = []
    for observation in observations:
        if observation.get("parentObservationId") is not None:
            continue
        usage = observation.get("usageDetails") or {}
        if observation.get("model") is not None:
            traces.append(
                LLMTrace(
                    messages=_parse_messages(observation.get("input")),
                    model=str(observation["model"]),
                    output=_stringify(observation.get("output")),
                    input_tokens=usage.get("input"),
                    output_tokens=usage.get("output"),
                )
            )
        else:
            traces.append(
                RetrievalTrace(
                    source=str(observation.get("name") or "retrieval"),
                    input=_extract_user_input(observation.get("input"))
                    or _stringify(observation.get("input")),
                    outputs=[_stringify(observation.get("output"))],
                )
            )
    return traces


def interaction_from_langfuse_trace(
    trace: LangfuseTrace, observations: list[LangfuseObservation]
) -> Interaction | None:
    session_id = trace.get("sessionId") or trace.get("id") or "unknown"
    end_user = trace.get("userId") or "unknown"
    time_start = trace.get("timestamp") or ""
    time_end = time_start
    for observation in observations:
        end_time = observation.get("endTime")
        if end_time and (not time_end or end_time > time_end):
            time_end = end_time

    input_text = _extract_user_input(trace.get("input")).strip()
    if not input_text:
        for observation in observations:
            if observation.get("model") is None:
                continue
            input_text = _extract_user_input(observation.get("input")).strip()
            if input_text:
                break
    output_text = _stringify(trace.get("output")).strip()
    if not input_text and not output_text:
        return None

    return Interaction(
        conversation_id=str(session_id),
        input=input_text,
        output=output_text,
        time_start=str(time_start),
        time_end=str(time_end),
        end_user=str(end_user),
        tags=_langfuse_tags_to_dict(trace.get("tags")),
        traces=convert_observations_to_traces(observations),
    )
