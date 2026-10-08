"""Convert Langfuse traces/observations into Nebuly interactions."""

from __future__ import annotations

import json
from typing import cast

from langfuse_sync.models import (
    ChatMessage,
    EmbeddingTrace,
    Interaction,
    JsonPrimitive,
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


_SKIP_OBSERVATION_TYPES = frozenset(
    {"EVENT", "AGENT", "CHAIN", "EVALUATOR", "GUARDRAIL"}
)


def _langfuse_tags_to_dict(tags: list[str] | dict[str, str] | None) -> dict[str, str]:
    if not tags:
        return {}
    if isinstance(tags, dict):
        return {str(k): str(v) for k, v in tags.items()}

    grouped: dict[str, set[str]] = {}
    for tag in tags:
        if tag is None:
            continue
        text = str(tag)
        if ":" in text:
            key, value = text.split(":", 1)
            key, value = key.strip(), value.strip()
        else:
            key, value = text.strip(), "true"
        if not key:
            continue
        grouped.setdefault(key, set()).add(value)
    return {key: ", ".join(sorted(values)) for key, values in grouped.items()}


def _scalar_to_str(value: JsonPrimitive) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _flatten_metadata(value: JsonValue | None, prefix: str = "") -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        result: dict[str, str] = {}
        for key, nested in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            result.update(_flatten_metadata(nested, child_prefix))
        return result
    if isinstance(value, list):
        if not value:
            return {}
        key = prefix or "metadata"
        scalar_parts: list[str] = []
        for item in value:
            if item is None or isinstance(item, (dict, list)):
                scalar_parts = []
                break
            scalar_parts.append(_scalar_to_str(item))
        else:
            return {key: ", ".join(scalar_parts)}
        return {key: json.dumps(value)}
    if not isinstance(value, (str, int, float, bool)):
        return {}
    key = prefix or "metadata"
    return {key: _scalar_to_str(value)}


def _observation_cost_micro_dollars(observation: LangfuseObservation) -> int | None:
    calculated = observation.get("calculatedTotalCost")
    if calculated is not None:
        return round(float(calculated) * 1_000_000)
    cost_details = observation.get("costDetails") or {}
    total = cost_details.get("total")
    if total is not None:
        return round(float(total) * 1_000_000)
    return None


def _observation_token_counts(
    observation: LangfuseObservation,
) -> tuple[int | None, int | None]:
    usage = observation.get("usageDetails") or {}
    fallback = observation.get("usage") or {}
    input_tokens = usage.get("input")
    if input_tokens is None:
        input_tokens = fallback.get("input")
    output_tokens = usage.get("output")
    if output_tokens is None:
        output_tokens = fallback.get("output")
    return input_tokens, output_tokens


def _child_observation_ids(observations: list[LangfuseObservation]) -> set[str]:
    return {
        parent_id
        for observation in observations
        if (parent_id := observation.get("parentObservationId"))
    }


def _sorted_observations(
    observations: list[LangfuseObservation],
) -> list[LangfuseObservation]:
    return sorted(
        observations,
        key=lambda obs: (obs.get("startTime") or "", obs.get("id") or ""),
    )


def convert_observations_to_traces(
    observations: list[LangfuseObservation],
) -> list[RetrievalTrace | LLMTrace | EmbeddingTrace]:
    if not observations:
        return []

    parents_with_children = _child_observation_ids(observations)
    traces: list[RetrievalTrace | LLMTrace | EmbeddingTrace] = []

    for observation in _sorted_observations(observations):
        obs_type = (observation.get("type") or "").upper()
        obs_id = observation.get("id") or ""

        if obs_type in _SKIP_OBSERVATION_TYPES:
            continue

        if obs_type == "SPAN" and obs_id in parents_with_children:
            continue

        if obs_type == "EMBEDDING":
            model = str(
                observation.get("model") or observation.get("name") or "embedding"
            )
            input_tokens, _ = _observation_token_counts(observation)
            traces.append(
                EmbeddingTrace(
                    model=model,
                    input=_stringify(observation.get("input")),
                    input_tokens=input_tokens,
                )
            )
            continue

        if obs_type in {"RETRIEVER", "TOOL"} or (
            obs_type == "SPAN"
            and obs_id not in parents_with_children
            and _stringify(observation.get("output")).strip()
        ):
            traces.append(
                RetrievalTrace(
                    source=str(
                        observation.get("name") or obs_type.lower() or "retrieval"
                    ),
                    input=_extract_user_input(observation.get("input"))
                    or _stringify(observation.get("input")),
                    outputs=[_stringify(observation.get("output"))],
                )
            )
            continue

        if obs_type == "GENERATION" or observation.get("model") is not None:
            input_tokens, output_tokens = _observation_token_counts(observation)
            model = str(
                observation.get("model") or observation.get("name") or "unknown"
            )
            traces.append(
                LLMTrace(
                    messages=_parse_messages(observation.get("input")),
                    model=model,
                    output=_stringify(observation.get("output")),
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cost=_observation_cost_micro_dollars(observation),
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
        tags={
            **_flatten_metadata(trace.get("metadata")),
            **_langfuse_tags_to_dict(trace.get("tags")),
        },
        traces=convert_observations_to_traces(observations),
    )
