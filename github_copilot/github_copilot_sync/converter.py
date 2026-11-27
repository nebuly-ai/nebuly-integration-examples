"""Map closed turns to Nebuly trace_interaction payloads."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from .config import datetime_to_timestamp_str
from .entra import apply_directory_segments, apply_directory_tags
from .models import (
    BlockKind,
    Call,
    ClosedTurn,
    Message,
    join_text,
    turn_start,
    turn_start_index,
)
from .openai_body import BodyFormatError, ParsedBody, parse_call_bodies

if TYPE_CHECKING:
    from .entra import EntraDirectory

MAX_CONTENT_CHARS = 8000


class SkipReason(Enum):
    EMPTY_INPUT = "empty_input"
    EMPTY_OUTPUT = "empty_output"


def _truncate(text: str) -> str:
    return text[:MAX_CONTENT_CHARS]


def _ms_to_iso(ms: int) -> str:
    return datetime_to_timestamp_str(datetime.fromtimestamp(ms / 1000, tz=UTC))


def _parse_call(call: Call) -> ParsedBody | None:
    try:
        return parse_call_bodies(call.request_body, call.response_body)
    except BodyFormatError:
        return None


def _turn_input(first: ParsedBody | None) -> str:
    if first is None:
        return ""
    start = turn_start(first.messages)
    if start is None:
        return ""
    return _truncate(join_text(start.blocks))


def _final_output(parsed_calls: list[ParsedBody]) -> str:
    for parsed in reversed(parsed_calls):
        if any(block.kind is BlockKind.TOOL_USE for block in parsed.output):
            continue
        text = join_text(
            tuple(block for block in parsed.output if block.kind is BlockKind.TEXT)
        )
        if text.strip():
            return _truncate(text.strip())
    return ""


def _turn_messages(parsed: ParsedBody) -> tuple[Message, ...]:
    """Messages after the turn-start user message, dropping earlier turns."""
    index = turn_start_index(parsed.messages)
    return () if index is None else parsed.messages[index + 1 :]


def _tool_names(parsed_calls: list[ParsedBody]) -> list[str]:
    names: set[str] = set()
    for parsed in parsed_calls:
        for message in _turn_messages(parsed):
            for block in message.blocks:
                if block.kind is BlockKind.TOOL_USE and block.tool_name:
                    names.add(block.tool_name)
        for block in parsed.output:
            if block.kind is BlockKind.TOOL_USE and block.tool_name:
                names.add(block.tool_name)
    return sorted(names)


def _llm_traces(
    parsed_calls: list[ParsedBody], user_input: str
) -> list[dict[str, Any]]:
    by_model: dict[str, list[ParsedBody]] = defaultdict(list)
    for parsed in parsed_calls:
        by_model[parsed.model or "unknown"].append(parsed)

    traces: list[dict[str, Any]] = []
    for model, model_calls in sorted(by_model.items()):
        output = ""
        for parsed in reversed(model_calls):
            text = join_text(
                tuple(block for block in parsed.output if block.kind is BlockKind.TEXT)
            ).strip()
            if text:
                output = _truncate(text)
                break
        usage_calls = [parsed for parsed in model_calls if parsed.usage is not None]
        trace: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": user_input}],
            "output": output,
            "input_tokens": sum(
                (parsed.usage.input_tokens if parsed.usage else 0)
                for parsed in usage_calls
            ),
            "output_tokens": sum(
                (parsed.usage.output_tokens if parsed.usage else 0)
                for parsed in usage_calls
            ),
            "cache_read_input_tokens": sum(
                (parsed.usage.cache_read_tokens if parsed.usage else 0)
                for parsed in usage_calls
            ),
            "cache_write_input_tokens": sum(
                (parsed.usage.cache_write_tokens if parsed.usage else 0)
                for parsed in usage_calls
            ),
        }
        traces.append(trace)
    return traces


def _retrieval_traces(parsed: ParsedBody | None) -> list[dict[str, Any]]:
    if parsed is None:
        return []

    pending: list[tuple[str, str, object]] = []
    traces: list[dict[str, Any]] = []
    for message in _turn_messages(parsed):
        if message.role == "assistant":
            pending.extend(
                (
                    block.tool_use_id or "",
                    block.tool_name or "tool",
                    block.tool_input,
                )
                for block in message.blocks
                if block.kind is BlockKind.TOOL_USE
            )
        else:
            for block in message.blocks:
                if block.kind is not BlockKind.TOOL_RESULT:
                    continue
                match_index = next(
                    (
                        index
                        for index, item in enumerate(pending)
                        if item[0] and item[0] == (block.tool_use_id or "")
                    ),
                    None,
                )
                if match_index is None:
                    continue
                _, tool_name, tool_input = pending.pop(match_index)
                traces.append(
                    {
                        "source": tool_name,
                        "input": _truncate(str(tool_input)),
                        "outputs": [_truncate(block.text)],
                    }
                )
    return traces


def _build_tags(
    calls: tuple[Call, ...], parsed_calls: list[ParsedBody]
) -> dict[str, str]:
    endpoints = sorted({call.endpoint for call in calls})
    models = [parsed.model for parsed in parsed_calls if parsed.model]
    unique_models = list(dict.fromkeys(models))
    tools = _tool_names(parsed_calls)
    tags: dict[str, str] = {
        "endpoint": endpoints[0] if len(endpoints) == 1 else ",".join(endpoints),
        "model": unique_models[-1] if unique_models else "unknown",
    }
    if len(unique_models) > 1:
        tags["models"] = ",".join(unique_models)
    if tools:
        tags["tools used"] = ",".join(tools)
    if any(call.truncated for call in calls):
        tags["truncated"] = "true"
    return tags


def turn_to_payload(
    turn: ClosedTurn,
    *,
    end_user: str,
    email: str | None,
    anonymize: bool,
    directory: EntraDirectory | None,
) -> dict[str, Any] | SkipReason:
    ordered_calls = sorted(
        turn.calls,
        key=lambda call: (call.request_timestamp_ms, call.response_timestamp_ms),
    )
    parsed_by_call = [_parse_call(call) for call in ordered_calls]
    parsed_calls = [parsed for parsed in parsed_by_call if parsed is not None]
    user_input = _turn_input(parsed_by_call[0])
    if not user_input.strip():
        return SkipReason.EMPTY_INPUT
    output = _final_output(parsed_calls)
    if not output.strip():
        return SkipReason.EMPTY_OUTPUT

    tags = apply_directory_tags(_build_tags(turn.calls, parsed_calls), directory, email)
    interaction: dict[str, Any] = {
        "conversation_id": turn.conversation_id,
        "input": user_input,
        "output": output,
        "time_start": _ms_to_iso(turn.time_start_ms),
        "time_end": _ms_to_iso(turn.time_end_ms),
        "end_user": end_user,
        "hide_content": False,
        "tags": tags,
    }
    apply_directory_segments(interaction, directory, email)

    traces = _llm_traces(parsed_calls, user_input)
    traces.extend(_retrieval_traces(parsed_by_call[-1]))
    return {
        "interaction": interaction,
        "traces": traces,
        "user_feedback": [],
        "anonymize": anonymize,
    }
