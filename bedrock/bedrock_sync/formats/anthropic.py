"""Anthropic Messages API on InvokeModel and InvokeModelWithResponseStream.

The non-streamed output is a ``message`` object. The streamed output is the
list of SSE events (``message_start`` ... ``message_stop``); Bedrock appends
``amazon-bedrock-invocationMetrics`` to the last one.
"""

from __future__ import annotations

import json
from typing import Any

from bedrock_sync.formats.base import (
    BodyFormatError,
    ParsedBody,
    as_dict,
    as_int,
    as_list,
    digest,
)
from bedrock_sync.models import Block, BlockKind, Message, StopReason, TokenUsage

_STOP_REASONS = {
    "end_turn": StopReason.END_TURN,
    "tool_use": StopReason.TOOL_USE,
    "max_tokens": StopReason.MAX_TOKENS,
    "stop_sequence": StopReason.STOP_SEQUENCE,
    "refusal": StopReason.CONTENT_FILTERED,
}


def matches(input_body: dict[str, Any], output_body: object) -> bool:
    if "anthropic_version" in input_body:
        return True
    if isinstance(output_body, dict):
        return output_body.get("type") == "message"
    if isinstance(output_body, list) and output_body:
        return as_dict(output_body[0]).get("type") == "message_start"
    return False


def _tool_result_text(content: object) -> str:
    if isinstance(content, str):
        return content
    parts: list[str] = []
    for item in as_list(content):
        item_dict = as_dict(item)
        if item_dict.get("type") == "text":
            parts.append(str(item_dict.get("text") or ""))
    return "\n".join(parts)


def _parse_block(raw: object) -> Block | None:
    block = as_dict(raw)
    block_type = block.get("type")
    if block_type == "text":
        return Block(BlockKind.TEXT, text=str(block.get("text") or ""))
    if block_type in ("tool_use", "server_tool_use"):
        return Block(
            BlockKind.TOOL_USE,
            tool_use_id=str(block.get("id") or ""),
            tool_name=str(block.get("name") or ""),
            tool_input=block.get("input"),
        )
    if block_type == "tool_result":
        return Block(
            BlockKind.TOOL_RESULT,
            text=_tool_result_text(block.get("content")),
            tool_use_id=str(block.get("tool_use_id") or ""),
            is_error=bool(block.get("is_error")),
        )
    if block_type == "thinking":
        return Block(BlockKind.REASONING, text=str(block.get("thinking") or ""))
    if block_type == "redacted_thinking":
        return Block(BlockKind.REASONING)
    if block_type == "image":
        return Block(BlockKind.IMAGE, text=digest(block.get("source")))
    if block_type == "document":
        return Block(BlockKind.DOCUMENT, text=digest(block.get("source")))
    return Block(BlockKind.OTHER)


def _parse_content(content: object) -> tuple[Block, ...]:
    if isinstance(content, str):
        return (Block(BlockKind.TEXT, text=content),)
    return tuple(b for raw in as_list(content) if (b := _parse_block(raw)) is not None)


def _parse_messages(raw_messages: object) -> tuple[Message, ...]:
    if not isinstance(raw_messages, list):
        raise BodyFormatError("Anthropic input has no messages list")
    messages: list[Message] = []
    for raw in raw_messages:
        message = as_dict(raw)
        role = message.get("role")
        if role not in ("user", "assistant"):
            continue
        messages.append(
            Message(role=role, blocks=_parse_content(message.get("content")))
        )
    return tuple(messages)


def _parse_system(system: object) -> str:
    if isinstance(system, str):
        return system
    return "\n".join(
        str(item.get("text") or "")
        for item in as_list(system)
        if isinstance(item, dict) and item.get("type") == "text"
    )


def _usage(usage: dict[str, Any]) -> TokenUsage:
    return TokenUsage(
        input_tokens=as_int(usage.get("input_tokens")) or 0,
        output_tokens=as_int(usage.get("output_tokens")) or 0,
        cache_read_tokens=as_int(usage.get("cache_read_input_tokens")) or 0,
        cache_write_tokens=as_int(usage.get("cache_creation_input_tokens")) or 0,
    )


def _merge_usage(base: TokenUsage, update: dict[str, Any]) -> TokenUsage:
    """``message_delta`` usage only carries the counters that changed."""
    new = _usage(update)
    return TokenUsage(
        input_tokens=new.input_tokens or base.input_tokens,
        output_tokens=new.output_tokens or base.output_tokens,
        cache_read_tokens=new.cache_read_tokens or base.cache_read_tokens,
        cache_write_tokens=new.cache_write_tokens or base.cache_write_tokens,
    )


_PARTIAL_JSON = "_partial_json"
# delta type -> (field in the delta, field of the block it extends)
_DELTA_FIELDS = {
    "text_delta": ("text", "text"),
    "thinking_delta": ("thinking", "thinking"),
    "input_json_delta": ("partial_json", _PARTIAL_JSON),
}


def _apply_delta(block: dict[str, Any], delta: dict[str, Any]) -> None:
    fields = _DELTA_FIELDS.get(str(delta.get("type")))
    if fields is None:
        return
    source, target = fields
    block[target] = str(block.get(target, "")) + str(delta.get(source) or "")


def _finish_block(block: dict[str, Any]) -> dict[str, Any]:
    """Tool input arrives as JSON fragments; parse it once the block is whole."""
    if _PARTIAL_JSON not in block:
        return block
    buffer = str(block.pop(_PARTIAL_JSON))
    try:
        block["input"] = json.loads(buffer) if buffer else {}
    except json.JSONDecodeError:
        block["input"] = buffer
    return block


def _reconstruct_stream(
    events: list[Any],
) -> tuple[tuple[Block, ...], str | None, TokenUsage, int | None]:
    blocks: dict[int, dict[str, Any]] = {}
    stop_reason: str | None = None
    usage = TokenUsage()
    latency_ms: int | None = None

    for raw in events:
        event = as_dict(raw)
        event_type = event.get("type")
        index = as_int(event.get("index"))
        if event_type == "message_start":
            usage = _usage(as_dict(as_dict(event.get("message")).get("usage")))
        elif event_type == "content_block_start" and index is not None:
            blocks[index] = dict(as_dict(event.get("content_block")))
        elif event_type == "content_block_delta" and index is not None:
            _apply_delta(blocks.setdefault(index, {}), as_dict(event.get("delta")))
        elif event_type == "message_delta":
            delta = as_dict(event.get("delta"))
            if delta.get("stop_reason"):
                stop_reason = str(delta["stop_reason"])
            usage = _merge_usage(usage, as_dict(event.get("usage")))

        metrics = as_dict(event.get("amazon-bedrock-invocationMetrics"))
        if metrics:
            latency_ms = as_int(metrics.get("invocationLatency"))

    content = tuple(
        b
        for i in sorted(blocks)
        if (b := _parse_block(_finish_block(blocks[i]))) is not None
    )
    return content, stop_reason, usage, latency_ms


def parse(input_body: dict[str, Any], output_body: object) -> ParsedBody:
    if isinstance(output_body, list):
        output, stop_reason, usage, latency_ms = _reconstruct_stream(output_body)
    elif isinstance(output_body, dict):
        output = _parse_content(output_body.get("content"))
        stop_reason = output_body.get("stop_reason")
        usage = _usage(as_dict(output_body.get("usage")))
        latency_ms = None
    else:
        raise BodyFormatError("Anthropic output is neither a message nor a stream")

    return ParsedBody(
        system=_parse_system(input_body.get("system")),
        messages=_parse_messages(input_body.get("messages")),
        output=output,
        stop_reason=_STOP_REASONS.get(str(stop_reason), StopReason.OTHER),
        usage=usage,
        latency_ms=latency_ms,
        max_tokens=as_int(input_body.get("max_tokens")),
    )
