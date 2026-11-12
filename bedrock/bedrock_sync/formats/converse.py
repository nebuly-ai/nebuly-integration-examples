"""Converse and ConverseStream.

Bedrock logs ConverseStream with the response already assembled, so both
operations have the same output shape::

    {"output": {"message": {"role", "content": [...]}}, "stopReason",
     "metrics": {"latencyMs"}, "usage": {"inputTokens", "outputTokens",
     "cacheReadInputTokens", "cacheWriteInputTokens"}}

Amazon Nova on InvokeModel uses the same request and response shapes, with
``max_new_tokens`` and ``cache*InputTokenCount`` spellings.
"""

from __future__ import annotations

from typing import Any

from bedrock_sync.formats.base import (
    BodyFormatError,
    ParsedBody,
    as_dict,
    as_int,
    as_list,
    canonical_json,
    digest,
)
from bedrock_sync.models import Block, BlockKind, Message, StopReason, TokenUsage

_STOP_REASONS = {
    "end_turn": StopReason.END_TURN,
    "tool_use": StopReason.TOOL_USE,
    "max_tokens": StopReason.MAX_TOKENS,
    "stop_sequence": StopReason.STOP_SEQUENCE,
    "guardrail_intervened": StopReason.CONTENT_FILTERED,
    "content_filtered": StopReason.CONTENT_FILTERED,
}


def matches(output_body: object) -> bool:
    """Converse-shaped output on InvokeModel (Amazon Nova)."""
    output = as_dict(as_dict(output_body).get("output"))
    return isinstance(output.get("message"), dict)


def _first_int(source: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = as_int(source.get(key))
        if value is not None:
            return value
    return None


def _tool_result_text(content: object) -> str:
    parts: list[str] = []
    for item in as_list(content):
        item_dict = as_dict(item)
        if "text" in item_dict:
            parts.append(str(item_dict["text"]))
        elif "json" in item_dict:
            parts.append(canonical_json(item_dict["json"]))
    return "\n".join(parts)


def _parse_block(raw: object) -> Block | None:
    block = as_dict(raw)
    if "text" in block:
        return Block(BlockKind.TEXT, text=str(block["text"]))
    if "guardContent" in block:
        # Text the caller marked for Guardrails: still the user's words.
        guarded = as_dict(as_dict(block["guardContent"]).get("text"))
        return Block(BlockKind.TEXT, text=str(guarded.get("text") or ""))
    if "toolUse" in block:
        tool = as_dict(block["toolUse"])
        return Block(
            BlockKind.TOOL_USE,
            tool_use_id=str(tool.get("toolUseId") or ""),
            tool_name=str(tool.get("name") or ""),
            tool_input=tool.get("input"),
        )
    if "toolResult" in block:
        result = as_dict(block["toolResult"])
        return Block(
            BlockKind.TOOL_RESULT,
            text=_tool_result_text(result.get("content")),
            tool_use_id=str(result.get("toolUseId") or ""),
            is_error=result.get("status") == "error",
        )
    if "reasoningContent" in block:
        reasoning = as_dict(as_dict(block["reasoningContent"]).get("reasoningText"))
        return Block(BlockKind.REASONING, text=str(reasoning.get("text") or ""))
    if "image" in block:
        return Block(BlockKind.IMAGE, text=digest(block["image"]))
    if "document" in block:
        return Block(BlockKind.DOCUMENT, text=digest(block["document"]))
    if "cachePoint" in block:
        return None
    return Block(BlockKind.OTHER)


def _parse_content(content: object) -> tuple[Block, ...]:
    return tuple(b for raw in as_list(content) if (b := _parse_block(raw)) is not None)


def _parse_messages(raw_messages: object) -> tuple[Message, ...]:
    if not isinstance(raw_messages, list):
        raise BodyFormatError("Converse input has no messages list")
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


def parse(input_body: dict[str, Any], output_body: object) -> ParsedBody:
    if not isinstance(output_body, dict):
        raise BodyFormatError("Converse output is not an object")

    system = "\n".join(
        str(item["text"])
        for item in as_list(input_body.get("system"))
        if isinstance(item, dict) and "text" in item
    )
    output_message = as_dict(as_dict(output_body.get("output")).get("message"))
    usage = as_dict(output_body.get("usage"))

    return ParsedBody(
        system=system,
        messages=_parse_messages(input_body.get("messages")),
        output=_parse_content(output_message.get("content")),
        stop_reason=_STOP_REASONS.get(
            str(output_body.get("stopReason")), StopReason.OTHER
        ),
        usage=TokenUsage(
            input_tokens=as_int(usage.get("inputTokens")) or 0,
            output_tokens=as_int(usage.get("outputTokens")) or 0,
            cache_read_tokens=_first_int(
                usage, "cacheReadInputTokens", "cacheReadInputTokenCount"
            )
            or 0,
            cache_write_tokens=_first_int(
                usage, "cacheWriteInputTokens", "cacheWriteInputTokenCount"
            )
            or 0,
        )
        if usage
        else None,
        latency_ms=as_int(as_dict(output_body.get("metrics")).get("latencyMs")),
        max_tokens=_first_int(
            as_dict(input_body.get("inferenceConfig")), "maxTokens", "max_new_tokens"
        ),
    )
