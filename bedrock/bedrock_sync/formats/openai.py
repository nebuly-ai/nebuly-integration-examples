"""OpenAI Chat Completions on InvokeModel and InvokeModelWithResponseStream.

The non-streamed output is a ``chat.completion`` object. The streamed output is
the list of ``chat.completion.chunk`` objects; Bedrock appends
``amazon-bedrock-invocationMetrics`` to the last one.

OpenAI puts the system prompt in the message list and tool results in
``role: tool`` messages. They are mapped to the shared model the Anthropic way:
system text apart, tool results as blocks of a user message.
"""

from __future__ import annotations

from typing import Any

from bedrock_sync.formats.base import (
    BodyFormatError,
    ParsedBody,
    as_dict,
    as_int,
    as_list,
    digest,
    parse_tool_arguments,
)
from bedrock_sync.models import Block, BlockKind, Message, StopReason, TokenUsage

_STOP_REASONS = {
    "stop": StopReason.END_TURN,
    "tool_calls": StopReason.TOOL_USE,
    "function_call": StopReason.TOOL_USE,
    "length": StopReason.MAX_TOKENS,
    "content_filter": StopReason.CONTENT_FILTERED,
}
_SYSTEM_ROLES = ("system", "developer")


def matches(output_body: object) -> bool:
    if isinstance(output_body, dict):
        return (
            output_body.get("object") == "chat.completion" or "choices" in output_body
        )
    if isinstance(output_body, list) and output_body:
        return as_dict(output_body[0]).get("object") == "chat.completion.chunk"
    return False


def _content_blocks(content: object) -> list[Block]:
    if isinstance(content, str):
        return [Block(BlockKind.TEXT, text=content)] if content else []
    blocks: list[Block] = []
    for raw in as_list(content):
        part = as_dict(raw)
        part_type = part.get("type")
        if part_type in ("text", "input_text"):
            blocks.append(Block(BlockKind.TEXT, text=str(part.get("text") or "")))
        elif part_type == "image_url":
            blocks.append(Block(BlockKind.IMAGE, text=digest(part.get("image_url"))))
        elif part_type == "file":
            blocks.append(Block(BlockKind.DOCUMENT, text=digest(part.get("file"))))
        else:
            blocks.append(Block(BlockKind.OTHER))
    return blocks


def _tool_call_blocks(tool_calls: object) -> list[Block]:
    blocks: list[Block] = []
    for raw in as_list(tool_calls):
        call = as_dict(raw)
        function = as_dict(call.get("function"))
        blocks.append(
            Block(
                BlockKind.TOOL_USE,
                tool_use_id=str(call.get("id") or ""),
                tool_name=str(function.get("name") or ""),
                tool_input=parse_tool_arguments(function.get("arguments")),
            )
        )
    return blocks


def _parse_messages(raw_messages: object) -> tuple[str, tuple[Message, ...]]:
    if not isinstance(raw_messages, list):
        raise BodyFormatError("OpenAI input has no messages list")
    system_parts: list[str] = []
    messages: list[Message] = []
    for raw in raw_messages:
        message = as_dict(raw)
        role = message.get("role")
        if role in _SYSTEM_ROLES:
            system_parts.extend(b.text for b in _content_blocks(message.get("content")))
        elif role == "user":
            messages.append(
                Message("user", tuple(_content_blocks(message.get("content"))))
            )
        elif role == "assistant":
            blocks = _content_blocks(message.get("content"))
            blocks += _tool_call_blocks(message.get("tool_calls"))
            messages.append(Message("assistant", tuple(blocks)))
        elif role == "tool":
            result = Block(
                BlockKind.TOOL_RESULT,
                text="\n".join(b.text for b in _content_blocks(message.get("content"))),
                tool_use_id=str(message.get("tool_call_id") or ""),
            )
            # Consecutive tool messages answer one assistant turn: keep them
            # in a single user message, like Anthropic and Converse do.
            if (
                messages
                and messages[-1].role == "user"
                and messages[-1].tool_result_ids
            ):
                messages[-1] = Message("user", (*messages[-1].blocks, result))
            else:
                messages.append(Message("user", (result,)))
    return "\n".join(p for p in system_parts if p), tuple(messages)


def _usage(usage: dict[str, Any]) -> TokenUsage | None:
    if not usage:
        return None
    prompt_details = as_dict(usage.get("prompt_tokens_details"))
    cache_read = as_int(prompt_details.get("cached_tokens")) or 0
    cache_write = as_int(prompt_details.get("cache_write_tokens")) or 0
    prompt = as_int(usage.get("prompt_tokens")) or 0
    return TokenUsage(
        # OpenAI counts cached tokens inside prompt_tokens; split them out to
        # match the Bedrock envelope.
        input_tokens=max(prompt - cache_read - cache_write, 0),
        output_tokens=as_int(usage.get("completion_tokens")) or 0,
        cache_read_tokens=cache_read,
        cache_write_tokens=cache_write,
    )


def _reconstruct_stream(
    chunks: list[Any],
) -> tuple[tuple[Block, ...], str | None, TokenUsage | None, int | None]:
    text_parts: list[str] = []
    calls: dict[int, dict[str, str]] = {}
    finish_reason: str | None = None
    usage: TokenUsage | None = None
    latency_ms: int | None = None

    for raw in chunks:
        chunk = as_dict(raw)
        for choice_raw in as_list(chunk.get("choices")):
            choice = as_dict(choice_raw)
            delta = as_dict(choice.get("delta"))
            if isinstance(delta.get("content"), str):
                text_parts.append(delta["content"])
            for call_raw in as_list(delta.get("tool_calls")):
                call_delta = as_dict(call_raw)
                index = as_int(call_delta.get("index")) or 0
                call = calls.setdefault(index, {"id": "", "name": "", "arguments": ""})
                function = as_dict(call_delta.get("function"))
                call["id"] = str(call_delta.get("id") or call["id"])
                call["name"] = str(function.get("name") or call["name"])
                call["arguments"] += str(function.get("arguments") or "")
            if choice.get("finish_reason"):
                finish_reason = str(choice["finish_reason"])
        if chunk.get("usage"):
            usage = _usage(as_dict(chunk.get("usage")))
        metrics = as_dict(chunk.get("amazon-bedrock-invocationMetrics"))
        if metrics:
            latency_ms = as_int(metrics.get("invocationLatency"))

    blocks = _content_blocks("".join(text_parts))
    blocks += [
        Block(
            BlockKind.TOOL_USE,
            tool_use_id=call["id"],
            tool_name=call["name"],
            tool_input=parse_tool_arguments(call["arguments"]),
        )
        for _, call in sorted(calls.items())
    ]
    return tuple(blocks), finish_reason, usage, latency_ms


def parse(input_body: dict[str, Any], output_body: object) -> ParsedBody:
    if isinstance(output_body, list):
        output, finish_reason, usage, latency_ms = _reconstruct_stream(output_body)
    elif isinstance(output_body, dict):
        choices = as_list(output_body.get("choices"))
        choice = as_dict(choices[0]) if choices else {}
        message = as_dict(choice.get("message"))
        output = tuple(
            _content_blocks(message.get("content"))
            + _tool_call_blocks(message.get("tool_calls"))
        )
        finish_reason = choice.get("finish_reason")
        usage = _usage(as_dict(output_body.get("usage")))
        latency_ms = None
    else:
        raise BodyFormatError("OpenAI output is neither a completion nor a stream")

    system, messages = _parse_messages(input_body.get("messages"))
    return ParsedBody(
        system=system,
        messages=messages,
        output=output,
        stop_reason=_STOP_REASONS.get(str(finish_reason), StopReason.OTHER),
        usage=usage,
        latency_ms=latency_ms,
        max_tokens=as_int(
            input_body.get("max_completion_tokens") or input_body.get("max_tokens")
        ),
    )
