"""OpenAI Chat Completions body parsing for Copilot usage record bodies.

Bodies are JSON-encoded strings in usage records. Streamed responses may be
stored as SSE text rather than a single JSON object (assumption, preview API).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .models import (
    Block,
    BlockKind,
    Message,
    StopReason,
    TokenUsage,
    digest,
    join_text,
)

_SYSTEM_ROLES = ("system", "developer")
_STOP_REASONS = {
    "stop": StopReason.END_TURN,
    "tool_calls": StopReason.TOOL_USE,
    "function_call": StopReason.TOOL_USE,
    "length": StopReason.MAX_TOKENS,
    "content_filter": StopReason.CONTENT_FILTERED,
}


class BodyFormatError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedBody:
    system: str
    messages: tuple[Message, ...]
    output: tuple[Block, ...]
    stop_reason: StopReason
    usage: TokenUsage | None
    model: str | None


def as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value)
    return None


def as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def parse_tool_arguments(raw: object) -> object:
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


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
            system_parts.extend(
                block.text for block in _content_blocks(message.get("content"))
            )
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
                text="\n".join(
                    block.text for block in _content_blocks(message.get("content"))
                ),
                tool_use_id=str(message.get("tool_call_id") or ""),
            )
            if (
                messages
                and messages[-1].role == "user"
                and messages[-1].tool_result_ids
            ):
                messages[-1] = Message("user", (*messages[-1].blocks, result))
            else:
                messages.append(Message("user", (result,)))
    return "\n".join(part for part in system_parts if part), tuple(messages)


def _usage(usage: dict[str, Any]) -> TokenUsage | None:
    if not usage:
        return None
    prompt_details = as_dict(usage.get("prompt_tokens_details"))
    cache_read = as_int(prompt_details.get("cached_tokens")) or 0
    cache_write = as_int(prompt_details.get("cache_write_tokens")) or 0
    prompt = as_int(usage.get("prompt_tokens")) or 0
    return TokenUsage(
        input_tokens=max(prompt - cache_read - cache_write, 0),
        output_tokens=as_int(usage.get("completion_tokens")) or 0,
        cache_read_tokens=cache_read,
        cache_write_tokens=cache_write,
    )


def _reconstruct_stream(
    chunks: list[Any],
) -> tuple[tuple[Block, ...], str | None, TokenUsage | None]:
    text_parts: list[str] = []
    calls: dict[int, dict[str, str]] = {}
    finish_reason: str | None = None
    usage: TokenUsage | None = None

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
    return tuple(blocks), finish_reason, usage


def decode_body_string(body: str) -> object:
    """Decode the JSON-encoded body field from a usage record."""
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError as exc:
        raise BodyFormatError("body is not valid JSON") from exc
    if isinstance(decoded, str):
        return _decode_embedded_payload(decoded)
    return decoded


def _decode_embedded_payload(text: str) -> object:
    stripped = text.strip()
    if not stripped:
        raise BodyFormatError("empty embedded body")
    if stripped.startswith(("{", "[")):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass
    if "data:" in stripped:
        return _parse_sse_text(stripped)
    raise BodyFormatError("unrecognized embedded body format")


def _parse_sse_text(text: str) -> list[Any]:
    chunks: list[Any] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if not payload or payload == "[DONE]":
            continue
        try:
            chunks.append(json.loads(payload))
        except json.JSONDecodeError:
            continue
    if not chunks:
        raise BodyFormatError("SSE body had no parseable chunks")
    return chunks


def parse_response_body(body: str) -> ParsedBody:
    decoded = decode_body_string(body)
    input_stub: dict[str, Any] = {"messages": []}
    return _parse_bodies(input_stub, decoded)


def parse_call_bodies(request_body: str, response_body: str) -> ParsedBody:
    request = decode_body_string(request_body)
    response = decode_body_string(response_body)
    if not isinstance(request, dict):
        raise BodyFormatError("request body is not an object")
    return _parse_bodies(request, response)


def _parse_bodies(input_body: dict[str, Any], output_body: object) -> ParsedBody:
    model = str(input_body.get("model") or "") or None
    if isinstance(output_body, list):
        output, finish_reason, usage = _reconstruct_stream(output_body)
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
        if not model:
            model = str(output_body.get("model") or "") or None
    else:
        raise BodyFormatError("output is neither a completion nor a stream")

    system, messages = _parse_messages(input_body.get("messages"))
    return ParsedBody(
        system=system,
        messages=messages,
        output=output,
        stop_reason=_STOP_REASONS.get(str(finish_reason), StopReason.OTHER),
        usage=usage,
        model=model,
    )


def turn_key_for_call(request: dict[str, Any]) -> str | None:
    """Digest of the raw messages through the turn-start user message.

    Not scoped to a user: the conversation id the key is combined with already is.
    """
    raw_messages = request.get("messages")
    if not isinstance(raw_messages, list):
        return None
    start_index: int | None = None
    for index in range(len(raw_messages) - 1, -1, -1):
        message = raw_messages[index]
        if isinstance(message, dict) and message.get("role") == "user":
            start_index = index
            break
    if start_index is None:
        return None
    return digest(raw_messages[: start_index + 1])


def first_user_message_text(request: dict[str, Any]) -> str | None:
    _system, messages = _parse_messages(request.get("messages"))
    for message in messages:
        if message.role == "user" and not message.tool_result_ids:
            text = join_text(message.blocks)
            if text:
                return text
    return None
