from __future__ import annotations

import json

import pytest
from github_copilot_sync.models import BlockKind, StopReason
from github_copilot_sync.openai_body import (
    BodyFormatError,
    decode_body_string,
    parse_call_bodies,
    parse_response_body,
    turn_key_for_call,
)


def _wrap_body(payload: object) -> str:
    return json.dumps(json.dumps(payload))


def _chat_request(
    messages: list[dict[str, object]], tools: list[dict[str, object]] | None = None
) -> str:
    body: dict[str, object] = {"model": "gpt-4o", "messages": messages}
    if tools is not None:
        body["tools"] = tools
    return _wrap_body(body)


def _chat_response(
    content: str,
    *,
    tool_calls: list[dict[str, object]] | None = None,
    usage: dict[str, int] | None = None,
) -> str:
    message: dict[str, object] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls
    body: dict[str, object] = {
        "object": "chat.completion",
        "model": "gpt-4o",
        "choices": [{"message": message, "finish_reason": "stop"}],
    }
    if usage:
        body["usage"] = usage
    return _wrap_body(body)


def test_parse_json_completion() -> None:
    request = _chat_request([{"role": "user", "content": "Hi"}])
    response = _chat_response(
        "Hello", usage={"prompt_tokens": 5, "completion_tokens": 3}
    )
    parsed = parse_call_bodies(request, response)

    assert parsed.stop_reason is StopReason.END_TURN
    assert parsed.model == "gpt-4o"
    assert parsed.usage is not None
    assert parsed.usage.output_tokens == 3


def test_tool_calls_round_trip() -> None:
    request = _chat_request(
        [
            {"role": "user", "content": "search"},
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "search", "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "ok"},
        ],
        tools=[{"type": "function", "function": {"name": "search"}}],
    )
    response = _chat_response("Done")
    parsed = parse_call_bodies(request, response)

    assert any(block.kind is BlockKind.TOOL_USE for block in parsed.messages[1].blocks)


def test_sse_stream_reconstruction() -> None:
    chunks = [
        '{"choices":[{"delta":{"content":"Hel"}}]}',
        '{"choices":[{"delta":{"content":"lo"}}]}',
        '{"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2}}',
    ]
    sse = "\n".join(f"data: {chunk}" for chunk in chunks)
    body = json.dumps(sse)
    parsed = parse_response_body(body)

    assert (
        "".join(block.text for block in parsed.output if block.kind is BlockKind.TEXT)
        == "Hello"
    )
    assert parsed.usage is not None


def test_truncated_body_still_parses_when_valid() -> None:
    request = _chat_request([{"role": "user", "content": "x" * 100}])
    response = _chat_response("y")
    parsed = parse_call_bodies(request, response)
    assert parsed.messages[0].blocks[0].text == "x" * 100


def test_unparseable_body_raises() -> None:
    with pytest.raises(BodyFormatError):
        decode_body_string(json.dumps("not-json-at-all{{{"))


def test_turn_key_stable_for_tool_loop() -> None:
    user_only = _chat_request([{"role": "user", "content": "Find bugs"}])
    with_tools = _chat_request(
        [
            {"role": "user", "content": "Find bugs"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "c1",
                        "type": "function",
                        "function": {"name": "read_file", "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "c1", "content": "file contents"},
        ],
    )
    assert turn_key_for_call(user_only, 42) == turn_key_for_call(with_tools, 42)
