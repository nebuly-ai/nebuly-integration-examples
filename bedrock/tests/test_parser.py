from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from bedrock_sync.formats import converse
from bedrock_sync.models import (
    ApiFormat,
    BlockKind,
    Invocation,
    Skipped,
    SkipReason,
    StopReason,
)
from bedrock_sync.parser import normalize_model_id, parse_record

from tests.records import (
    PROFILE_ARN,
    assistant,
    converse_record,
    envelope,
    user,
    with_bodies,
)


def _parse(record: dict[str, Any]) -> Invocation:
    result = parse_record(record, source_key="key")
    assert isinstance(result, Invocation), result
    return result


# --- envelope ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("model_id", "expected"),
    [
        ("global.openai.gpt-6.1-sol", "openai.gpt-6.1-sol"),
        ("eu.anthropic.claude-haiku-5-5", "anthropic.claude-haiku-5-5"),
        (f"{PROFILE_ARN}global.openai.gpt-6.1-sol", "openai.gpt-6.1-sol"),
        ("anthropic.claude-haiku-5-5", "anthropic.claude-haiku-5-5"),
    ],
)
def test_normalize_model_id(model_id: str, expected: str) -> None:
    assert normalize_model_id(model_id) == expected


def test_error_record_is_skipped() -> None:
    record = {**envelope(), "errorCode": "ThrottlingException"}

    result = parse_record(record)

    assert result == Skipped("req-1", SkipReason.ERROR_RECORD, "ThrottlingException")


def test_body_left_in_s3_is_missing() -> None:
    record = {
        **envelope(),
        "input": {"inputBodyS3Path": "s3://bucket/key", "inputTokenCount": 2},
        "output": {"outputBodyJson": {}, "outputTokenCount": 1},
    }

    result = parse_record(record)

    assert isinstance(result, Skipped)
    assert result.reason is SkipReason.MISSING_BODY


def test_unknown_native_format_is_unsupported() -> None:
    record = with_bodies(
        envelope(operation="InvokeModel", model_id="meta.llama4"),
        {"prompt": "hi"},
        {"generation": "hello"},
    )

    result = parse_record(record)

    assert isinstance(result, Skipped)
    assert result.reason is SkipReason.UNSUPPORTED_FORMAT


def test_malformed_converse_body_is_invalid() -> None:
    record = with_bodies(envelope(), {"messages": "oops"}, {"output": {}})

    result = parse_record(record)

    assert isinstance(result, Skipped)
    assert result.reason is SkipReason.INVALID_BODY


def test_unreadable_record_logs_a_warning_with_its_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    record = {**envelope(), "input": {"inputBodyS3Path": "s3://bucket/key"}}

    with caplog.at_level(logging.DEBUG, logger="bedrock_sync.parser"):
        result = parse_record(record, source_key="2026/11/11/10/file.json.gz")

    assert result == Skipped("req-1", SkipReason.MISSING_BODY, "input, output")
    (log,) = caplog.records
    assert log.levelno == logging.WARNING
    assert "req-1" in log.getMessage()
    assert "2026/11/11/10/file.json.gz" in log.getMessage()


def test_failed_call_is_logged_at_debug_only(caplog: pytest.LogCaptureFixture) -> None:
    record = {**envelope(), "errorCode": "ThrottlingException"}

    with caplog.at_level(logging.DEBUG, logger="bedrock_sync.parser"):
        parse_record(record, source_key="key")

    assert [r.levelno for r in caplog.records] == [logging.DEBUG]


def test_adapter_bug_skips_the_record_instead_of_crashing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(*_: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(converse, "parse", broken)

    with caplog.at_level(logging.WARNING, logger="bedrock_sync.parser"):
        result = parse_record(converse_record([user("Q")], "A"), source_key="key")

    assert result == Skipped("req-1", SkipReason.INVALID_BODY, "RuntimeError: boom")
    assert caplog.records[0].levelno == logging.ERROR


def test_envelope_token_counts_win_and_include_cache() -> None:
    record = converse_record([user("hi")], "hello")
    record["input"].update(
        inputTokenCount=2, cacheReadInputTokenCount=0, cacheWriteInputTokenCount=27909
    )

    invocation = _parse(record)

    assert invocation.usage.input_tokens == 2
    assert invocation.usage.total_input_tokens == 27911


# --- Converse ---------------------------------------------------------------


def test_converse_text_turn() -> None:
    record = converse_record(
        [user("Q1"), assistant("A1"), user("Q2")],
        "A2",
        system="Be brief.",
        metadata={"session_id": "s-1", "user_id": "u-1"},
    )

    invocation = _parse(record)

    assert invocation.api_format is ApiFormat.CONVERSE
    assert invocation.model == "anthropic.claude-haiku-5-5"
    assert invocation.system == "Be brief."
    assert [m.role for m in invocation.messages] == ["user", "assistant", "user"]
    assert invocation.messages[-1].text == "Q2"
    assert invocation.output_text == "A2"
    assert invocation.stop_reason is StopReason.END_TURN
    assert invocation.latency_ms == 1000
    assert invocation.max_tokens == 512
    assert invocation.request_metadata == {"session_id": "s-1", "user_id": "u-1"}
    assert invocation.time_end.isoformat() == "2026-11-11T10:00:01+00:00"


def test_nova_on_invoke_model_uses_the_converse_adapter() -> None:
    record = with_bodies(
        envelope(operation="InvokeModel", model_id="eu.amazon.nova-lite-v1:0"),
        {
            "schemaVersion": "messages-v1",
            "system": [{"text": "Be brief."}],
            "messages": [user("Q1")],
            "inferenceConfig": {"max_new_tokens": 300},
        },
        {
            "output": {"message": {"role": "assistant", "content": [{"text": "A1"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 5},
        },
    )

    invocation = _parse(record)

    assert invocation.api_format is ApiFormat.CONVERSE
    assert invocation.model == "amazon.nova-lite-v1:0"
    assert invocation.system == "Be brief."
    assert invocation.output_text == "A1"
    assert invocation.max_tokens == 300


def test_converse_tool_round_trip() -> None:
    tool_use = {
        "toolUse": {"toolUseId": "t1", "name": "get_weather", "input": {"city": "Oslo"}}
    }
    first = _parse(converse_record([user("Weather?")], [tool_use], stop="tool_use"))
    second = _parse(
        converse_record(
            [
                user("Weather?"),
                {"role": "assistant", "content": [tool_use]},
                {
                    "role": "user",
                    "content": [
                        {
                            "toolResult": {
                                "toolUseId": "t1",
                                "content": [{"json": {"temp_c": 4}}],
                            }
                        }
                    ],
                },
            ],
            "It is 4 C.",
        )
    )

    assert first.stop_reason is StopReason.TOOL_USE
    (block,) = first.tool_uses
    assert (block.tool_use_id, block.tool_name, block.tool_input) == (
        "t1",
        "get_weather",
        {"city": "Oslo"},
    )
    assert second.messages[-1].tool_result_ids == {"t1"}
    assert second.messages[-1].blocks[0].text == '{"temp_c":4}'


def test_converse_reasoning_is_not_answer_text() -> None:
    answer: list[dict[str, Any]] = [
        {"reasoningContent": {"redactedContent": "xyz"}},
        {"text": "Paris."},
    ]

    invocation = _parse(converse_record([user("Capital of France?")], answer))

    assert invocation.output[0].kind is BlockKind.REASONING
    assert invocation.output_text == "Paris."


# --- Anthropic native -------------------------------------------------------


def _anthropic_record(output_body: object, **body_extra: object) -> dict[str, Any]:
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "system": [{"type": "text", "text": "You are terse.", "cache_control": {}}],
        "messages": [{"role": "user", "content": "List files"}],
        **body_extra,
    }
    return with_bodies(
        envelope(
            operation="InvokeModel",
            model_id=f"{PROFILE_ARN}global.anthropic.claude-haiku-5-5",
        ),
        body,
        output_body,
        cache_read=None,
        cache_write=None,
    )


def test_anthropic_message_with_claude_code_metadata() -> None:
    user_id = json.dumps({"session_id": "cc-1", "device_id": "dev", "account_uuid": ""})
    output = {
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "thinking", "thinking": "hmm"},
            {"type": "text", "text": "Use ls."},
        ],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 3,
            "output_tokens": 4,
            "cache_read_input_tokens": 100,
            "cache_creation_input_tokens": 7,
        },
    }

    invocation = _parse(_anthropic_record(output, metadata={"user_id": user_id}))

    assert invocation.api_format is ApiFormat.ANTHROPIC
    assert invocation.system == "You are terse."
    assert invocation.messages[0].text == "List files"
    assert invocation.output_text == "Use ls."
    assert invocation.claude_code is not None
    assert invocation.claude_code.session_id == "cc-1"
    assert invocation.claude_code.account_uuid is None
    # inputTokenCount comes from the envelope, cache counters from the body
    assert invocation.usage.total_input_tokens == 10 + 100 + 7


def test_anthropic_stream_with_tool_use() -> None:
    events = [
        {
            "type": "message_start",
            "message": {"usage": {"input_tokens": 9, "output_tokens": 1}},
        },
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Let me "},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "check."},
        },
        {"type": "content_block_stop", "index": 0},
        {
            "type": "content_block_start",
            "index": 1,
            "content_block": {
                "type": "tool_use",
                "id": "tu1",
                "name": "bash",
                "input": {},
            },
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": '{"cmd": '},
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": '"ls"}'},
        },
        {"type": "content_block_stop", "index": 1},
        {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 20},
        },
        {
            "type": "message_stop",
            "amazon-bedrock-invocationMetrics": {"invocationLatency": 917},
        },
    ]
    record = _anthropic_record(events)
    record["operation"] = "InvokeModelWithResponseStream"

    invocation = _parse(record)

    assert invocation.output_text == "Let me check."
    (tool,) = invocation.tool_uses
    assert (tool.tool_use_id, tool.tool_name, tool.tool_input) == (
        "tu1",
        "bash",
        {"cmd": "ls"},
    )
    assert invocation.stop_reason is StopReason.TOOL_USE
    assert invocation.latency_ms == 917
    assert invocation.usage.output_tokens == 5  # envelope wins over the stream


# --- OpenAI native ----------------------------------------------------------


def _openai_record(
    output_body: object, messages: list[dict[str, Any]]
) -> dict[str, Any]:
    return with_bodies(
        envelope(
            operation="InvokeModel",
            model_id=f"{PROFILE_ARN}global.openai.gpt-6.1-sol",
        ),
        {"messages": messages, "max_completion_tokens": 256},
        output_body,
        cache_read=None,
        cache_write=None,
    )


def test_openai_completion_with_tool_history() -> None:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "You are terse."},
        {"role": "user", "content": "Weather in Oslo and Rome?"},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "c1",
                    "type": "function",
                    "function": {"name": "weather", "arguments": '{"city": "Oslo"}'},
                },
                {
                    "id": "c2",
                    "type": "function",
                    "function": {"name": "weather", "arguments": '{"city": "Rome"}'},
                },
            ],
        },
        {"role": "tool", "tool_call_id": "c1", "content": "4 C"},
        {"role": "tool", "tool_call_id": "c2", "content": "18 C"},
    ]
    output = {
        "object": "chat.completion",
        "choices": [
            {
                "finish_reason": "stop",
                "message": {"role": "assistant", "content": "Oslo 4 C, Rome 18 C."},
            }
        ],
        "usage": {
            "prompt_tokens": 50,
            "completion_tokens": 9,
            "prompt_tokens_details": {"cached_tokens": 40},
        },
    }

    invocation = _parse(_openai_record(output, messages))

    assert invocation.api_format is ApiFormat.OPENAI
    assert invocation.system == "You are terse."
    assert [m.role for m in invocation.messages] == ["user", "assistant", "user"]
    assert [b.tool_input for b in invocation.messages[1].blocks] == [
        {"city": "Oslo"},
        {"city": "Rome"},
    ]
    assert invocation.messages[2].tool_result_ids == {"c1", "c2"}
    assert invocation.output_text == "Oslo 4 C, Rome 18 C."
    assert invocation.stop_reason is StopReason.END_TURN
    assert invocation.max_tokens == 256
    assert invocation.usage.cache_read_tokens == 40  # from the body, split out


def test_openai_stream_with_tool_call() -> None:
    def chunk(delta: dict[str, Any], finish: str | None = None) -> dict[str, Any]:
        return {
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }

    chunks = [
        chunk({"role": "assistant", "content": ""}),
        chunk({"content": "Checking"}),
        chunk(
            {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "c1",
                        "function": {"name": "weather", "arguments": '{"ci'},
                    }
                ]
            }
        ),
        chunk(
            {"tool_calls": [{"index": 0, "function": {"arguments": 'ty": "Oslo"}'}}]}
        ),
        {
            **chunk({}, "tool_calls"),
            "amazon-bedrock-invocationMetrics": {"invocationLatency": 2612},
        },
    ]
    record = _openai_record(chunks, [{"role": "user", "content": "Weather?"}])
    record["operation"] = "InvokeModelWithResponseStream"

    invocation = _parse(record)

    assert invocation.output_text == "Checking"
    (tool,) = invocation.tool_uses
    assert (tool.tool_use_id, tool.tool_input) == ("c1", {"city": "Oslo"})
    assert invocation.stop_reason is StopReason.TOOL_USE
    assert invocation.latency_ms == 2612


def test_record_without_request_id_is_invalid() -> None:
    record = {**envelope(), "requestId": ""}

    result = parse_record(record)

    assert isinstance(result, Skipped)
    assert result.reason is SkipReason.INVALID_BODY
