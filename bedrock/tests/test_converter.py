from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from typing import Any, TypedDict, Unpack

import pytest
from bedrock_sync.converter import SkipReason, nebuly_model_name, turn_to_payload
from bedrock_sync.grouping import Grouper, Turn
from bedrock_sync.models import ClaudeCodeMetadata, Invocation, TokenUsage
from bedrock_sync.parser import parse_record
from bedrock_sync.pseudonymize import pseudonymize_user

from tests.records import ACCOUNT, assistant, converse_record, user

_SECRET = "test-secret"
_TOOL_USE = {"toolUse": {"toolUseId": "t1", "name": "search", "input": {"q": "x"}}}
_SSO_ARN = f"arn:aws:sts::{ACCOUNT}:assumed-role/AWSReservedSSO_PowerUser_0123/jane.doe"


class _Extra(TypedDict, total=False):
    stop: str
    system: str
    metadata: dict[str, str] | None
    arn: str


def _inv(
    request_id: str,
    second: int,
    messages: list[dict[str, Any]],
    answer: str | list[dict[str, Any]],
    **kwargs: Unpack[_Extra],
) -> Invocation:
    record = converse_record(
        messages,
        answer,
        request_id=request_id,
        timestamp=f"2026-11-11T10:00:{second:02d}Z",
        **kwargs,
    )
    result = parse_record(record)
    assert isinstance(result, Invocation)
    return result


def _turns(*invocations: Invocation) -> list[Turn]:
    grouper = Grouper()
    for invocation in invocations:
        grouper.add(invocation)
    grouper.flush()
    return grouper.drain_complete() + grouper.open_turns()


def _payload(turn: Turn, *, plain: bool = False) -> dict[str, Any]:
    payload = turn_to_payload(
        turn, anonymize=True, send_plain_end_user=plain, user_hash_secret=_SECRET
    )
    assert isinstance(payload, dict), payload
    return payload


def _tool_calls() -> tuple[Invocation, Invocation]:
    return (
        _inv("a", 0, [user("Find x")], [_TOOL_USE], stop="tool_use"),
        _inv(
            "b",
            5,
            [
                user("Find x"),
                {"role": "assistant", "content": [_TOOL_USE]},
                {
                    "role": "user",
                    "content": [
                        {
                            "toolResult": {
                                "toolUseId": "t1",
                                "content": [{"text": "x is 42"}],
                            }
                        }
                    ],
                },
            ],
            "x is 42.",
        ),
    )


def _tool_turn() -> Turn:
    (turn,) = _turns(*_tool_calls())
    return turn


def test_single_turn_payload() -> None:
    (turn,) = _turns(
        _inv("a", 0, [user("What is Bedrock?")], "A service.", system="Be brief.")
    )

    payload = _payload(turn)

    interaction = payload["interaction"]
    assert interaction["conversation_id"] == "bedrock-a"
    assert interaction["input"] == "What is Bedrock?"
    assert interaction["output"] == "A service."
    assert interaction["time_start"] == "2026-11-11T10:00:00Z"
    assert interaction["time_end"] == "2026-11-11T10:00:01Z"
    assert interaction["hide_content"] is False
    assert interaction["tags"]["model"] == "claude-haiku-5-5"
    assert payload["anonymize"] is True
    assert payload["user_feedback"] == []
    (trace,) = payload["traces"]
    assert trace["messages"] == [{"role": "user", "content": "What is Bedrock?"}]
    assert trace["output"] == "A service."
    assert (trace["input_tokens"], trace["output_tokens"]) == (10, 5)


def test_tool_round_trip_has_llm_and_tool_traces() -> None:
    payload = _payload(_tool_turn())

    assert payload["interaction"]["output"] == "x is 42."
    assert payload["interaction"]["time_end"] == "2026-11-11T10:00:06Z"
    llm, tool = payload["traces"]
    # Both calls of the agent loop in one trace: total tokens, final answer.
    assert llm["messages"] == [{"role": "user", "content": "Find x"}]
    assert llm["output"] == "x is 42."
    assert (llm["input_tokens"], llm["output_tokens"]) == (20, 10)
    assert tool == {"source": "search", "input": '{"q":"x"}', "outputs": ["x is 42"]}
    assert payload["interaction"]["tags"]["tools used"] == "search"


def test_calls_to_different_models_get_one_trace_each() -> None:
    turn = _tool_turn()
    turn.invocations[1] = replace(turn.last, model="anthropic.claude-sonnet-5-5")

    payload = _payload(turn)

    assert [t["model"] for t in payload["traces"][:2]] == [
        "claude-haiku-5-5",
        "claude-sonnet-5-5",
    ]
    tags = payload["interaction"]["tags"]
    assert tags["model"] == "claude-sonnet-5-5"
    assert tags["models"] == "claude-haiku-5-5,claude-sonnet-5-5"


def test_trace_messages_hold_only_the_current_turn() -> None:
    history = [user("Q1"), assistant("A1"), user("Q2")]
    (_, turn) = _turns(_inv("a", 0, [user("Q1")], "A1"), _inv("b", 9, history, "A2"))

    (trace,) = _payload(turn)["traces"]

    assert trace["messages"] == [{"role": "user", "content": "Q2"}]
    assert trace["output"] == "A2"


def test_injected_context_is_removed_from_the_input() -> None:
    text = "<system-reminder>\nToday is Monday.\n</system-reminder>\nFix the bug"
    (turn,) = _turns(_inv("a", 0, [user(text)], "Done."))

    assert _payload(turn)["interaction"]["input"] == "Fix the bug"


def test_input_made_only_of_injected_context_is_skipped() -> None:
    text = "<system-reminder>context</system-reminder>"
    (turn,) = _turns(_inv("a", 0, [user(text)], "Ok."))

    assert turn_to_payload(
        turn, anonymize=False, send_plain_end_user=True, user_hash_secret=""
    ) is (SkipReason.EMPTY_INPUT)


def test_cache_tokens_count_as_input() -> None:
    (turn,) = _turns(_inv("a", 0, [user("Q")], "A"))
    usage = TokenUsage(
        input_tokens=3, output_tokens=5, cache_read_tokens=100, cache_write_tokens=20
    )
    turn.invocations[0] = replace(turn.first, usage=usage)

    payload = _payload(turn)

    assert payload["traces"][0]["input_tokens"] == 123


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"max_tokens": 1}, SkipReason.SINGLE_TOKEN_PROBE),
        (
            {"system": "You write short session titles. Reply with the title."},
            SkipReason.SESSION_TITLE,
        ),
    ],
)
def test_service_calls_are_skipped(change: dict[str, Any], reason: SkipReason) -> None:
    (turn,) = _turns(_inv("a", 0, [user("Hi")], "Hello"))
    turn.invocations[0] = replace(turn.first, **change)

    assert (
        turn_to_payload(
            turn, anonymize=False, send_plain_end_user=True, user_hash_secret=""
        )
        is reason
    )


def test_web_fetch_summary_is_skipped() -> None:
    text = "\nWeb page content:\n---\nHome\n---\nWhat is the main heading?"
    (turn,) = _turns(_inv("a", 0, [user(text)], "Home"))

    assert (
        turn_to_payload(
            turn, anonymize=False, send_plain_end_user=True, user_hash_secret=""
        )
        is SkipReason.WEB_FETCH_SUMMARY
    )


def test_timed_out_tool_turn_is_sent_without_output() -> None:
    (turn,) = _turns(_inv("a", 0, [user("Find x")], [_TOOL_USE], stop="tool_use"))
    turn.timed_out = True

    payload = _payload(turn)

    assert payload["interaction"]["output"] == ""


def _end_user(turn: Turn) -> str:
    return str(_payload(turn, plain=True)["interaction"]["end_user"])


def test_end_user_from_request_metadata() -> None:
    (turn,) = _turns(
        _inv("a", 0, [user("Q")], "A", metadata={"user_id": "u-7", "team": "ops"})
    )

    payload = _payload(turn, plain=True)

    assert payload["interaction"]["end_user"] == "u-7"
    assert payload["interaction"]["tags"]["team"] == "ops"
    assert "user_id" not in payload["interaction"]["tags"]


def test_end_user_from_sso_session() -> None:
    (turn,) = _turns(_inv("a", 0, [user("Q")], "A", arn=_SSO_ARN))

    assert _end_user(turn) == "jane.doe"


def test_end_user_from_claude_code_device_with_a_shared_key() -> None:
    key_arn = f"arn:aws:iam::{ACCOUNT}:user/BedrockAPIKey-ab12"
    (turn,) = _turns(_inv("a", 0, [user("Q")], "A", arn=key_arn))
    turn.invocations[0] = replace(
        turn.first,
        claude_code=ClaudeCodeMetadata(
            session_id="s-1", device_id="device-9", account_uuid=""
        ),
    )

    assert _end_user(turn) == "device-9"
    assert _payload(turn)["interaction"]["tags"]["claude code session-id"] == "s-1"


def test_end_user_falls_back_to_the_caller_and_is_pseudonymized() -> None:
    (turn,) = _turns(_inv("a", 0, [user("Q")], "A"))

    payload = _payload(turn)

    assert payload["interaction"]["end_user"] == pseudonymize_user(
        turn.first.identity_arn, secret=_SECRET
    )
    assert turn.first.identity_arn not in json.dumps(payload)


def test_tool_call_delivered_after_its_continuation() -> None:
    asked, answered = _tool_calls()
    grouper = Grouper()
    grouper.add(answered)
    grouper.advance(answered.timestamp + timedelta(minutes=20))
    grouper.add(asked)
    grouper.advance(asked.timestamp + timedelta(hours=2))

    (turn,) = grouper.drain_complete() + grouper.open_turns()

    assert turn.partial_start
    assert grouper.stats.late_tool_calls == 1
    interaction = _payload(turn)["interaction"]
    # The question comes from the continuation's history.
    assert (interaction["input"], interaction["output"]) == ("Find x", "x is 42.")


def _use(tool_id: str) -> dict[str, Any]:
    return {
        "toolUse": {"toolUseId": tool_id, "name": "search", "input": {"q": tool_id}}
    }


def _result(tool_id: str) -> dict[str, Any]:
    content = [{"text": f"found {tool_id}"}]
    return {
        "role": "user",
        "content": [{"toolResult": {"toolUseId": tool_id, "content": content}}],
    }


def _two_tool_calls() -> tuple[Invocation, Invocation, Invocation]:
    history = [user("Find x")]
    loop_a = [{"role": "assistant", "content": [_use("A")]}, _result("A")]
    loop_b = [{"role": "assistant", "content": [_use("B")]}, _result("B")]
    return (
        _inv("c1", 0, history, [_use("A")], stop="tool_use"),
        _inv("c2", 5, history + loop_a, [_use("B")], stop="tool_use"),
        _inv("c3", 10, history + loop_a + loop_b, "x is 42."),
    )


def test_several_late_calls_do_not_reopen_the_turn() -> None:
    asked_a, asked_b, answered = _two_tool_calls()
    grouper = Grouper()
    grouper.add(answered)
    grouper.advance(answered.timestamp + timedelta(minutes=20))
    grouper.add(asked_b)
    grouper.add(asked_a)
    grouper.advance(answered.timestamp + timedelta(hours=5))

    (turn,) = grouper.drain_complete() + grouper.open_turns()

    assert grouper.stats.late_tool_calls == 2
    assert grouper.stats.timed_out_turns == 0
    assert [i.request_id for i in turn.invocations] == ["c3"]


def test_partial_turn_keeps_the_tool_calls_of_its_history() -> None:
    *_, answered = _two_tool_calls()
    (turn,) = _turns(answered)

    payload = _payload(turn)

    assert turn.partial_start
    assert [t.get("source") for t in payload["traces"][1:]] == ["search", "search"]
    assert [t["outputs"] for t in payload["traces"][1:]] == [
        ["found A"],
        ["found B"],
    ]
    assert payload["interaction"]["tags"]["tools used"] == "search"


def test_tool_ids_are_matched_within_the_same_caller() -> None:
    other = f"arn:aws:sts::{ACCOUNT}:assumed-role/OtherApp/session"
    asked, answered = _tool_calls()
    other_asked = replace(asked, request_id="oa", identity_arn=other)
    other_answered = replace(
        answered,
        request_id="ob",
        identity_arn=other,
        timestamp=answered.timestamp + timedelta(seconds=1),
    )

    turns = _turns(asked, other_asked, answered, other_answered)

    assert sorted([i.request_id for i in t.invocations] for t in turns) == [
        ["a", "b"],
        ["oa", "ob"],
    ]
    assert not any(t.partial_start or t.timed_out for t in turns)


def _numbered(query: str) -> dict[str, Any]:
    """A tool call numbered like OpenAI models on Bedrock do."""
    return {
        "toolUse": {
            "toolUseId": "functions.search:0",
            "name": "search",
            "input": {"q": query},
        }
    }


def _numbered_result(text: str) -> dict[str, Any]:
    content = [{"text": text}]
    return {
        "role": "user",
        "content": [
            {"toolResult": {"toolUseId": "functions.search:0", "content": content}}
        ],
    }


def _numbered_loop(prefix: str, second: int, question: str) -> list[Invocation]:
    asked = {"role": "assistant", "content": [_numbered(question)]}
    return [
        _inv(
            f"{prefix}1",
            second,
            [user(question)],
            [_numbered(question)],
            stop="tool_use",
        ),
        _inv(
            f"{prefix}2",
            second + 2,
            [user(question), asked, _numbered_result(f"result {question}")],
            f"answer {question}",
        ),
    ]


def test_numbered_tool_ids_do_not_mix_conversations() -> None:
    a1, a2 = _numbered_loop("a", 0, "QA")
    b1, b2 = _numbered_loop("b", 1, "QB")

    turns = _turns(a1, b1, a2, b2)

    pairs = sorted(
        (_payload(t)["interaction"]["input"], _payload(t)["interaction"]["output"])
        for t in turns
    )
    assert pairs == [("QA", "answer QA"), ("QB", "answer QB")]
    assert not any(t.partial_start for t in turns)


def test_numbered_tool_id_of_a_new_question_is_not_late() -> None:
    _, partial = _numbered_loop("p", 0, "QA")
    q1, q2 = _numbered_loop("q", 30, "QB")
    grouper = Grouper()
    for invocation in (partial, q1, q2):
        grouper.add(invocation)
    grouper.flush()

    turns = grouper.drain_complete() + grouper.open_turns()

    assert grouper.stats.late_tool_calls == 0
    assert [[i.request_id for i in t.invocations] for t in turns] == [
        ["p2"],
        ["q1", "q2"],
    ]


def test_late_middle_call_joins_the_open_turn() -> None:
    asked_a, asked_b, answered = _two_tool_calls()
    grouper = Grouper()
    grouper.add(asked_a)
    grouper.add(answered)
    grouper.advance(answered.timestamp + timedelta(minutes=20))
    grouper.add(asked_b)
    grouper.advance(answered.timestamp + timedelta(hours=5))

    (turn,) = grouper.drain_complete() + grouper.open_turns()

    assert [i.request_id for i in turn.invocations] == ["c1", "c3"]
    assert not turn.partial_start
    assert not turn.timed_out
    assert grouper.stats.late_tool_calls == 1
    assert grouper.stats.gap_continuations == 1
    payload = _payload(turn)
    assert payload["interaction"]["input"] == "Find x"
    assert [t["outputs"] for t in payload["traces"][1:]] == [["found A"], ["found B"]]


def test_round_trip_within_the_same_second_keeps_its_order() -> None:
    asked, answered = _tool_calls()
    # The continuation's request id sorts first, at the same timestamp.
    asked = replace(asked, request_id="z-asked")
    answered = replace(answered, request_id="a-answered", timestamp=asked.timestamp)

    (turn,) = _turns(answered, asked)

    assert [i.request_id for i in turn.invocations] == ["z-asked", "a-answered"]
    assert not turn.partial_start


def test_reused_tool_id_is_paired_per_round() -> None:
    first = {"role": "assistant", "content": [_numbered("first")]}
    second = {"role": "assistant", "content": [_numbered("second")]}
    history = [user("Find x")]
    loop_1 = [first, _numbered_result("R1")]
    loop_2 = [second, _numbered_result("R2")]
    calls = (
        _inv("c1", 0, history, [_numbered("first")], stop="tool_use"),
        _inv("c2", 2, history + loop_1, [_numbered("second")], stop="tool_use"),
        _inv("c3", 4, history + loop_1 + loop_2, "x is 42."),
    )

    (turn,) = _turns(*calls)
    traces = _payload(turn)["traces"][1:]

    assert [(t["input"], t["outputs"]) for t in traces] == [
        ('{"q":"first"}', ["R1"]),
        ('{"q":"second"}', ["R2"]),
    ]


def test_guarded_text_is_the_question() -> None:
    guarded = {"role": "user", "content": [{"guardContent": {"text": {"text": "Q"}}}]}
    (turn,) = _turns(_inv("a", 0, [guarded], "A"))

    assert _payload(turn)["interaction"]["input"] == "Q"


def test_partial_turn_without_a_question_is_skipped() -> None:
    _, answered = _tool_calls()
    (turn,) = _turns(replace(answered, messages=answered.messages[1:]))

    assert turn.partial_start
    assert (
        turn_to_payload(
            turn, anonymize=False, send_plain_end_user=True, user_hash_secret=""
        )
        is SkipReason.PARTIAL_START
    )


def test_one_token_answer_to_a_real_question_is_kept() -> None:
    question = "Is this review positive? Answer yes or no: great product"
    (turn,) = _turns(_inv("a", 0, [user(question)], "yes"))
    turn.invocations[0] = replace(turn.first, max_tokens=1)

    assert _payload(turn)["interaction"]["output"] == "yes"


def test_compaction_summary_does_not_hide_the_question() -> None:
    message = {
        "role": "user",
        "content": [
            {"text": "This session is being continued from a previous conversation."},
            {"text": "Now fix the bug"},
        ],
    }
    (turn,) = _turns(_inv("a", 0, [message], "Done."))

    assert _payload(turn)["interaction"]["input"] == "Now fix the bug"


def test_model_tag_is_the_model_of_the_answer() -> None:
    turn = _tool_turn()
    haiku = turn.last
    turn.invocations[1] = replace(haiku, model="anthropic.claude-sonnet-5-5")
    turn.invocations.append(replace(haiku, request_id="c"))

    tags = _payload(turn)["interaction"]["tags"]

    assert tags["model"] == "claude-haiku-5-5"
    assert tags["models"] == "claude-haiku-5-5,claude-sonnet-5-5"


@pytest.mark.parametrize(
    ("plain", "expected"),
    [(True, "unknown"), (False, pseudonymize_user("unknown", secret=_SECRET))],
)
def test_unknown_end_user_has_a_placeholder(*, plain: bool, expected: str) -> None:
    (turn,) = _turns(_inv("a", 0, [user("Q")], "A", arn=""))

    assert _payload(turn, plain=plain)["interaction"]["end_user"] == expected


@pytest.mark.parametrize(
    ("model", "expected"),
    [
        ("deepseek.r1-v1:0", "deepseek-r1"),
        ("anthropic.claude-3-sonnet-20240229-v1:0:200k", "claude-3-sonnet"),
        ("amazon.nova-pro-v1:0:300k", "nova-pro"),
        ("anthropic.claude-haiku-4-5-20251001-v1:0", "claude-haiku-4-5"),
        ("openai.gpt-4o-2024-08-06", "gpt-4o"),
        ("anthropic.claude-haiku-5-5", "claude-haiku-5-5"),
        ("openai.gpt-oss-120b-1:0", "gpt-oss-120b"),
        ("openai.gpt-6.1-sol", "gpt-6.1-sol"),
        ("amazon.nova-pro-v1:0", "nova-pro"),
        ("meta.llama3-70b-instruct-v1:0", "llama3-70b-instruct"),
        ("anthropic.claude-v2:1", "claude-2.1"),
        ("anthropic.claude-v2", "claude-2.0"),
        ("anthropic.claude-instant-v1", "claude-instant-1.2"),
    ],
)
def test_nebuly_model_name(model: str, expected: str) -> None:
    assert nebuly_model_name(model) == expected
