from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict, Unpack

from bedrock_sync.grouping import ConversationSource, Grouper, Turn
from bedrock_sync.models import ClaudeCodeMetadata, Invocation
from bedrock_sync.parser import parse_record

from tests.records import assistant, converse_record, user

_TOOL_USE = {"toolUse": {"toolUseId": "t1", "name": "search", "input": {"q": "x"}}}


class _Extra(TypedDict, total=False):
    stop: str
    metadata: dict[str, str] | None
    arn: str


def _inv(
    request_id: str,
    minute: int,
    messages: list[dict[str, Any]],
    answer: str | list[dict[str, Any]],
    **kwargs: Unpack[_Extra],
) -> Invocation:
    record = converse_record(
        messages,
        answer,
        request_id=request_id,
        timestamp=f"2026-11-11T10:{minute:02d}:00Z",
        **kwargs,
    )
    result = parse_record(record)
    assert isinstance(result, Invocation)
    return result


def _tool_result(tool_id: str, text: str) -> dict[str, Any]:
    return {
        "role": "user",
        "content": [
            {"toolResult": {"toolUseId": tool_id, "content": [{"text": text}]}}
        ],
    }


def _group(*invocations: Invocation, grouper: Grouper | None = None) -> list[Turn]:
    grouper = grouper or Grouper()
    for invocation in invocations:
        grouper.add(invocation)
    grouper.flush()
    return grouper.drain_complete()


def _conversations(turns: list[Turn]) -> list[list[str]]:
    by_id: dict[str, list[str]] = {}
    for turn in turns:
        by_id.setdefault(turn.conversation_id, []).extend(
            i.request_id for i in turn.invocations
        )
    return sorted(by_id.values())


def test_request_metadata_session_wins() -> None:
    turns = _group(
        _inv("a", 0, [user("Q1")], "A1", metadata={"session_id": "s-1"}),
        _inv("b", 1, [user("Other")], "B1", metadata={"session_id": "s-1"}),
    )

    assert {t.conversation_id for t in turns} == {"s-1"}
    assert turns[0].conversation_source is ConversationSource.REQUEST_METADATA


def test_hash_chain_links_follow_up_turns() -> None:
    turns = _group(
        _inv("a", 0, [user("Q1")], "A1"),
        _inv("b", 1, [user("Q1"), assistant("A1"), user("Q2")], "A2"),
        _inv(
            "c",
            2,
            [user("Q1"), assistant("A1"), user("Q2"), assistant("A2"), user("Q3")],
            "A3",
        ),
    )

    assert _conversations(turns) == [["a", "b", "c"]]
    assert turns[0].conversation_id == "bedrock-a"
    assert [t.conversation_source for t in turns] == [
        ConversationSource.NEW,
        ConversationSource.HASH_CHAIN,
        ConversationSource.HASH_CHAIN,
    ]


def test_same_first_question_stays_in_separate_conversations() -> None:
    """Regression: the prototype merged two conversations sharing a caller and
    a first question, because the chain ignored the answers."""
    turns = _group(
        _inv("a1", 0, [user("Plan a trip")], "Plan A"),
        _inv("b1", 1, [user("Plan a trip")], "Plan B"),
        _inv(
            "a2", 2, [user("Plan a trip"), assistant("Plan A"), user("More")], "A more"
        ),
        _inv(
            "b2", 3, [user("Plan a trip"), assistant("Plan B"), user("More")], "B more"
        ),
    )

    assert _conversations(turns) == [["a1", "a2"], ["b1", "b2"]]


def test_different_callers_never_chain() -> None:
    other_arn = "arn:aws:sts::123456789012:assumed-role/other/session"
    turns = _group(
        _inv("a", 0, [user("Q1")], "A1"),
        _inv("b", 1, [user("Q1"), assistant("A1"), user("Q2")], "A2", arn=other_arn),
    )

    assert _conversations(turns) == [["a"], ["b"]]


def test_regenerated_answer_joins_the_conversation() -> None:
    history = [user("Q1"), assistant("A1"), user("Q2")]
    turns = _group(
        _inv("a", 0, [user("Q1")], "A1"),
        _inv("b", 1, history, "A2"),
        _inv("c", 2, history, "A2 again"),
    )

    assert _conversations(turns) == [["a", "b", "c"]]


def test_whitespace_and_reasoning_do_not_break_the_chain() -> None:
    answer: list[dict[str, Any]] = [
        {"reasoningContent": {"redactedContent": "xyz"}},
        {"text": "A1\n"},
    ]
    turns = _group(
        _inv("a", 0, [user("Q1")], answer),
        _inv("b", 1, [user("Q1"), assistant("A1"), user("Q2")], "A2"),
    )

    assert _conversations(turns) == [["a", "b"]]


def test_chain_expires_after_ttl() -> None:
    grouper = Grouper(chain_ttl=timedelta(minutes=5))

    turns = _group(
        _inv("a", 0, [user("Q1")], "A1"),
        _inv("b", 10, [user("Q1"), assistant("A1"), user("Q2")], "A2"),
        grouper=grouper,
    )

    assert _conversations(turns) == [["a"], ["b"]]


def test_tool_round_trip_is_one_turn() -> None:
    turns = _group(
        _inv("a", 0, [user("Find x")], [_TOOL_USE], stop="tool_use"),
        _inv(
            "b",
            1,
            [
                user("Find x"),
                {"role": "assistant", "content": [_TOOL_USE]},
                _tool_result("t1", "found"),
            ],
            "Here is x.",
        ),
    )

    (turn,) = turns
    assert [i.request_id for i in turn.invocations] == ["a", "b"]
    assert turn.complete


def test_tool_turn_waits_for_its_result() -> None:
    grouper = Grouper()
    grouper.add(_inv("a", 0, [user("Find x")], [_TOOL_USE], stop="tool_use"))
    grouper.flush()

    assert grouper.drain_complete() == []
    (open_turn,) = grouper.open_turns()
    assert open_turn.pending_tool_ids == {"t1"}


def test_interleaved_calls_in_one_session_do_not_mix() -> None:
    """Claude Code runs side calls (titles, sub-agents) in the same session."""
    session = ClaudeCodeMetadata(session_id="cc-1", device_id=None, account_uuid=None)
    calls = [
        _inv("a", 0, [user("Fix bug")], [_TOOL_USE], stop="tool_use"),
        _inv("b", 1, [user("Write a title")], "Bug fix"),
        _inv(
            "c",
            2,
            [
                user("Fix bug"),
                {"role": "assistant", "content": [_TOOL_USE]},
                _tool_result("t1", "ok"),
            ],
            "Fixed.",
        ),
    ]

    turns = _group(*(replace(c, claude_code=session) for c in calls))

    assert {t.conversation_id for t in turns} == {"cc-1"}
    assert turns[0].conversation_source is ConversationSource.CLAUDE_CODE
    assert sorted([i.request_id for i in t.invocations] for t in turns) == [
        ["a", "c"],
        ["b"],
    ]


def test_tool_result_without_known_call_is_partial() -> None:
    turns = _group(
        _inv(
            "b",
            1,
            [
                user("Find x"),
                {"role": "assistant", "content": [_TOOL_USE]},
                _tool_result("t1", "found"),
            ],
            "Here is x.",
        )
    )

    (turn,) = turns
    assert turn.partial_start


def _tool_round_trip() -> tuple[Invocation, Invocation]:
    return (
        _inv("a", 0, [user("Find x")], [_TOOL_USE], stop="tool_use"),
        _inv(
            "b",
            1,
            [
                user("Find x"),
                {"role": "assistant", "content": [_TOOL_USE]},
                _tool_result("t1", "found"),
            ],
            "Here is x.",
        ),
    )


def _at(minute: int) -> datetime:
    return datetime(2026, 11, 11, 10, minute, tzinfo=UTC)


def test_calls_delivered_out_of_order_are_regrouped() -> None:
    """Real logs: the continuation's file was delivered before the caller's."""
    call, continuation = _tool_round_trip()

    turns = _group(continuation, call)

    (turn,) = turns
    assert [i.request_id for i in turn.invocations] == ["a", "b"]
    assert not turn.partial_start


def test_advance_releases_only_calls_older_than_the_window() -> None:
    grouper = Grouper(reorder_window=timedelta(minutes=15))
    grouper.add(_inv("a", 0, [user("Q1")], "A1"))
    grouper.add(_inv("b", 20, [user("Q2")], "A2"))

    grouper.advance(_at(30))

    assert [t.first.request_id for t in grouper.drain_complete()] == ["a"]
    assert [i.request_id for i in grouper.buffered()] == ["b"]


def test_open_turn_times_out() -> None:
    call, _ = _tool_round_trip()
    grouper = Grouper(
        reorder_window=timedelta(minutes=5),
        open_turn_timeout=timedelta(minutes=30),
    )
    grouper.add(call)

    grouper.advance(_at(30))
    assert grouper.drain_complete() == []
    assert len(grouper.open_turns()) == 1

    grouper.advance(_at(40))
    (turn,) = grouper.drain_complete()
    assert turn.timed_out
    assert grouper.open_turns() == []


def test_duplicate_request_is_dropped() -> None:
    grouper = Grouper()
    call = _inv("a", 0, [user("Q1")], "A1")

    turns = _group(call, call, grouper=grouper)

    assert len(turns) == 1
    assert grouper.stats.duplicates == 1


def test_turn_spans_from_first_call_to_end_of_last() -> None:
    (turn,) = _group(*_tool_round_trip())

    # Each call starts at its minute and the record says it took 1 s.
    assert turn.time_start == _at(0)
    assert turn.time_end == _at(1) + timedelta(seconds=1)


def test_identical_conversations_are_counted_as_ambiguous() -> None:
    """Same caller, same question, same answer: content cannot tell them
    apart, so the follow-up joins the later one and the stat records it."""
    grouper = Grouper()

    turns = _group(
        _inv("a1", 0, [user("Hi")], "Hello"),
        _inv("b1", 1, [user("Hi")], "Hello"),
        _inv("x2", 2, [user("Hi"), assistant("Hello"), user("Next")], "More"),
        grouper=grouper,
    )

    assert _conversations(turns) == [["a1"], ["b1", "x2"]]
    assert grouper.stats.ambiguous_links == 1


def test_chain_links_a_conversation_resumed_days_later() -> None:
    first = _inv("a", 0, [user("Q1")], "A1")
    later = replace(
        _inv("b", 0, [user("Q1"), assistant("A1"), user("Q2")], "A2"),
        timestamp=first.timestamp + timedelta(days=10),
    )

    assert _conversations(_group(first, later)) == [["a", "b"]]


def test_tips_stay_bounded_per_conversation() -> None:
    grouper = Grouper()
    history: list[dict[str, Any]] = []
    for turn in range(20):
        history = [*history, user(f"Q{turn}")]
        grouper.add(_inv(f"r{turn}", turn, history, f"A{turn}"))
        history = [*history, assistant(f"A{turn}")]
    grouper.flush()

    turns = grouper.drain_complete()

    assert len({t.conversation_id for t in turns}) == 1
    assert len(grouper._tips) <= 2 * 8


def _numbered_call(query: str) -> dict[str, Any]:
    """A tool call numbered by the model, not given a unique id."""
    return {
        "toolUse": {
            "toolUseId": "functions.search:0",
            "name": "search",
            "input": {"q": query},
        }
    }


def _same_question_by_two_users() -> list[Invocation]:
    def asked(query: str) -> dict[str, Any]:
        return {"role": "assistant", "content": [_numbered_call(query)]}

    return [
        _inv("a1", 0, [user("Q")], [_numbered_call("x")], stop="tool_use"),
        _inv("b1", 1, [user("Q")], [_numbered_call("y")], stop="tool_use"),
        _inv(
            "a2",
            2,
            [user("Q"), asked("x"), _tool_result("functions.search:0", "RA")],
            "A",
        ),
        _inv(
            "b2",
            3,
            [user("Q"), asked("y"), _tool_result("functions.search:0", "RB")],
            "B",
        ),
    ]


def test_turns_waiting_for_the_same_numbered_tool_id_stay_apart() -> None:
    turns = _group(*_same_question_by_two_users())

    assert sorted([i.request_id for i in t.invocations] for t in turns) == [
        ["a1", "a2"],
        ["b1", "b2"],
    ]
    assert not any(t.partial_start for t in turns)
