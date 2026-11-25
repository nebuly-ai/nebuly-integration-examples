from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from bedrock_sync.runtime.grouping import (
    AddResult,
    CloseReason,
    Grouper,
    Triplet,
    is_subagent,
    triplet_of,
)
from bedrock_sync.runtime.models import Invocation
from bedrock_sync.runtime.parser import parse_record

from tests.runtime.records import converse_record, user

if TYPE_CHECKING:
    import pytest

START = datetime(2026, 11, 11, 10, tzinfo=UTC)
IDLE = timedelta(minutes=30)
MARGIN = timedelta(minutes=5)


def triplet(
    interaction: str = "i-1", conversation: str = "c-1", user_id: str = "u-1"
) -> dict[str, str]:
    return {
        "interaction_id": interaction,
        "conversation_id": conversation,
        "user_id": user_id,
    }


def call(
    request_id: str,
    seconds: int,
    metadata: dict[str, str] | None,
    *,
    question: str = "Q",
    answer: str = "A",
    messages: list[dict[str, Any]] | None = None,
) -> Invocation:
    at = START + timedelta(seconds=seconds)
    parsed = parse_record(
        converse_record(
            messages or [user(question)],
            answer,
            request_id=request_id,
            timestamp=at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            metadata=metadata,
        ),
        source_key=f"key-{request_id}",
    )
    assert isinstance(parsed, Invocation)
    return parsed


def grouper(closed: set[str] | None = None) -> Grouper:
    closed = closed or set()
    return Grouper(idle=IDLE, delivery_margin=MARGIN, is_closed=closed.__contains__)


def at(minutes: float) -> datetime:
    return START + timedelta(minutes=minutes)


def test_triplet_requires_three_nonblank_keys() -> None:
    assert triplet_of(call("r", 0, triplet())) == Triplet("i-1", "c-1", "u-1")
    assert triplet_of(call("r", 0, None)) is None
    assert triplet_of(call("r", 0, {**triplet(), "user_id": "  "})) is None
    assert triplet_of(call("r", 0, {"session_id": "s", "user_id": "u"})) is None


def test_subagent_marker_is_only_the_exact_value() -> None:
    assert is_subagent(call("r", 0, {**triplet(), "agent_role": "subagent"}))
    assert not is_subagent(call("r", 0, {**triplet(), "agent_role": "main"}))
    assert not is_subagent(call("r", 0, triplet()))


def test_calls_with_the_same_interaction_id_form_one_interaction() -> None:
    g = grouper()
    g.add(call("r2", 10, triplet(), answer="final"))
    g.add(call("r1", 0, triplet()))
    g.advance(at(40))

    [interaction] = g.drain_closed()
    assert [i.request_id for i in interaction.invocations] == ["r1", "r2"]
    assert interaction.close_reason is CloseReason.IDLE
    assert g.stats.accepted == 2


def test_call_without_triplet_is_skipped_and_counted() -> None:
    g = grouper()

    assert g.add(call("r1", 0, None)) is AddResult.MISSING_TRIPLET
    assert g.stats.missing_triplet == 1
    assert g.open_count == 0


def test_duplicate_request_id_is_dropped() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet()))

    assert g.add(call("r1", 0, triplet())) is AddResult.DUPLICATE
    assert g.stats.duplicates == 1


def test_same_interaction_id_in_two_conversations_is_two_turns() -> None:
    """Turns may be numbered per conversation: the key is the pair."""
    g = grouper()
    g.add(call("r1", 0, triplet(), question="first chat"))

    result = g.add(call("r2", 5, triplet(conversation="c-2"), question="second chat"))

    assert result is AddResult.ACCEPTED
    g.advance(at(40))
    closed = g.drain_closed()
    assert [i.triplet.conversation_id for i in closed] == ["c-1", "c-2"]
    assert [len(i.invocations) for i in closed] == [1, 1]


def test_turn_closed_in_an_earlier_run_is_closed_only_in_its_conversation() -> None:
    g = grouper({Triplet("i-1", "c-1", "u-1").turn_key})

    assert g.add(call("r1", 0, triplet(conversation="c-2"))) is AddResult.ACCEPTED
    assert g.add(call("r2", 0, triplet())) is AddResult.CLOSED
    assert g.stats.calls_for_closed_interaction == 1


def test_idle_closes_after_thirty_minutes_plus_the_delivery_margin() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet()))  # ends after 1 second
    g.advance(at(34))
    assert g.drain_closed() == []

    g.advance(at(35.1))
    [closed] = g.drain_closed()
    assert closed.close_reason is CloseReason.IDLE
    assert (g.stats.closed_idle, g.stats.closed_next_interaction) == (1, 0)


def test_call_within_the_idle_time_joins_even_if_read_late() -> None:
    """Delivered after the idle time but within the margin: still joins."""
    g = grouper()
    g.add(call("r1", 0, triplet()))
    g.advance(at(32))
    assert g.add(call("r2", 60 * 29, triplet())) is AddResult.ACCEPTED


def test_call_after_the_idle_time_is_dropped_whenever_it_is_read() -> None:
    """The same late call is dropped in one run or across two: the result
    depends on when the call ran, not on when the connector runs."""
    late = 60 * 31

    one_run = grouper()
    one_run.add(call("r1", 0, triplet()))
    one_run.add(call("r2", late, triplet()))
    one_run.advance(at(36))
    [closed] = one_run.drain_closed()
    assert [item.request_id for item in closed.invocations] == ["r1"]
    assert one_run.stats.calls_for_closed_interaction == 1
    assert one_run.stats.accepted == 1

    two_runs = grouper()
    two_runs.add(call("r1", 0, triplet()))
    two_runs.advance(at(36))
    two_runs.drain_closed()
    assert two_runs.add(call("r2", late, triplet())) is AddResult.CLOSED
    assert two_runs.stats.calls_for_closed_interaction == 1


def test_out_of_order_call_can_bridge_an_apparent_idle_gap() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet()))
    g.add(call("r3", 60 * 31, triplet()))
    g.advance(at(32))
    assert g.drain_closed() == []

    g.add(call("r2", 60 * 29, triplet()))
    g.advance(at(33))
    assert g.drain_closed() == []
    g.advance(at(67))
    [closed] = g.drain_closed()
    assert [item.request_id for item in closed.invocations] == ["r1", "r2", "r3"]
    assert g.stats.calls_for_closed_interaction == 0


def test_next_interaction_closes_the_previous_after_the_delivery_margin() -> None:
    g = grouper()
    g.add(call("a1", 0, triplet("i-a")))
    g.add(call("b1", 60, triplet("i-b")))

    g.advance(at(1) + MARGIN - timedelta(seconds=1))
    assert g.drain_closed() == []

    g.advance(START + timedelta(seconds=80) + MARGIN)
    [closed] = g.drain_closed()
    assert closed.triplet.interaction_id == "i-a"
    assert closed.close_reason is CloseReason.NEXT_INTERACTION
    assert g.open_count == 1


def test_late_last_call_of_previous_turn_within_the_margin_still_joins() -> None:
    """S3 delivered the next turn's file before the previous turn's last call."""
    g = grouper()
    g.add(call("a1", 0, triplet("i-a")))
    g.add(call("b1", 60, triplet("i-b")))
    g.advance(at(2))
    g.add(call("a2", 50, triplet("i-a"), answer="final A"))
    g.advance(START + timedelta(seconds=80) + MARGIN)

    [closed] = g.drain_closed()
    assert [i.request_id for i in closed.invocations] == ["a1", "a2"]


def test_turn_still_working_after_the_next_message_stays_open() -> None:
    """The user writes again while the agent still works on turn A: A closes
    only once it is quiet for the margin, with its final answer."""
    g = grouper()
    g.add(call("a1", 0, triplet("i-a")))
    g.add(call("b1", 120, triplet("i-b")))
    g.add(call("a2", 420, triplet("i-a"), answer="final A"))
    g.advance(at(2) + MARGIN)
    assert g.drain_closed() == []

    g.advance(at(8) + MARGIN)
    [closed] = g.drain_closed()
    assert closed.triplet.interaction_id == "i-a"
    assert closed.close_reason is CloseReason.NEXT_INTERACTION
    assert [i.request_id for i in closed.invocations] == ["a1", "a2"]
    assert closed.final is not None
    assert closed.final.request_id == "a2"


def test_finished_turn_closes_at_the_next_turn_plus_the_margin() -> None:
    g = grouper()
    g.add(call("a1", 0, triplet("i-a")))
    g.add(call("b1", 120, triplet("i-b")))
    g.advance(at(2) + MARGIN)

    [closed] = g.drain_closed()
    assert closed.triplet.interaction_id == "i-a"
    assert closed.close_reason is CloseReason.NEXT_INTERACTION


def test_other_conversations_do_not_close_an_interaction() -> None:
    g = grouper()
    g.add(call("a1", 0, triplet("i-a", "c-1")))
    g.add(call("b1", 60, triplet("i-b", "c-2")))
    g.advance(at(20))

    assert g.drain_closed() == []


def test_call_after_closure_is_dropped() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet()))
    g.advance(at(40))
    g.drain_closed()

    assert g.add(call("r2", 60 * 41, triplet())) is AddResult.CLOSED


def test_interleaved_turns_drop_the_calls_after_the_next_turn_closed_them() -> None:
    g = grouper()
    g.add(call("a1", 0, triplet("i-a")))
    g.add(call("b1", 30, triplet("i-b")))
    g.advance(at(10))
    g.drain_closed()

    assert g.add(call("a2", 60 * 11, triplet("i-a"))) is AddResult.CLOSED


def test_final_call_skips_subagents() -> None:
    g = grouper()
    g.add(call("p1", 0, triplet(), answer="delegating"))
    g.add(call("c1", 1, {**triplet(), "agent_role": "subagent"}, answer="child"))
    g.add(call("p2", 20, triplet(), answer="final"))
    g.add(call("c2", 30, {**triplet(), "agent_role": "subagent"}, answer="late"))
    g.advance(at(40))

    [interaction] = g.drain_closed()
    assert interaction.final is not None
    assert interaction.final.request_id == "p2"
    assert len(interaction.invocations) == 4


def test_open_refs_and_restore_round_trip() -> None:
    g = grouper()
    first = call("r1", 0, triplet())
    g.add(first)
    assert [(r.request_id, r.source_key) for r in g.open_refs()] == [("r1", "key-r1")]

    restored = grouper()
    restored.restore([first])
    restored.add(call("r2", 10, triplet()))
    restored.advance(at(40))

    [interaction] = restored.drain_closed()
    assert [i.request_id for i in interaction.invocations] == ["r1", "r2"]
    assert restored.stats.accepted == 1


def test_another_user_in_the_conversation_has_their_own_turns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Nebuly keys a conversation by id and end user: so does the grouper."""
    g = grouper()
    g.add(call("a1", 0, triplet("i-1", "c-1", "u-alice"), question="alice"))
    with caplog.at_level(logging.WARNING):
        assert g.add(call("b1", 60, triplet("i-2", "c-1", "u-bob"))) is (
            AddResult.ACCEPTED
        )
        g.add(call("b2", 70, triplet("i-3", "c-1", "u-bob")))
        g.add(call("a2", 80, triplet("i-4", "c-1", "u-alice")))
    assert g.stats.conversations_with_several_users == 1
    assert caplog.text.count("several user_id values") == 1
    # Bob's next turn does not close Alice's: only her own next turn does.
    g.advance(START + timedelta(seconds=80) + MARGIN)
    closed = g.drain_closed()
    assert [(i.triplet.user_id, i.triplet.interaction_id) for i in closed] == [
        ("u-alice", "i-1"),
        ("u-bob", "i-2"),
    ]


def test_same_interaction_id_of_two_users_is_one_interaction_per_user(
    caplog: pytest.LogCaptureFixture,
) -> None:
    g = grouper()
    g.add(call("a1", 0, triplet("i-1", "c-1", "u-alice")))
    with caplog.at_level(logging.WARNING):
        g.add(call("b1", 5, triplet("i-1", "c-1", "u-bob")))
    assert g.stats.interactions_with_several_users == 1
    assert "one interaction per user" in caplog.text
    g.advance(at(40))
    closed = g.drain_closed()
    assert sorted(i.triplet.user_id for i in closed) == ["u-alice", "u-bob"]
    assert closed[0].triplet.turn_key != closed[1].triplet.turn_key
