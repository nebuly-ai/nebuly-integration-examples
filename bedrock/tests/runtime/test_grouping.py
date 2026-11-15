from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

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


def grouper(
    closed: dict[str, str] | None = None, conversations: dict[str, str] | None = None
) -> Grouper:
    closed = closed or {}
    conversations = conversations or {}
    return Grouper(
        idle=IDLE,
        delivery_margin=MARGIN,
        closed_owner=closed.get,
        conversation_owner=conversations.get,
    )


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


def test_reused_interaction_id_in_another_conversation_is_dropped() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet()))

    result = g.add(call("r2", 5, triplet(conversation="c-2")))

    assert result is AddResult.CONFLICT
    assert g.stats.interaction_id_conflicts == 1
    g.advance(at(40))
    [interaction] = g.drain_closed()
    assert interaction.triplet.conversation_id == "c-1"
    assert len(interaction.invocations) == 1


def test_another_user_in_the_conversation_wins_over_the_id_check() -> None:
    """Conversation = one user comes first: a newer call of another user with
    the same ids is a conversation conflict."""
    g = grouper()
    g.add(call("r1", 0, triplet()))

    assert g.add(call("r2", 5, triplet(user_id="u-2"))) is (
        AddResult.CONVERSATION_CONFLICT
    )
    assert g.stats.interaction_id_conflicts == 0


def test_reused_interaction_id_of_an_earlier_run_is_a_conflict() -> None:
    owner = Triplet("i-1", "c-1", "u-1").owner
    g = grouper({"i-1": owner})

    assert g.add(call("r1", 0, triplet(conversation="c-2"))) is AddResult.CONFLICT
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
    assert one_run.add(call("r2", late, triplet())) is AddResult.CLOSED

    two_runs = grouper()
    two_runs.add(call("r1", 0, triplet()))
    two_runs.advance(at(36))
    two_runs.drain_closed()
    assert two_runs.add(call("r2", late, triplet())) is AddResult.CLOSED
    assert two_runs.stats.calls_for_closed_interaction == 1


def test_next_interaction_closes_the_previous_after_the_delivery_margin() -> None:
    g = grouper()
    g.add(call("a1", 0, triplet("i-a")))
    g.add(call("b1", 60, triplet("i-b")))

    g.advance(at(1) + MARGIN - timedelta(seconds=1))
    assert g.drain_closed() == []

    g.advance(at(1) + MARGIN)
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
    g.advance(at(1) + MARGIN)

    [closed] = g.drain_closed()
    assert [i.request_id for i in closed.invocations] == ["a1", "a2"]


def test_a_call_still_running_after_the_next_turn_does_not_delay_closure() -> None:
    """A background call of turn A starts after turn B: A still closes after
    B's start plus the margin, whether or not that call was read first."""
    g = grouper()
    g.add(call("a1", 0, triplet("i-a")))
    g.add(call("b1", 60, triplet("i-b")))
    g.add(call("a2", 70, triplet("i-a"), answer="final A"))
    g.advance(at(1) + MARGIN)

    [closed] = g.drain_closed()
    assert closed.triplet.interaction_id == "i-a"
    assert closed.close_reason is CloseReason.NEXT_INTERACTION
    assert [i.request_id for i in closed.invocations] == ["a1", "a2"]


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


def test_conversation_of_another_user_is_dropped() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet("i-1", "c-1", "u-1")))

    result = g.add(call("r2", 60, triplet("i-2", "c-1", "u-2")))

    assert result is AddResult.CONVERSATION_CONFLICT
    assert g.stats.conversation_user_conflicts == 1
    assert g.open_count == 1
    assert g.add(call("r3", 90, triplet("i-3", "c-1", "u-1"))) is AddResult.ACCEPTED


def test_conversation_owner_of_an_earlier_run_is_respected() -> None:
    owner = Triplet("i-0", "c-1", "u-1")
    g = grouper(conversations={owner.conversation_key: owner.owner})

    assert g.add(call("r1", 0, triplet("i-1", "c-1", "u-2"))) is (
        AddResult.CONVERSATION_CONFLICT
    )
    assert g.add(call("r2", 0, triplet("i-2", "c-1", "u-1"))) is AddResult.ACCEPTED
    assert g.conversation_owners() == {}  # nothing closed in this run


def test_older_call_of_another_user_read_later_takes_the_conversation() -> None:
    """S3 delivered the wrong (newer) calls first: the oldest call decides."""
    g = grouper()
    g.add(call("bob", 2, triplet("i-bob", "c-1", "u-bob")))
    g.add(call("bob2", 3, triplet("i-bob2", "c-1", "u-bob")))

    assert g.add(call("alice", 0, triplet("i-a", "c-1", "u-alice"))) is (
        AddResult.ACCEPTED
    )
    assert g.add(call("alice2", 60, triplet("i-a2", "c-1", "u-alice"))) is (
        AddResult.ACCEPTED
    )
    assert (g.stats.conversation_user_conflicts, g.stats.accepted) == (2, 2)
    g.advance(at(40))
    assert [i.triplet.interaction_id for i in g.drain_closed()] == ["i-a", "i-a2"]


def test_take_over_wins_over_a_reused_interaction_id() -> None:
    """The conversation rule comes first: the older user takes the
    conversation even when the newer one used the same interaction_id."""
    g = grouper()
    g.add(call("bob", 2, triplet("i-1", "c-1", "u-bob")))

    assert g.add(call("alice", 0, triplet("i-1", "c-1", "u-alice"))) is (
        AddResult.ACCEPTED
    )
    assert g.stats.interaction_id_conflicts == 0


def test_take_over_does_not_uncount_calls_restored_from_an_earlier_run() -> None:
    g = grouper()
    g.restore([call("bob", 2, triplet("i-bob", "c-1", "u-bob"))])

    g.add(call("alice", 0, triplet("i-a", "c-1", "u-alice")))

    assert (g.stats.accepted, g.stats.conversation_user_conflicts) == (1, 1)


def test_owner_is_final_once_an_interaction_closed() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet("i-1", "c-1", "u-1")))
    g.advance(at(40))
    [closed] = g.drain_closed()

    older = call("r0", -60, triplet("i-0", "c-1", "u-2"))
    assert g.add(older) is AddResult.CONVERSATION_CONFLICT
    assert g.conversation_owners() == {
        closed.triplet.conversation_key: closed.triplet.owner
    }


def test_same_second_keeps_the_user_read_first() -> None:
    g = grouper()
    g.add(call("r1", 0, triplet("i-1", "c-1", "u-1")))

    assert g.add(call("r2", 0, triplet("i-2", "c-1", "u-2"))) is (
        AddResult.CONVERSATION_CONFLICT
    )
