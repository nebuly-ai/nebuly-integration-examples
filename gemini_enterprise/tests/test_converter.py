from __future__ import annotations

from gemini_enterprise_sync.converter import (
    SkipReason,
    session_to_turns,
    turn_to_payload,
)

from .conftest import (
    _session,
    empty_replies_turn,
    grounding_separate_reply_turn,
    inline_data_suggestions_turn,
    multi_turn_session,
    plain_text_turn,
    thought_reply_turn,
)


def test_plain_text_payload() -> None:
    turn = plain_text_turn("hello", "world")
    session = _session(turns=[turn.model_dump(by_alias=True)])
    payload = turn_to_payload(
        session_to_turns(session)[0], session=session, anonymize=False
    )
    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["input"] == "hello"
    assert payload["interaction"]["output"] == "world"
    assert payload["interaction"]["conversation_id"] == "sess_1"
    assert payload["interaction"]["end_user"] == "user_1"


def test_thought_and_inline_data_skipped_in_final_answer() -> None:
    thought_turn = thought_reply_turn()
    session = _session(turns=[thought_turn.model_dump(by_alias=True)])
    payload = turn_to_payload(
        session_to_turns(session)[0], session=session, anonymize=False
    )
    assert not isinstance(payload, SkipReason)
    assert "Thinking step one" not in payload["interaction"]["output"]
    assert payload["interaction"]["output"] == "Here is the answer."

    inline_turn = inline_data_suggestions_turn()
    session = _session(turns=[inline_turn.model_dump(by_alias=True)])
    payload = turn_to_payload(
        session_to_turns(session)[0], session=session, anonymize=False
    )
    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["output"] == "Main answer."
    assert "A1" not in payload["interaction"]["output"]


def test_grounding_builds_retrieval_traces() -> None:
    turn = grounding_separate_reply_turn(ref_count=5)
    session = _session(turns=[turn.model_dump(by_alias=True)])
    payload = turn_to_payload(
        session_to_turns(session)[0], session=session, anonymize=False
    )
    assert not isinstance(payload, SkipReason)
    assert len(payload["traces"]) == 5
    for trace in payload["traces"]:
        assert trace["source"]
        assert trace["input"] == "search nebuly"
        assert len(trace["outputs"]) == 1
        assert trace["outputs"][0].startswith("Reference snippet")


def test_empty_output_skip() -> None:
    turn = empty_replies_turn()
    session = _session(turns=[turn.model_dump(by_alias=True)])
    result = turn_to_payload(
        session_to_turns(session)[0], session=session, anonymize=False
    )
    assert result is SkipReason.EMPTY_OUTPUT


def test_multi_turn_session_shares_conversation_id() -> None:
    session = multi_turn_session()
    turns = session_to_turns(session)
    assert len(turns) == 2
    payloads = [
        turn_to_payload(turn, session=session, anonymize=False) for turn in turns
    ]
    assert all(not isinstance(p, SkipReason) for p in payloads)
    assert {p["interaction"]["conversation_id"] for p in payloads} == {"sess_multi"}  # type: ignore[index]


def test_timing_from_reply_create_times() -> None:
    turn = thought_reply_turn()
    session = _session(turns=[turn.model_dump(by_alias=True)])
    payload = turn_to_payload(
        session_to_turns(session)[0], session=session, anonymize=False
    )
    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["time_start"] == "2026-06-29T12:50:02Z"
    assert payload["interaction"]["time_end"] == "2026-06-29T12:50:03Z"
