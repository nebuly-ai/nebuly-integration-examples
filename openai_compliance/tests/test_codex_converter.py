from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from openai_compliance_sync.codex_converter import (
    CodexTurn,
    assemble_codex_turns,
    codex_record_to_payload,
    codex_turn_to_payload,
)
from openai_compliance_sync.entra import EntraDirectory, EntraUser
from openai_compliance_sync.models import (
    Actor,
    CodexAgentRef,
    CodexEventDetails,
    CodexLogRecord,
    CodexTurnClassification,
    CodexTurnPayload,
    CodexTurnRecord,
    TokenUsage,
)

_ENTRA_USER = EntraUser.model_validate(
    {"mail": "dev@company.com", "department": "Platform"}
)


def _ts(hour: int = 9, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _log(
    event_id: str,
    event_type: str,
    *,
    session_id: str = "sess-1",
    turn_id: str | None = "turn-1",
    timestamp: datetime | None = None,
    actor: Actor | None = None,
    agent_name: str | None = None,
    **details: object,
) -> CodexLogRecord:
    base: dict[str, Any] = {
        "session_id": session_id,
        "turn_id": turn_id,
    }
    for key, value in details.items():
        base[str(key)] = value
    return CodexLogRecord(
        event_id=event_id,
        event_type=event_type,
        timestamp=timestamp or _ts(),
        actor=actor,
        agent=CodexAgentRef(name=agent_name) if agent_name else None,
        event_details=CodexEventDetails.model_validate(base),
    )


def _assemble(
    events: list[CodexLogRecord], *, now: datetime, idle: int = 30
) -> list[CodexTurn]:
    return assemble_codex_turns(events, now=now, idle_minutes=idle)


def _closed_turn(events: list[CodexLogRecord], index: int = 0) -> CodexTurn:
    turns = _assemble(events, now=_ts(12))
    assert len(turns) > index
    assert turns[index].closed
    return turns[index]


def test_pair_by_turn_id() -> None:
    events = [
        _log("p1", "PROMPT_SENT", turn_id="t1", prompt_text="hi", timestamp=_ts(9)),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            turn_id="t2",
            timestamp=_ts(9, 1),
            response_text="wrong",
            status="ok",
        ),
        _log(
            "r2",
            "PROMPT_RESPONSE_RECEIVED",
            turn_id="t1",
            timestamp=_ts(9, 2),
            response_text="ok",
            status="ok",
            token_usage=TokenUsage(input_tokens=3, output_tokens=7),
        ),
        _log("u", "PROMPT_SENT", turn_id="t3", prompt_text="next", timestamp=_ts(9, 3)),
    ]
    turn = _closed_turn(events)
    assert turn.turn_id == "t1"
    assert [r.event_id for r in turn.responses] == ["r2"]


def test_fallback_without_turn_id() -> None:
    events = [
        _log("p1", "PROMPT_SENT", turn_id=None, prompt_text="hi", timestamp=_ts(9)),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            turn_id=None,
            timestamp=_ts(9, 1),
            response_text="ok",
            status="ok",
        ),
        _log(
            "p2", "PROMPT_SENT", turn_id=None, prompt_text="next", timestamp=_ts(9, 5)
        ),
    ]
    turns = _assemble(events, now=_ts(10))
    assert turns[0].responses[0].event_id == "r1"
    assert turns[0].closed is True


def test_multi_response_joined_and_tokens_summed() -> None:
    events = [
        _log("p1", "PROMPT_SENT", prompt_text="q", timestamp=_ts(9)),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 1),
            response_text="a",
            status="ok",
            token_usage=TokenUsage(input_tokens=1, output_tokens=2),
        ),
        _log(
            "r2",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 2),
            response_text="b",
            status="ok",
            token_usage=TokenUsage(input_tokens=4, output_tokens=8),
            model="gpt-codex",
        ),
        _log("p2", "PROMPT_SENT", prompt_text="x", timestamp=_ts(9, 3)),
    ]
    turn = _closed_turn(events)
    payload = codex_turn_to_payload(turn, None, anonymize=False)
    assert payload is not None
    assert payload["interaction"]["output"] == "a\nb"
    llm = payload["traces"][-1]
    assert llm["input_tokens"] == 5
    assert llm["output_tokens"] == 10


def test_error_response_excluded_from_output() -> None:
    events = [
        _log("p1", "PROMPT_SENT", prompt_text="q", timestamp=_ts(9)),
        _log(
            "r-bad",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 1),
            response_text="nope",
            status="error",
        ),
        _log(
            "r-good",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 2),
            response_text="yes",
            status="ok",
        ),
        _log("p2", "PROMPT_SENT", prompt_text="x", timestamp=_ts(9, 3)),
    ]
    turn = _closed_turn(events)
    payload = codex_turn_to_payload(turn, None, anonymize=False)
    assert payload is not None
    assert payload["interaction"]["output"] == "yes"


def test_open_vs_closed_by_idle_and_next_prompt() -> None:
    events = [
        _log("p1", "PROMPT_SENT", prompt_text="q", timestamp=_ts(9)),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 1),
            response_text="a",
            status="ok",
        ),
    ]
    open_turn = _assemble(events, now=_ts(9, 5), idle=30)[0]
    assert open_turn.closed is False
    assert codex_turn_to_payload(open_turn, None, anonymize=False) is None

    closed_idle = _assemble(events, now=_ts(10, 0), idle=30)[0]
    assert closed_idle.closed is True

    closed_next = [
        *events,
        _log("p2", "PROMPT_SENT", prompt_text="n", timestamp=_ts(9, 3)),
    ]
    assert _assemble(closed_next, now=_ts(9, 4))[0].closed is True


def test_tool_traces_completed_and_failed() -> None:
    events = [
        _log("p1", "PROMPT_SENT", prompt_text="q", timestamp=_ts(9)),
        _log(
            "t-ok",
            "TOOL_CALL_COMPLETED",
            timestamp=_ts(9, 1),
            tool_name="grep",
            tool_input={"pattern": "foo"},
        ),
        _log(
            "t-bad",
            "TOOL_CALL_FAILED",
            timestamp=_ts(9, 2),
            tool_name="shell",
            tool_input={"cmd": "rm"},
        ),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 3),
            response_text="done",
            status="ok",
        ),
        _log("p2", "PROMPT_SENT", prompt_text="x", timestamp=_ts(9, 4)),
    ]
    turn = _closed_turn(events)
    payload = codex_turn_to_payload(turn, None, anonymize=False)
    assert payload is not None
    tools = [t for t in payload["traces"] if "model" not in t]
    assert tools[0] == {
        "source": "grep",
        "input": '{"pattern":"foo"}',
        "outputs": [],
    }
    assert tools[1]["outputs"] == ["failed"]


def test_end_user_and_entra_tags() -> None:
    events = [
        _log(
            "p1",
            "PROMPT_SENT",
            prompt_text="q",
            actor=Actor(user_id="usr-9", user_email="dev@company.com"),
            timestamp=_ts(9),
        ),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 1),
            response_text="a",
            status="ok",
        ),
        _log("p2", "PROMPT_SENT", prompt_text="x", timestamp=_ts(9, 2)),
    ]
    turn = _closed_turn(events)
    payload = codex_turn_to_payload(
        turn, None, anonymize=False, directory=EntraDirectory([_ENTRA_USER])
    )
    assert payload is not None
    assert payload["interaction"]["end_user"] == "usr-9"
    assert payload["interaction"]["tags"]["department"] == "Platform"


def test_turn_record_cost_and_classification_tags() -> None:
    events = [
        _log("p1", "PROMPT_SENT", prompt_text="q", timestamp=_ts(9)),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 1),
            response_text="a",
            status="ok",
        ),
        _log("p2", "PROMPT_SENT", prompt_text="x", timestamp=_ts(9, 2)),
    ]
    turn = _closed_turn(events)
    turn_record = CodexTurnRecord(
        event_id="turn-file-1",
        payload=CodexTurnPayload(
            turn_id="turn-1",
            cost_usd=0.01,
            estimated_cost_usd=None,
            token_usage=TokenUsage(
                uncached_input_tokens=5,
                cached_input_tokens=2,
                output_tokens=11,
            ),
            classification=CodexTurnClassification(level_1="code", level_2="debug"),
        ),
    )
    payload = codex_turn_to_payload(turn, turn_record, anonymize=False)
    assert payload is not None
    assert payload["interaction"]["cost"] == 10_000
    llm = payload["traces"][-1]
    assert llm["input_tokens"] == 7
    assert llm["output_tokens"] == 11
    assert payload["interaction"]["tags"]["codex category"] == "code"


def test_without_turn_record_no_cost_uses_summed_tokens() -> None:
    events = [
        _log("p1", "PROMPT_SENT", prompt_text="q", timestamp=_ts(9)),
        _log(
            "r1",
            "PROMPT_RESPONSE_RECEIVED",
            timestamp=_ts(9, 1),
            response_text="a",
            status="ok",
            token_usage=TokenUsage(input_tokens=2, output_tokens=3),
        ),
        _log("p2", "PROMPT_SENT", prompt_text="x", timestamp=_ts(9, 2)),
    ]
    turn = _closed_turn(events)
    payload = codex_turn_to_payload(turn, None, anonymize=False)
    assert payload is not None
    assert "cost" not in payload["interaction"]
    llm = payload["traces"][-1]
    assert llm["input_tokens"] == 2
    assert llm["output_tokens"] == 3


def test_legacy_codex_record_to_payload() -> None:
    record = CodexLogRecord(
        event_id="legacy-1",
        client_id="client-1",
        timestamp=_ts(),
        event_details=CodexEventDetails(
            detail_type="PROMPT_RESPONSE_RECEIVED",
            session_id="sess-1",
            prompt_text="hello",
            response_text="world",
            status="ok",
            model="gpt-5-codex",
            token_usage=TokenUsage(input_tokens=10, output_tokens=20),
        ),
    )
    payload = codex_record_to_payload(record, anonymize=False)
    assert payload is not None
    assert payload["interaction"]["conversation_id"] == "sess-1"
    assert payload["traces"][-1]["input_tokens"] == 10


def test_skipped_empty_without_output() -> None:
    events = [
        _log("p1", "PROMPT_SENT", prompt_text="only", timestamp=_ts(9)),
        _log("p2", "PROMPT_SENT", prompt_text="close", timestamp=_ts(9, 2)),
    ]
    turn = _assemble(events, now=_ts(9, 3))[0]
    assert codex_turn_to_payload(turn, None, anonymize=False) is None
