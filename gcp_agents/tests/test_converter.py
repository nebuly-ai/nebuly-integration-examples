from __future__ import annotations

from gcp_agents_sync.converter import SkipReason, group_turns, turn_to_payload
from gcp_agents_sync.models import Event, Session


def _session(session_id: str = "sess_1") -> Session:
    return Session.model_validate(
        {
            "name": f"projects/p/locations/l/reasoningEngines/e/sessions/{session_id}",
            "userId": "user_1",
            "createTime": "2026-06-29T12:50:00.000000Z",
            "updateTime": "2026-06-29T12:50:07.000000Z",
        }
    )


def _multi_agent_turn_events() -> list[Event]:
    invocation = "inv-1"
    return [
        Event.model_validate(
            {
                "author": "user",
                "invocationId": invocation,
                "timestamp": "2026-06-29T12:50:01.000000Z",
                "content": {"role": "user", "parts": [{"text": "Translate hello"}]},
            }
        ),
        Event.model_validate(
            {
                "author": "root_agent",
                "invocationId": invocation,
                "timestamp": "2026-06-29T12:50:02.000000Z",
                "content": {
                    "role": "model",
                    "parts": [
                        {
                            "functionCall": {
                                "name": "transfer_to_agent",
                                "args": {"agent_name": "translator"},
                            }
                        }
                    ],
                },
                "eventMetadata": {
                    "customMetadata": {
                        "_usage_metadata": {
                            "prompt_token_count": 10,
                            "candidates_token_count": 5,
                            "total_token_count": 15,
                            "traffic_type": "ON_DEMAND",
                        }
                    }
                },
                "rawEvent": {"modelVersion": "gemini-2.5-flash"},
            }
        ),
        Event.model_validate(
            {
                "author": "root_agent",
                "invocationId": invocation,
                "timestamp": "2026-06-29T12:50:02.500000Z",
                "content": {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "name": "transfer_to_agent",
                                "response": {"result": None},
                            }
                        }
                    ],
                },
            }
        ),
        Event.model_validate(
            {
                "author": "translator",
                "invocationId": invocation,
                "timestamp": "2026-06-29T12:50:03.000000Z",
                "content": {
                    "role": "model",
                    "parts": [{"text": "Hola"}],
                },
                "eventMetadata": {
                    "customMetadata": {
                        "_usage_metadata": {
                            "prompt_token_count": 20,
                            "candidates_token_count": 2,
                            "total_token_count": 22,
                        }
                    }
                },
                "rawEvent": {
                    "modelVersion": "gemini-2.5-flash",
                    "nodeInfo": {"path": "translator@1"},
                },
            }
        ),
    ]


def _multi_turn_events() -> list[Event]:
    return [
        Event.model_validate(
            {
                "author": "user",
                "invocationId": "inv-a",
                "timestamp": "2026-06-29T12:50:01.000000Z",
                "content": {"role": "user", "parts": [{"text": "first"}]},
            }
        ),
        Event.model_validate(
            {
                "author": "agent",
                "invocationId": "inv-a",
                "timestamp": "2026-06-29T12:50:02.000000Z",
                "content": {"role": "model", "parts": [{"text": "one"}]},
            }
        ),
        Event.model_validate(
            {
                "author": "user",
                "invocationId": "inv-b",
                "timestamp": "2026-06-29T12:51:01.000000Z",
                "content": {"role": "user", "parts": [{"text": "second"}]},
            }
        ),
        Event.model_validate(
            {
                "author": "agent",
                "invocationId": "inv-b",
                "timestamp": "2026-06-29T12:51:02.000000Z",
                "content": {"role": "model", "parts": [{"text": "two"}]},
            }
        ),
    ]


def test_multi_agent_turn_groups_to_one_turn() -> None:
    turns = group_turns(_multi_agent_turn_events())
    assert len(turns) == 1
    assert turns[0].final_response is not None
    assert turns[0].final_response.author == "translator"


def test_multi_agent_payload_and_traces() -> None:
    turn = group_turns(_multi_agent_turn_events())[0]
    payload = turn_to_payload(turn, session=_session(), anonymize=False)
    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["input"] == "Translate hello"
    assert payload["interaction"]["output"] == "Hola"
    assert payload["interaction"]["conversation_id"] == "sess_1"
    assert len(payload["traces"]) == 2
    for trace in payload["traces"]:
        assert trace["messages"][-1]["role"] == "user"


def test_empty_session_events_returns_no_turns() -> None:
    assert group_turns([]) == []


def test_multi_turn_session_shares_conversation_id() -> None:
    turns = group_turns(_multi_turn_events())
    assert len(turns) == 2
    session = _session()
    payloads = [
        turn_to_payload(turn, session=session, anonymize=False) for turn in turns
    ]
    assert all(not isinstance(p, SkipReason) for p in payloads)
    assert {p["interaction"]["conversation_id"] for p in payloads} == {"sess_1"}  # type: ignore[index]


def test_dangling_user_message_skipped() -> None:
    events = [
        Event.model_validate(
            {
                "author": "user",
                "invocationId": "inv-x",
                "timestamp": "2026-06-29T12:50:01.000000Z",
                "content": {"role": "user", "parts": [{"text": "hello?"}]},
            }
        )
    ]
    turn = group_turns(events)[0]
    result = turn_to_payload(turn, session=_session(), anonymize=False)
    assert result is SkipReason.EMPTY_OUTPUT
