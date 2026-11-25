from __future__ import annotations

import json
from pathlib import Path

from github_copilot_sync.conversations import ConversationBuilder
from github_copilot_sync.converter import SkipReason, turn_to_payload
from github_copilot_sync.models import Call
from github_copilot_sync.store import Store


def _wrap(payload: object) -> str:
    return json.dumps(json.dumps(payload))


def _call(
    *,
    user_text: str = "hello",
    assistant_text: str = "world",
    request_ms: int = 1_000,
    response_ms: int = 2_000,
) -> Call:
    request_body = _wrap(
        {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": user_text}],
        }
    )
    response_body = _wrap(
        {
            "object": "chat.completion",
            "model": "gpt-4o",
            "choices": [
                {
                    "message": {"role": "assistant", "content": assistant_text},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
            },
        }
    )
    return Call(
        github_request_id="req-1",
        user_id=42,
        endpoint="/enterprises/e/copilot/chat/completions",
        request_body=request_body,
        response_body=response_body,
        request_timestamp_ms=request_ms,
        response_timestamp_ms=response_ms,
        request_event_id="e-req",
        response_event_id="e-resp",
    )


def test_turn_to_payload_builds_llm_trace(tmp_path: Path) -> None:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    closed = builder.add_call(_call())
    closed.extend(builder.close_all_open())
    assert len(closed) == 1
    payload = turn_to_payload(
        closed[0],
        end_user="dev@example.com",
        email="dev@example.com",
        anonymize=False,
        directory=None,
    )
    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["input"] == "hello"
    assert payload["interaction"]["output"] == "world"
    assert payload["traces"][0]["model"] == "gpt-4o"
    assert payload["traces"][0]["input_tokens"] == 10
