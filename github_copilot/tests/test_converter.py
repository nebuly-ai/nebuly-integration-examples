from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

from github_copilot_sync import converter
from github_copilot_sync.conversations import ConversationBuilder
from github_copilot_sync.converter import SkipReason, turn_to_payload
from github_copilot_sync.models import Call, ClosedTurn
from github_copilot_sync.openai_body import parse_call_bodies
from github_copilot_sync.store import Store

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from github_copilot_sync.openai_body import ParsedBody


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


def _tool_call_message(tool_id: str, name: str) -> dict[str, object]:
    return {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": tool_id,
                "type": "function",
                "function": {"name": name, "arguments": "{}"},
            }
        ],
    }


def test_tools_from_prior_turn_are_not_reported(tmp_path: Path) -> None:
    messages = [
        {"role": "user", "content": "first question"},
        _tool_call_message("t-old", "old_tool"),
        {"role": "tool", "tool_call_id": "t-old", "content": "old result"},
        {"role": "assistant", "content": "first answer"},
        {"role": "user", "content": "second question"},
        _tool_call_message("t-new", "new_tool"),
        {"role": "tool", "tool_call_id": "t-new", "content": "new result"},
    ]
    call = replace(
        _call(user_text="second question", assistant_text="second answer"),
        request_body=_wrap({"model": "gpt-4o", "messages": messages}),
    )
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    builder.add_call(call)
    turn = builder.close_all_open()[0]
    payload = turn_to_payload(
        turn, end_user="u", email=None, anonymize=False, directory=None
    )

    assert not isinstance(payload, SkipReason)
    retrieval = [t["source"] for t in payload["traces"] if "source" in t]
    assert retrieval == ["new_tool"]
    assert payload["interaction"]["tags"]["tools used"] == "new_tool"
    store.close()


def test_turn_without_tools_has_no_tool_tag_despite_history(tmp_path: Path) -> None:
    messages = [
        {"role": "user", "content": "first question"},
        _tool_call_message("t-old", "old_tool"),
        {"role": "tool", "tool_call_id": "t-old", "content": "old result"},
        {"role": "assistant", "content": "first answer"},
        {"role": "user", "content": "second question"},
    ]
    call = replace(
        _call(user_text="second question", assistant_text="second answer"),
        request_body=_wrap({"model": "gpt-4o", "messages": messages}),
    )
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    builder.add_call(call)
    turn = builder.close_all_open()[0]
    payload = turn_to_payload(
        turn, end_user="u", email=None, anonymize=False, directory=None
    )

    assert not isinstance(payload, SkipReason)
    assert [t for t in payload["traces"] if "source" in t] == []
    assert "tools used" not in payload["interaction"]["tags"]
    store.close()


def _closed_turn(tmp_path: Path, *calls: Call) -> ClosedTurn:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    for call in calls:
        builder.add_call(call)
    return builder.close_all_open()[0]


def test_each_call_is_parsed_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    turn = _closed_turn(
        tmp_path,
        _call(request_ms=1_000, response_ms=2_000),
        replace(_call(request_ms=3_000, response_ms=4_000), github_request_id="req-2"),
    )
    parse_count = {"n": 0}
    real_parse = parse_call_bodies

    def counting_parse(request_body: str, response_body: str) -> ParsedBody:
        parse_count["n"] += 1
        return real_parse(request_body, response_body)

    monkeypatch.setattr(converter, "parse_call_bodies", counting_parse)

    payload = turn_to_payload(
        turn, end_user="u", email=None, anonymize=False, directory=None
    )

    assert not isinstance(payload, SkipReason)
    assert parse_count["n"] == len(turn.calls)


def test_unparseable_call_is_skipped_in_later_calls(tmp_path: Path) -> None:
    good = _call(request_ms=1_000, response_ms=2_000)
    bad = replace(
        _call(request_ms=3_000, response_ms=4_000),
        github_request_id="req-2",
        request_body="not json",
        response_body="not json",
    )
    turn = replace(_closed_turn(tmp_path, good), calls=(good, bad))

    payload = turn_to_payload(
        turn, end_user="u", email=None, anonymize=False, directory=None
    )

    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["output"] == "world"
    assert len(payload["traces"]) == 1
