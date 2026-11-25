from __future__ import annotations

import json

import pytest
from github_copilot_sync import user_defined
from github_copilot_sync.conversations import ConversationBuilder
from github_copilot_sync.models import Call
from github_copilot_sync.store import Store


def _wrap(payload: object) -> str:
    return json.dumps(json.dumps(payload))


def _call(
    *,
    github_request_id: str,
    user_id: int = 1,
    user_text: str,
    extra_messages: list[dict[str, object]] | None = None,
    endpoint: str = "/enterprises/e/copilot/chat/completions",
    request_ms: int,
    response_ms: int,
    system: str | None = None,
    tools: list | None = None,
) -> Call:
    messages: list[dict[str, object]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user_text})
    if extra_messages:
        messages.extend(extra_messages)
    request_body = _wrap(
        {"model": "gpt-4o", "messages": messages, **({"tools": tools} if tools else {})}
    )
    response_body = _wrap(
        {
            "object": "chat.completion",
            "choices": [
                {
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
        }
    )
    return Call(
        github_request_id=github_request_id,
        user_id=user_id,
        endpoint=endpoint,
        request_body=request_body,
        response_body=response_body,
        request_timestamp_ms=request_ms,
        response_timestamp_ms=response_ms,
        request_event_id=f"req-{github_request_id}",
        response_event_id=f"resp-{github_request_id}",
    )


def test_tool_loop_stays_in_same_turn(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    first = _call(
        github_request_id="a",
        user_text="debug",
        request_ms=1_000,
        response_ms=1_100,
        extra_messages=[
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "t1",
                        "type": "function",
                        "function": {"name": "grep", "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "t1", "content": "hits"},
        ],
    )
    second = _call(
        github_request_id="b",
        user_text="debug",
        request_ms=1_200,
        response_ms=1_300,
        extra_messages=[
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "t1",
                        "type": "function",
                        "function": {"name": "grep", "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "t1", "content": "hits"},
            {"role": "assistant", "content": "fixed"},
        ],
    )

    builder.add_call(first)
    builder.add_call(second)
    open_turns = builder.close_all_open()

    assert len(open_turns) == 1
    assert len(open_turns[0].calls) == 2
    store.close()


def test_new_turn_closes_previous(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    first = _call(
        github_request_id="t1",
        user_text="first question",
        request_ms=1_000,
        response_ms=1_500,
    )
    closed_on_first = builder.add_call(first)
    assert closed_on_first == []

    second = _call(
        github_request_id="t2",
        user_text="first question",
        request_ms=2_000,
        response_ms=2_500,
        extra_messages=[
            {"role": "assistant", "content": "here is the answer"},
            {"role": "user", "content": "second question"},
        ],
    )
    closed_on_second = builder.add_call(second)

    assert len(closed_on_second) == 1
    assert closed_on_second[0].calls[0].github_request_id == "t1"
    store.close()


def test_gap_splits_conversation(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    gap_hours = 8
    builder = ConversationBuilder(
        store, conversation_gap_hours=gap_hours, interaction_idle_minutes=30
    )
    ms_gap = (gap_hours + 1) * 3_600_000
    c1 = _call(
        github_request_id="g1",
        user_text="same opener",
        request_ms=1_000,
        response_ms=2_000,
    )
    c2 = _call(
        github_request_id="g2",
        user_text="same opener",
        request_ms=1_000 + ms_gap,
        response_ms=2_000 + ms_gap,
    )

    builder.add_call(c1)
    turns = builder.add_call(c2)
    open_turns = builder.close_all_open()

    conversation_ids = {t.conversation_id for t in (turns + open_turns)}
    assert len(conversation_ids) == 2
    store.close()


def test_idle_close_on_watermark(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    call = _call(
        github_request_id="idle",
        user_text="wait",
        request_ms=1_000,
        response_ms=5_000,
    )
    builder.add_call(call)
    idle_ms = 30 * 60_000
    closed = builder.close_idle_turns(watermark_ms=5_000 + idle_ms)

    assert len(closed) == 1
    store.close()


def test_skips_non_chat_endpoint(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    call = _call(
        github_request_id="embed",
        user_text="vec",
        endpoint="/embeddings",
        request_ms=1_000,
        response_ms=1_100,
    )
    builder.add_call(call)

    assert builder.counters.skipped_non_chat == 1
    assert builder.close_all_open() == []
    store.close()


def test_skips_helper_call(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )
    call = _call(
        github_request_id="title",
        user_text="my chat",
        system="Generate a concise title for this conversation",
        request_ms=1_000,
        response_ms=1_100,
    )
    builder.add_call(call)

    assert builder.counters.skipped_background == 1
    store.close()


def test_user_defined_hook_can_drop_call(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "db.sqlite")
    builder = ConversationBuilder(
        store, conversation_gap_hours=8, interaction_idle_minutes=30
    )

    def _always_drop(_call: Call) -> bool:
        return True

    monkeypatch.setattr(user_defined, "is_background_call", _always_drop)
    call = _call(
        github_request_id="hook",
        user_text="real",
        request_ms=1_000,
        response_ms=1_100,
    )
    builder.add_call(call)

    assert builder.counters.skipped_background == 1
    store.close()
