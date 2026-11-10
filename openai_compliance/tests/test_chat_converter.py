from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from openai_compliance_sync.chat_converter import (
    AssembledInteraction,
    app_calls_to_traces,
    assemble_interactions,
    interaction_to_payload,
)
from openai_compliance_sync.entra import EntraDirectory, EntraUser
from openai_compliance_sync.models import (
    Actor,
    ChatMessagePayload,
    ConversationMessageRecord,
    ConversationRef,
    MessageAuthor,
    MessageContent,
)
from openai_compliance_sync.store import StoredAppCall

_ENTRA_USER = EntraUser.model_validate(
    {
        "mail": "user@example.com",
        "department": "Engineering",
        "country": "IT",
        "employeeHireDate": "2020-01-01T00:00:00Z",
    }
)


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _msg(
    msg_id: str,
    *,
    author_type: str,
    created_at: datetime,
    text: str,
    previous_message_id: str | None = None,
    model: str | None = None,
    conversation_id: str = "conv-1",
) -> ConversationMessageRecord:
    return ConversationMessageRecord(
        actor=Actor(user_id="u1", user_email="user@example.com"),
        message=ChatMessagePayload(
            id=msg_id,
            created_at=created_at,
            author=MessageAuthor(type=author_type, model=model),
            content=MessageContent(type="text", value=text),
        ),
        previous_message_id=previous_message_id,
        conversation=ConversationRef(id=conversation_id, title="Title"),
    )


def _assembled(
    records: list[ConversationMessageRecord],
    *,
    now: datetime,
    idle_minutes: int = 30,
) -> list[AssembledInteraction]:
    return assemble_interactions(records, now=now, idle_minutes=idle_minutes)


def test_simple_pair_closed_by_next_user() -> None:
    records = [
        _msg("u1", author_type="user", created_at=_ts(9), text="hello"),
        _msg(
            "a1",
            author_type="assistant",
            created_at=_ts(9, 1),
            text="hi",
            previous_message_id="u1",
        ),
        _msg(
            "u2",
            author_type="user",
            created_at=_ts(9, 2),
            text="again",
            previous_message_id="a1",
        ),
    ]
    items = _assembled(records, now=_ts(9, 3))
    first = items[0]
    assert first.closed is True
    assert [row.message.id for row in first.assistants] == ["a1"]
    payload = interaction_to_payload(
        first, retrieval_traces=[], anonymize=False, directory=None
    )
    assert payload is not None
    assert payload["interaction"]["output"] == "hi"


def test_multi_assistant_chain_output() -> None:
    records = [
        _msg("u1", author_type="user", created_at=_ts(9), text="hello"),
        _msg(
            "a1",
            author_type="assistant",
            created_at=_ts(9, 1),
            text="part1",
            previous_message_id="u1",
        ),
        _msg(
            "a2",
            author_type="assistant",
            created_at=_ts(9, 2),
            text="part2",
            previous_message_id="a1",
            model="gpt-4.1",
        ),
        _msg(
            "u2",
            author_type="user",
            created_at=_ts(9, 3),
            text="done",
            previous_message_id="a2",
        ),
    ]
    first = _assembled(records, now=_ts(9, 4))[0]
    assert [row.message.id for row in first.assistants] == ["a1", "a2"]
    payload = interaction_to_payload(
        first, retrieval_traces=[], anonymize=False, directory=None
    )
    assert payload is not None
    assert payload["interaction"]["output"] == "part1\npart2"
    assert payload["interaction"]["tags"]["models"] == "gpt-4.1"


def test_regeneration_picks_latest_assistant() -> None:
    records = [
        _msg("u1", author_type="user", created_at=_ts(9), text="hello"),
        _msg(
            "a-old",
            author_type="assistant",
            created_at=_ts(9, 1),
            text="old",
            previous_message_id="u1",
        ),
        _msg(
            "a-new",
            author_type="assistant",
            created_at=_ts(9, 2),
            text="new",
            previous_message_id="u1",
        ),
        _msg(
            "u2",
            author_type="user",
            created_at=_ts(9, 3),
            text="next",
            previous_message_id="a-new",
        ),
    ]
    first = _assembled(records, now=_ts(9, 4))[0]
    assert [row.message.id for row in first.assistants] == ["a-new"]
    payload = interaction_to_payload(
        first, retrieval_traces=[], anonymize=False, directory=None
    )
    assert payload is not None
    assert payload["interaction"]["output"] == "new"


def test_edited_user_message_is_separate_interaction() -> None:
    records = [
        _msg("u-original", author_type="user", created_at=_ts(9), text="v1"),
        _msg(
            "a1",
            author_type="assistant",
            created_at=_ts(9, 1),
            text="ok",
            previous_message_id="u-original",
        ),
        _msg("u-edited", author_type="user", created_at=_ts(9, 2), text="v2"),
    ]
    items = _assembled(records, now=_ts(10))
    assert len(items) == 2
    assert items[0].interaction_key == "u-original"
    assert items[1].interaction_key == "u-edited"


def test_idle_closes_interaction() -> None:
    records = [
        _msg("u1", author_type="user", created_at=_ts(9), text="hello"),
        _msg(
            "a1",
            author_type="assistant",
            created_at=_ts(9, 5),
            text="late",
            previous_message_id="u1",
        ),
    ]
    open_item = _assembled(records, now=_ts(9, 20), idle_minutes=30)[0]
    closed_item = _assembled(records, now=_ts(10, 0), idle_minutes=30)[0]
    assert open_item.closed is False
    assert closed_item.closed is True


def test_empty_user_text_skipped() -> None:
    records = [
        _msg("u1", author_type="user", created_at=_ts(9), text="   "),
        _msg(
            "a1",
            author_type="assistant",
            created_at=_ts(9, 1),
            text="hi",
            previous_message_id="u1",
        ),
        _msg(
            "u2",
            author_type="user",
            created_at=_ts(9, 2),
            text="x",
            previous_message_id="a1",
        ),
    ]
    first = _assembled(records, now=_ts(9, 3))[0]
    assert (
        interaction_to_payload(
            first, retrieval_traces=[], anonymize=False, directory=None
        )
        is None
    )


def test_model_fallback_unknown() -> None:
    records = [
        _msg("u1", author_type="user", created_at=_ts(9), text="hello"),
        _msg(
            "a1",
            author_type="assistant",
            created_at=_ts(9, 1),
            text="hi",
            previous_message_id="u1",
        ),
        _msg(
            "u2",
            author_type="user",
            created_at=_ts(9, 2),
            text="next",
            previous_message_id="a1",
        ),
    ]
    first = _assembled(records, now=_ts(9, 3))[0]
    payload = interaction_to_payload(
        first, retrieval_traces=[], anonymize=False, directory=None
    )
    assert payload is not None
    assert payload["interaction"]["tags"]["model"] == "unknown"
    llm_trace = payload["traces"][-1]
    assert llm_trace["model"] == "unknown"


def test_app_log_window_pairing() -> None:
    calls = [
        StoredAppCall(
            event_id="req-1",
            conversation_id="conv-1",
            app_id="app-a",
            record_kind="request",
            timestamp=_ts(9, 1),
            record_json=json.dumps(
                {
                    "event_id": "req-1",
                    "app_id": "app-a",
                    "app_name": "Search",
                    "conversation_id": "conv-1",
                    "timestamp": "2025-06-15T09:01:00Z",
                    "request": {"input": "query"},
                }
            ),
        ),
        StoredAppCall(
            event_id="res-1",
            conversation_id="conv-1",
            app_id="app-a",
            record_kind="response",
            timestamp=_ts(9, 2),
            record_json=json.dumps(
                {
                    "event_id": "res-1",
                    "app_id": "app-a",
                    "conversation_id": "conv-1",
                    "timestamp": "2025-06-15T09:02:00Z",
                    "response": {
                        "output": [
                            {"response": {"result": {"content": [{"text": "result"}]}}}
                        ]
                    },
                }
            ),
        ),
    ]
    traces = app_calls_to_traces(calls)
    assert traces == [{"source": "Search", "input": "query", "outputs": ["result"]}]


def test_entra_tags_and_segments_applied() -> None:
    directory = EntraDirectory([_ENTRA_USER])
    records = [
        _msg("u1", author_type="user", created_at=_ts(9), text="hello"),
        _msg(
            "a1",
            author_type="assistant",
            created_at=_ts(9, 1),
            text="hi",
            previous_message_id="u1",
        ),
        _msg(
            "u2",
            author_type="user",
            created_at=_ts(9, 2),
            text="next",
            previous_message_id="a1",
        ),
    ]
    first = _assembled(records, now=_ts(9, 3))[0]
    payload = interaction_to_payload(
        first, retrieval_traces=[], anonymize=False, directory=directory
    )
    assert payload is not None
    tags: dict[str, Any] = payload["interaction"]["tags"]
    assert tags["department"] == "Engineering"
    assert payload["interaction"]["team"] == "Engineering"
    assert payload["interaction"]["market"] == "IT"
