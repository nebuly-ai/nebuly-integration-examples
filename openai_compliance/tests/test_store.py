from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from openai_compliance_sync.store import ComplianceStore

if TYPE_CHECKING:
    from pathlib import Path


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, tzinfo=UTC)


def _store(
    tmp_path: Path, *, principal: str = "org-demo", dry_run: bool = False
) -> ComplianceStore:
    return ComplianceStore(tmp_path / "state.db", principal, dry_run=dry_run)


def test_cursor_round_trip(tmp_path: Path) -> None:
    store = _store(tmp_path)
    assert store.get_cursor("CONVERSATION_MESSAGE") is None
    store.set_cursor("CONVERSATION_MESSAGE", _ts(8))
    store.commit()
    assert store.get_cursor("CONVERSATION_MESSAGE") == _ts(8)


def test_insert_chat_message_is_idempotent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    payload: dict[str, object] = {"message": {"id": "m1"}}
    assert (
        store.insert_chat_message(
            message_id="m1",
            conversation_id="c1",
            author_type="user",
            created_at=_ts(9),
            previous_message_id=None,
            record=payload,
        )
        is True
    )
    assert (
        store.insert_chat_message(
            message_id="m1",
            conversation_id="c1",
            author_type="user",
            created_at=_ts(9),
            previous_message_id=None,
            record=payload,
        )
        is False
    )


def test_app_calls_window_query(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.insert_app_call(
        event_id="req-1",
        conversation_id="c1",
        app_id="app-a",
        record_kind="request",
        timestamp=_ts(10),
        record={"request": {"input": "q"}},
    )
    store.insert_app_call(
        event_id="res-1",
        conversation_id="c1",
        app_id="app-a",
        record_kind="response",
        timestamp=_ts(10, 5),
        record={"response": {}},
    )
    store.insert_app_call(
        event_id="other",
        conversation_id="c2",
        app_id="app-a",
        record_kind="request",
        timestamp=_ts(10),
        record={},
    )

    in_window = store.app_calls_in_window("c1", start=_ts(9), end=_ts(11))
    assert [row.event_id for row in in_window] == ["req-1", "res-1"]


def test_unemitted_user_messages_excludes_recorded(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.insert_chat_message(
        message_id="u1",
        conversation_id="c1",
        author_type="user",
        created_at=_ts(9),
        previous_message_id=None,
        record={},
    )
    store.insert_chat_message(
        message_id="a1",
        conversation_id="c1",
        author_type="assistant",
        created_at=_ts(9, 1),
        previous_message_id="u1",
        record={},
    )
    pending = store.unemitted_user_messages()
    assert [m.message_id for m in pending] == ["u1"]

    store.record_emitted("chats", "u1", "sent")
    assert store.unemitted_user_messages() == []


def test_prune_drops_old_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    old = datetime.now(tz=UTC) - timedelta(days=40)
    store.insert_chat_message(
        message_id="old-u",
        conversation_id="c1",
        author_type="user",
        created_at=old,
        previous_message_id=None,
        record={},
    )
    store.insert_chat_message(
        message_id="new-u",
        conversation_id="c1",
        author_type="user",
        created_at=datetime.now(tz=UTC),
        previous_message_id=None,
        record={},
    )
    store.insert_app_call(
        event_id="old-call",
        conversation_id="c1",
        app_id="app",
        record_kind="request",
        timestamp=old,
        record={},
    )
    store.prune(retention_days=35)

    ids = {m.message_id for m in store.conversation_messages("c1")}
    assert ids == {"new-u"}
    assert store.app_calls_in_window("c1", start=old, end=datetime.now(tz=UTC)) == []


def test_dry_run_uses_memory_and_persists_nothing(tmp_path: Path) -> None:
    store = _store(tmp_path, dry_run=True)
    store.set_cursor("CONVERSATION_MESSAGE", _ts(12))
    store.insert_chat_message(
        message_id="m1",
        conversation_id="c1",
        author_type="user",
        created_at=_ts(10),
        previous_message_id=None,
        record={},
    )
    store.commit()
    store.close()

    assert not (tmp_path / "state.db").exists()
