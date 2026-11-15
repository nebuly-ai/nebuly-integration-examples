"""The SQLite state and the outbox rules both connectors share."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest

from bedrock_sync.shared.errors import StateError
from tests.shared.stores import PAYLOAD, OutboxStore

if TYPE_CHECKING:
    from pathlib import Path


def test_second_process_is_refused_until_the_first_closes(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path), pytest.raises(StateError, match="Another process"):
        OutboxStore(path)
    with OutboxStore(path):
        pass


def test_foreign_database_is_refused_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE ingest_cursor(value TEXT)")
    before = path.read_bytes()
    with pytest.raises(StateError, match="Not a supported test database"):
        OutboxStore(path)
    assert path.read_bytes() == before


def test_a_file_that_is_not_sqlite_is_refused_with_a_clear_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    path.write_text('{"not": "a database"}')
    with pytest.raises(StateError, match=r"Cannot use state\.db as the test state"):
        OutboxStore(path)


def test_dry_run_works_on_a_copy_and_creates_nothing(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path) as store:
        store.put("i-1")
        store.begin_attempt("i-1")
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with OutboxStore(path, dry_run=True) as store:
        assert store.get("i-1").status == "sending"  # no crash recovery in a dry run
        store.put("i-2")
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before
    with OutboxStore(tmp_path / "absent" / "state.db", dry_run=True) as store:
        store.put("i-3")
    assert not (tmp_path / "absent").exists()


def test_dry_run_refuses_a_database_with_an_active_journal(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    OutboxStore(path).close()
    path.with_name(path.name + "-wal").write_bytes(b"synthetic")
    with pytest.raises(StateError, match="active journal"):
        OutboxStore(path, dry_run=True)


@pytest.mark.parametrize(
    ("outcome", "status"), [("sent", 200), ("rejected", 422), ("uncertain", 503)]
)
def test_final_outcomes_are_durable_and_never_replayed(
    tmp_path: Path, outcome: str, status: int
) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path) as store:
        store.put("i-1")
        attempt = store.begin_attempt("i-1")
        assert store.pending_deliveries() == ()
        store.finish_attempt(attempt, outcome, http_status=status)  # type: ignore[arg-type]
        with pytest.raises(StateError, match="already finished"):
            store.finish_attempt(attempt, outcome, http_status=status)  # type: ignore[arg-type]
    with OutboxStore(path) as store:
        assert store.get("i-1").status == outcome
        with pytest.raises(StateError, match="Only pending"):
            store.begin_attempt("i-1")


def test_crash_while_sending_becomes_uncertain(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path) as store:
        store.put("i-1")
        store.begin_attempt("i-1")
    with OutboxStore(path) as store:
        assert store.get("i-1").status == "uncertain"
        assert store.pending_deliveries() == ()


@pytest.mark.parametrize(
    ("outcome", "status"),
    [("sent", None), ("sent", 500), ("rejected", 429), ("rejected", 408)],
)
def test_unconfirmed_responses_are_neither_sent_nor_rejected(
    tmp_path: Path, outcome: str, status: int | None
) -> None:
    with OutboxStore(tmp_path / "state.db") as store:
        store.put("i-1")
        attempt = store.begin_attempt("i-1")
        with pytest.raises(ValueError, match="requires"):
            store.finish_attempt(attempt, outcome, http_status=status)  # type: ignore[arg-type]
        assert store.get("i-1").status == "sending"


def test_retryable_waits_for_its_deadline(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path, now_ms=1_000) as store:
        store.put("i-1")
        store.put("i-2")
        attempt = store.begin_attempt("i-1")
        store.finish_attempt(attempt, "retryable", retry_after_seconds=60)
        # The deadline holds back the whole source, not only the item.
        assert store.pending_deliveries() == ()
    with OutboxStore(path, now_ms=62_000) as store:
        assert [d.interaction_key for d in store.pending_deliveries()] == [
            "i-1",
            "i-2",
        ]


def test_only_a_never_attempted_payload_can_change(tmp_path: Path) -> None:
    with OutboxStore(tmp_path / "state.db") as store:
        store.put("i-1", {"version": 1})
        store.put("i-1", {"version": 2})
        delivery = store.find_delivery("i-1")
        assert delivery is not None
        assert delivery.payload == {"version": 2}
        store.begin_attempt("i-1")
        with pytest.raises(StateError, match="cannot be replaced"):
            store.put("i-1", {"version": 3})


def test_retention_compacts_only_old_final_payloads(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path, now_ms=1_000) as store:
        store.put("i-1")
        store.put("i-2")
        store.finish_attempt(store.begin_attempt("i-1"), "sent", http_status=200)
    with OutboxStore(path, now_ms=1_000 + 8 * 86_400_000) as store:
        assert store.prune(7 * 86_400) == 1
        sent, pending = store.find_delivery("i-1"), store.find_delivery("i-2")
        assert sent is not None
        assert pending is not None
        assert (sent.status, sent.payload) == ("sent", {})
        assert pending.payload == PAYLOAD
        with pytest.raises(ValueError, match="nonnegative"):
            store.prune(-1)
