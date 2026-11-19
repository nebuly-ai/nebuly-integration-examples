"""The SQLite state and the outbox rules both connectors share."""

from __future__ import annotations

import logging
import os
import sqlite3
import stat
from pathlib import Path

import pytest

from bedrock_sync.shared.errors import StateError
from bedrock_sync.shared.outbox import GIVE_UP_SECONDS, OutboxError
from tests.shared.stores import PAYLOAD, OutboxStore


def test_second_process_is_refused_until_the_first_closes(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path), pytest.raises(StateError, match="Another process"):
        OutboxStore(path)
    with OutboxStore(path):
        pass


def test_a_new_state_file_is_announced_with_its_absolute_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO, logger="bedrock_sync.shared.store")
    with OutboxStore(Path("state.db")):
        pass
    absolute = str(tmp_path.resolve() / "state.db")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert absolute in warnings[0].getMessage()
    assert "--state-db" in warnings[0].getMessage()

    caplog.clear()
    with OutboxStore(Path("state.db")):
        pass
    assert [r.getMessage() for r in caplog.records] == [f"State file: {absolute}"]


def test_foreign_database_is_refused_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE ingest_cursor(value TEXT)")
    before = path.read_bytes()
    with pytest.raises(StateError, match="Not a supported test database"):
        OutboxStore(path)
    assert path.read_bytes() == before


def test_new_database_is_private_under_a_permissive_umask(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    previous = os.umask(0o022)
    try:
        with OutboxStore(path):
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        os.umask(previous)


def test_existing_database_has_a_permissions_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "state.db"
    OutboxStore(path).close()
    path.chmod(0o640)
    with OutboxStore(path):
        assert stat.S_IMODE(path.stat().st_mode) == 0o640
    assert "accessible to other users or groups" in caplog.text


def test_metadata_insert_failure_does_not_leave_a_half_initialized_database(
    tmp_path: Path,
) -> None:
    class FailingStore(OutboxStore):
        def _initialize(self) -> None:
            self._create_schema(
                "CREATE TABLE marker(value TEXT NOT NULL);",
                initialize=("INSERT INTO marker VALUES (?)", (None,)),
            )

    path = tmp_path / "state.db"
    with pytest.raises(StateError, match="NOT NULL"):
        FailingStore(path)
    with OutboxStore(path) as store:
        store.put("i-1")


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


@pytest.mark.parametrize(("outcome", "status"), [("sent", 200), ("rejected", 422)])
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


def test_crash_while_sending_is_sent_again(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path, now_ms=1_000) as store:
        store.put("i-1")
        store.begin_attempt("i-1")
    with OutboxStore(path, now_ms=2_000) as store:
        assert store.get("i-1").status == "pending"
        assert not store.pending_deliveries()
    with OutboxStore(path, now_ms=62_000) as store:
        assert len(store.pending_deliveries()) == 1


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


def test_item_cooldown_is_reported_even_without_a_source_pause(tmp_path: Path) -> None:
    with OutboxStore(tmp_path / "state.db", now_ms=1_000) as store:
        store.put("retry")
        store.put("ready")
        store.finish_attempt(
            store.begin_attempt("retry"),
            "retryable",
            http_status=503,
            maybe_received=True,
        )
        assert not store.delivery_paused()
        assert store.delivery_waiting_for_retry()
        assert store.delivery_waiting_for_retry("retry")
        assert not store.delivery_waiting_for_retry("ready")
        assert not store.pending_delivery("retry")
        assert len(store.pending_delivery("ready")) == 1


def test_retry_given_up_requeues_only_the_retained_ambiguous_payload(
    tmp_path: Path,
) -> None:
    with OutboxStore(tmp_path / "state.db", now_ms=1_000) as store:
        for key in ("given-up", "compacted", "permanent", "other"):
            store.put(key)
        store.finish_attempt(
            store.begin_attempt("permanent"), "rejected", http_status=422
        )
        for key in ("given-up", "compacted"):
            for stamp in (1_000, 1_000 + GIVE_UP_SECONDS * 1_000):
                store.finish_attempt(
                    store.begin_attempt(key),
                    "retryable",
                    http_status=503,
                    finished_ms=stamp,
                    maybe_received=True,
                )
            assert store.get(key).status == "rejected"
        store.now_ms = 1_000 + GIVE_UP_SECONDS * 1_000
        store.retry_given_up("given-up")
        assert store.delivery_was_attempted("given-up")
        assert not store.pending_delivery("given-up")
        assert store.delivery_waiting_for_retry("given-up")
        store.now_ms += 60_000
        [delivery] = store.pending_delivery("given-up")
        assert delivery.payload == PAYLOAD
        assert store.get("other").status == "pending"
        with pytest.raises(OutboxError, match="No retained delivery"):
            store.retry_given_up("permanent")
        store.prune(0)
        with pytest.raises(OutboxError, match="No retained delivery"):
            store.retry_given_up("compacted")


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
        assert store.prune(10**20) == 0
        assert store.prune(7 * 86_400) == 1
        sent, pending = store.find_delivery("i-1"), store.find_delivery("i-2")
        assert sent is not None
        assert pending is not None
        assert (sent.status, sent.payload) == ("sent", {})
        assert pending.payload == PAYLOAD
        with pytest.raises(ValueError, match="nonnegative"):
            store.prune(-1)


def test_history_retention_deletes_only_old_final_items(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path, now_ms=1_000) as store:
        for key in ("old-sent", "old-rejected", "pending"):
            store.put(key)
        store.finish_attempt(store.begin_attempt("old-sent"), "sent", http_status=200)
        store.finish_attempt(
            store.begin_attempt("old-rejected"), "rejected", http_status=422
        )
    with OutboxStore(path, now_ms=1_000 + 20 * 86_400_000) as store:
        store.put("recent")
        store.finish_attempt(store.begin_attempt("recent"), "sent", http_status=200)
    with OutboxStore(path, now_ms=1_000 + 31 * 86_400_000) as store:
        assert store.purge(30 * 86_400) == 2
        assert store.find_delivery("old-sent") is None
        assert store.find_delivery("old-rejected") is None
        assert store.get("pending").status == "pending"
        assert store.get("recent").status == "sent"
        assert store.delivery_counts() == {"pending": 1, "sent": 1}


def test_delivered_item_is_sent_again_with_a_corrected_payload(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with OutboxStore(path, now_ms=1_000) as store:
        store.put("i-1")
        attempt = store.begin_attempt("i-1")
        store.finish_attempt(attempt, "sent", http_status=200)
        with pytest.raises(OutboxError, match="delivered"):
            store.requeue("i-2", {"corrected": True})
        store.requeue("i-1", {"corrected": True})
        assert store.get("i-1").status == "pending"
        assert not store.pending_deliveries()  # Nebuly may still process the first
    with OutboxStore(path, now_ms=62_000) as store:
        [delivery] = store.pending_deliveries()
        assert delivery.payload == {"corrected": True}
