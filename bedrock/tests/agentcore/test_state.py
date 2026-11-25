"""What the AgentCore state adds to the shared store (tests/shared/test_store.py)."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from bedrock_sync.shared.delivery import Delivery

import pytest
from bedrock_sync.agentcore.records import LogPage, RawLogRecord, SourceScope
from bedrock_sync.agentcore.state import AgentCoreState, StateError

SOURCE = SourceScope("123456789012", "eu-north-1", "synthetic-source")
GROUPS = ("/test/runtime", "aws/spans")


def record(event_id: str = "event-1", group: str = GROUPS[0]) -> RawLogRecord:
    return RawLogRecord(group, "otel", event_id, 110, 120, '{"synthetic":true}')


def page(group: str, *records: RawLogRecord, complete: bool = True) -> LogPage:
    return LogPage(group, 100, 200, records, complete)


def delivery(state: AgentCoreState, key: str) -> Delivery:
    found = state.find_delivery(key)
    assert found is not None
    return found


def test_coverage_requires_all_groups_and_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        assert state.ingest_page(window, page(GROUPS[0], record())) == 1
        assert state.coverage is None
        with pytest.raises(StateError, match="every configured"):
            state.complete_window(window)
        state.ingest_page(window, page(GROUPS[1]))
        state.complete_window(window)
        state.complete_window(window)  # Idempotent finalization.
        assert state.coverage == (100, 200)
        with pytest.raises(StateError, match="already complete"):
            state.ingest_page(window, page(GROUPS[0]))
    with AgentCoreState(path, SOURCE, tuple(reversed(GROUPS))) as state:
        assert state.coverage == (100, 200)
        assert state.records() == (record(),)


def test_crash_replay_deduplicates_and_preserves_partial_pages(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record(), complete=False))
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        assert state.coverage is None
        window = state.begin_window(100, 200)
        assert state.ingest_page(window, page(GROUPS[0], record())) == 0
        assert state.records() == (record(),)


def test_page_failure_rolls_back_every_record_and_completion(tmp_path: Path) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        invalid = replace(record("bad"), timestamp_ms=200)
        with pytest.raises(StateError, match="outside"):
            state.ingest_page(window, page(GROUPS[0], record(), invalid))
        assert state.records() == ()
        assert state.ingest_page(window, page(GROUPS[0], record())) == 1


def test_record_conflicts_do_not_silently_overwrite(tmp_path: Path) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record(), complete=False))
        with pytest.raises(StateError, match="Conflicting"):
            state.ingest_page(
                window, page(GROUPS[0], replace(record(), message="different"))
            )
        assert state.records() == (record(),)


def test_same_event_id_in_different_groups_is_not_a_duplicate(tmp_path: Path) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        for group in GROUPS:
            assert state.ingest_page(window, page(group, record(group=group))) == 1
        assert len(state.records()) == 2


@pytest.mark.parametrize("bad", [(99, 200), (100, 201)])
def test_wrong_page_window_is_rejected(tmp_path: Path, bad: tuple[int, int]) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        with pytest.raises(StateError, match="does not belong"):
            state.ingest_page(window, LogPage(GROUPS[0], *bad, (), complete=True))


def test_backfill_extends_coverage_without_moving_watermark_back(
    tmp_path: Path,
) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, (GROUPS[0],)) as state:
        for start, end in [(100, 200), (150, 300), (0, 100), (50, 150)]:
            window = state.begin_window(start, end)
            state.ingest_page(window, LogPage(GROUPS[0], start, end, (), complete=True))
            state.complete_window(window)
        assert state.coverage == (0, 300)
        with pytest.raises(StateError, match="gap"):
            state.begin_window(301, 400)
        with pytest.raises(ValueError, match="positive"):
            state.begin_window(300, 300)


@pytest.mark.parametrize("change", ["source", "groups"])
def test_database_binding_is_strict(tmp_path: Path, change: str) -> None:
    path = tmp_path / "state.db"
    with AgentCoreState(path, SOURCE, GROUPS):
        pass
    source = replace(SOURCE, source_id="other") if change == "source" else SOURCE
    groups = (GROUPS[0],) if change == "groups" else GROUPS
    with pytest.raises(StateError, match="configuration differs"):
        AgentCoreState(path, source, groups)
    # Constructor failure must release the lock.
    with AgentCoreState(path, SOURCE, GROUPS):
        pass


def test_processed_records_and_pending_traces_persist(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record()))
        state.mark_processed(record(), trace_id="trace")
        state.save_trace("trace", "open", 110, {"input": "private"})
        state.save_trace("trace", "blocked", 120, {"diagnostics": ["missing_actor"]})
        state.save_trace("done", "ready", 100, {})
        state.save_trace("other-actor", "dropped", 100, {})
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        assert state.records(unprocessed_only=True) == ()
        assert state.records(trace_id="trace") == (record(),)
        [pending] = state.pending_traces()  # dropped traces are not pending
        assert (pending.trace_id, pending.status) == ("trace", "blocked")
        assert pending.earliest_ms == 110
        assert pending.snapshot == {"diagnostics": ["missing_actor"]}
        assert "missing_actor" not in repr(pending)
        with pytest.raises(StateError, match="not been ingested"):
            state.mark_processed(record("absent"), trace_id=None)


def test_unsupported_records_are_parsed_again_when_the_namespace_changes(
    tmp_path: Path,
) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS) as state:
        state.configure_parser(("/actors/{actorId}/",))
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record(), record("event-2")))
        state.mark_processed(record(), trace_id="trace", unsupported=True)
        state.mark_processed(record("event-2"), trace_id="trace")
        state.configure_parser(("/actors/{actorId}/",))
        assert state.records(unprocessed_only=True) == ()
        state.configure_parser(("/actors/{actorId}/", "/facts/{actorId}/"))
        assert state.records(unprocessed_only=True) == (record(),)


def test_dry_run_reads_and_ingests_on_a_copy(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with AgentCoreState(path, SOURCE, GROUPS):
        pass
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with AgentCoreState(path, SOURCE, GROUPS, dry_run=True) as state:
        state.put_payload("other", "root", {})
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record()))
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_recovery_pages_preserve_coverage_and_support_incremental_trace_reads(
    tmp_path: Path,
) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, (GROUPS[0],)) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record()))
        state.complete_window(window)
        earlier = replace(record("earlier"), timestamp_ms=10)
        recovery = LogPage(GROUPS[0], 0, 100, (earlier,), complete=True)
        assert state.ingest_recovery_page(recovery) == 1
        assert state.coverage == (100, 200)
        assert len(state.records(unprocessed_only=True)) == 2
        state.mark_processed(earlier, trace_id="trace-old")
        assert state.records(trace_id="trace-old") == (earlier,)
        assert state.records(unprocessed_only=True) == (record(),)
        assert state.records(trace_id="trace-old", unprocessed_only=True) == ()
        with pytest.raises(StateError, match="invalid group"):
            state.ingest_recovery_page(replace(recovery, log_group="foreign"))


@pytest.mark.parametrize("invalid", [-1, True, 1.5])
def test_invalid_timestamps_are_rejected_at_persistence_boundary(
    tmp_path: Path, invalid: int
) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS) as state:
        with pytest.raises(ValueError, match="integer milliseconds"):
            state.begin_window(invalid, 200)
        with pytest.raises(ValueError, match="integer milliseconds"):
            state.save_trace("trace", "open", invalid, {})
        window = state.begin_window(100, 200)
        malformed = replace(record(), ingestion_time_ms=invalid)
        with pytest.raises(ValueError, match="integer milliseconds"):
            state.ingest_page(window, page(GROUPS[0], malformed))
        assert state.records() == ()


def test_blank_log_group_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="nonblank"):
        AgentCoreState(tmp_path / "state.db", SOURCE, (" ",))


def test_filtered_trace_pages_never_establish_unfiltered_coverage(
    tmp_path: Path,
) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        filtered = replace(page(GROUPS[0], record()), trace_id="trace")
        with pytest.raises(StateError, match="cannot establish coverage"):
            state.ingest_page(window, filtered)
        assert state.coverage is None
        assert state.records() == ()
        assert state.ingest_recovery_page(filtered) == 1
        assert state.coverage is None


def test_ready_snapshot_is_recoverable_after_crash_before_outbox_creation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    snapshot = {"root_span_id": "root", "input": "synthetic input"}
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record()))
        state.mark_processed(record(), trace_id="trace")
        state.save_trace("trace", "ready", 100, snapshot)
        state.save_trace("waiting", "open", 90, {})
        # A process can stop here before the converter freezes an outbox payload.
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        assert state.records(unprocessed_only=True) == ()
        assert state.pending_deliveries() == ()
        ready = state.ready_traces()
        assert len(ready) == 1
        assert ready[0].trace_id == "trace"
        assert ready[0].status == "ready"
        assert ready[0].snapshot == snapshot
        assert ready[0].earliest_ms == 100
        key = state.put_payload(
            "trace", str(ready[0].snapshot["root_span_id"]), {"output": "synthetic"}
        )
        attempt = state.begin_attempt(key)
        state.finish_attempt(attempt, "sent", http_status=200)
    with AgentCoreState(path, SOURCE, GROUPS) as state:
        # Retaining assembly history cannot reset or replay confirmed delivery.
        assert len(state.ready_traces()) == 1
        assert delivery(state, key).status == "sent"
        assert state.pending_deliveries() == ()
        with pytest.raises(StateError, match="Only pending"):
            state.begin_attempt(key)


@pytest.mark.parametrize(("outcome", "status"), [("sent", 200), ("rejected", 422)])
def test_content_retention_erases_all_copies_but_preserves_dedup(
    tmp_path: Path,
    outcome: str,
    status: int,
) -> None:
    path = tmp_path / "retained.db"
    marker = "private-content-retention-check-unique"
    raw = replace(record(), message=json.dumps({"text": marker}))
    with AgentCoreState(path, SOURCE, (GROUPS[0],), now_ms=1000) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], raw))
        state.complete_window(window)
        state.mark_processed(raw, trace_id="trace")
        state.save_trace("trace", "ready", 100, {"input": marker, "end_ns": 200000000})
        key = state.put_payload("trace", "root", {"input": marker})
        state.finish_attempt(state.begin_attempt(key), outcome, http_status=status)  # type: ignore[arg-type]
        state.now_ms = 60999
        assert state.prune_content(60) == 0
        state.now_ms = 61000
        assert state.prune_content(60) == 1
        assert delivery(state, key).payload == {}
        compacted = state.trace("trace")
        assert compacted is not None
        assert compacted.snapshot == {"compacted": True}
        assert not state.records()
        window = state.begin_window(100, 200)
        assert state.ingest_page(window, page(GROUPS[0], raw)) == 0
        state.complete_window(window)
        assert delivery(state, key).status == outcome
        window = state.begin_window(100, 200)
        with pytest.raises(StateError, match="compacted record"):
            state.ingest_page(
                window, page(GROUPS[0], replace(raw, message="different"))
            )
    assert marker.encode() not in path.read_bytes()
    with AgentCoreState(path, SOURCE, (GROUPS[0],), now_ms=62000) as state:
        assert not state.pending_deliveries()
        assert state.is_compacted("trace")
        assert state.coverage == (100, 200)


@pytest.mark.parametrize("outcome", ["pending", "retried"])
def test_retention_preserves_unresolved_delivery_content(
    tmp_path: Path, outcome: str
) -> None:
    with AgentCoreState(
        tmp_path / "state.db", SOURCE, (GROUPS[0],), now_ms=1000
    ) as state:
        state.save_trace("trace", "ready", 100, {"input": "private"})
        key = state.put_payload("trace", "root", {"input": "private"})
        if outcome == "retried":
            state.finish_attempt(state.begin_attempt(key), "retryable", http_status=503)
        state.now_ms = 10000000
        state.prune_content(0)
        assert delivery(state, key).payload == {"input": "private"}
        assert not state.is_compacted("trace")


def test_trace_retention_waits_for_latest_delivery_to_age(tmp_path: Path) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS, now_ms=62_000) as state:
        state.save_trace("trace", "ready", 100, {"input": "private"})
        child = state.put_payload("trace", "child", {"input": "child"})
        state.finish_attempt(
            state.begin_attempt(child), "sent", http_status=200, finished_ms=1000
        )
        assert state.supersede_roots("trace", "root") == 1
        root = state.put_payload("trace", "root", {"input": "root"})
        state.finish_attempt(state.begin_attempt(root), "sent", http_status=200)
        state.prune_content(60)
        assert not state.is_compacted("trace")
        assert delivery(state, child).payload == {"input": "child"}
        assert delivery(state, root).payload == {"input": "root"}
        state.now_ms = 122_000
        state.prune_content(60)
        assert state.is_compacted("trace")
        assert delivery(state, child).payload == {}
        assert delivery(state, root).payload == {}


def test_dropped_trace_stays_dropped_when_a_delivery_is_old(tmp_path: Path) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS, now_ms=62_000) as state:
        state.save_trace("trace", "dropped", 100, {"expired": True})
        old = state.put_payload("trace", "old", {"input": "old"})
        state.finish_attempt(
            state.begin_attempt(old), "sent", http_status=200, finished_ms=1000
        )
        recent = state.put_payload("trace", "recent", {"input": "recent"})
        state.finish_attempt(state.begin_attempt(recent), "sent", http_status=200)
        state.prune_content(60)
        trace = state.trace("trace")
        assert trace is not None
        assert trace.status == "dropped"
        assert state.is_compacted("trace")
        assert delivery(state, old).payload == {}
        assert delivery(state, recent).payload == {"input": "recent"}
        state.now_ms = 122_000
        state.prune_content(60)
        assert delivery(state, recent).payload == {}


def test_old_unrelated_records_are_pruned_with_receipts(tmp_path: Path) -> None:
    with AgentCoreState(
        tmp_path / "state.db", SOURCE, (GROUPS[0],), now_ms=1000
    ) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record()))
        state.complete_window(window)
        state.mark_processed(record(), trace_id=None)
        assert state.prune_content(0) == 1
        window = state.begin_window(100, 200)
        assert state.ingest_page(window, page(GROUPS[0], record())) == 0


def _staged(state: AgentCoreState, trace_id: str) -> str:
    state.save_trace(trace_id, "ready", 100, {})
    return state.put_payload(trace_id, "root", {"synthetic": True})


def test_payload_of_an_expired_staged_trace_is_erased(tmp_path: Path) -> None:
    with AgentCoreState(tmp_path / "state.db", SOURCE, GROUPS, now_ms=61_000) as state:
        key = _staged(state, "t1")
        state.save_trace("t1", "blocked", 100, {})
        assert state.expire_traces(200) == (1, {"not_assembled": 1}, 0)
        state.prune_content(60)
        assert delivery(state, key).payload == {}
        assert state.pending_deliveries(ended_before_ns=2**62) == ()


def test_dropped_traces_are_erased_after_the_retention(tmp_path: Path) -> None:
    with AgentCoreState(
        tmp_path / "state.db", SOURCE, (GROUPS[0],), now_ms=1000
    ) as state:
        window = state.begin_window(100, 200)
        state.ingest_page(window, page(GROUPS[0], record()))
        state.mark_processed(record(), trace_id="trace")
        state.save_trace("trace", "dropped", 100, {"expired": True})
        assert state.prune_content(60) == 0  # 100 ms + 60 s is not past yet
        state.now_ms = 61_000
        assert state.prune_content(60) == 1
        assert state.records() == ()
        assert state.is_compacted("trace")
        trace = state.trace("trace")
        assert trace is not None
        assert (trace.status, trace.snapshot) == ("dropped", {"compacted": True})
