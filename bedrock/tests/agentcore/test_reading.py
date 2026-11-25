"""Bounded reconciliation and targeted recovery planning."""

from datetime import UTC, datetime, timedelta

import pytest
from bedrock_sync.agentcore.reading import (
    datetime_to_epoch_ms,
    plan_reads,
    plan_trace_recovery,
)


def test_first_run_requires_start() -> None:
    with pytest.raises(ValueError, match="first-run"):
        plan_reads(
            coverage=None,
            requested_start_ms=None,
            end_ms=100,
            overlap_seconds=0,
            reconciliation_seconds=0,
        )


def test_new_late_trace_outside_overlap_is_reconciled() -> None:
    plan = plan_reads(
        coverage=(0, 200_000),
        requested_start_ms=None,
        end_ms=300_000,
        overlap_seconds=5,
        reconciliation_seconds=100,
    )
    assert (plan.primary.start_ms, plan.primary.end_ms) == (195_000, 300_000)
    assert plan.reconciliation is not None
    assert (plan.reconciliation.start_ms, plan.reconciliation.end_ms) == (
        100_000,
        195_000,
    )
    assert plan.reconciliation.start_ms <= 150_000 < plan.reconciliation.end_ms


def test_backfill_avoids_duplicate_reconciliation() -> None:
    plan = plan_reads(
        coverage=(1000, 200_000),
        requested_start_ms=0,
        end_ms=100_000,
        overlap_seconds=5,
        reconciliation_seconds=100,
    )
    assert plan.primary.start_ms == 0
    assert plan.reconciliation is None


def test_forward_gap_rejected() -> None:
    with pytest.raises(ValueError, match="gap"):
        plan_reads(
            coverage=(0, 100),
            requested_start_ms=101,
            end_ms=200,
            overlap_seconds=5,
            reconciliation_seconds=100,
        )


def test_epoch_conversion_avoids_float_rounding() -> None:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    assert datetime_to_epoch_ms(epoch + timedelta(microseconds=999)) == 0
    assert datetime_to_epoch_ms(epoch + timedelta(microseconds=1001)) == 1
    assert datetime_to_epoch_ms(epoch - timedelta(microseconds=1)) == -1


def test_newly_discovered_long_root_requires_prefix_even_with_final_chat() -> None:
    trace = "a" * 32
    # Both root and its last model call ended in this run's event-time window.
    # This does not establish that model calls ending before the window were read.
    root_start, root_end, last_chat_end = 100, 1200, 1190
    read_start_ms, end_ms = 1000, 2000
    assert read_start_ms <= root_end < end_ms
    assert read_start_ms <= last_chat_end < end_ms
    recovery = plan_trace_recovery(
        [(trace, root_start)], read_start_ms=read_start_ms, end_ms=end_ms
    )
    assert len(recovery) == 1
    assert (recovery[0].start_ms, recovery[0].end_ms, recovery[0].trace_id) == (
        100,
        1000,
        trace,
    )


def test_discovered_root_recovery_merges_starts_and_skips_scanned_prefixes() -> None:
    old, current = "a" * 32, "b" * 32
    recovery = plan_trace_recovery(
        [(old, 100), (old, 50), (current, 1000)], read_start_ms=1000, end_ms=2000
    )
    assert len(recovery) == 1
    assert recovery[0].start_ms == 50
    assert recovery[0].trace_id == old
