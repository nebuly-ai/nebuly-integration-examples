"""Which CloudWatch time windows a run reads. No I/O here."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class ReadWindow:
    start_ms: int
    end_ms: int
    trace_id: str | None = None


@dataclass(frozen=True)
class ReadPlan:
    primary: ReadWindow
    reconciliation: ReadWindow | None

    @property
    def windows(self) -> tuple[ReadWindow, ...]:
        before = (self.reconciliation,) if self.reconciliation is not None else ()
        return (*before, self.primary)


def datetime_to_epoch_ms(value: datetime) -> int:
    """Floor UTC time using integer arithmetic, without float boundary rounding."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def plan_reads(
    *,
    coverage: tuple[int, int] | None,
    requested_start_ms: int | None,
    end_ms: int,
    overlap_seconds: int,
    reconciliation_seconds: int,
) -> ReadPlan:
    """The primary window (from the coverage end minus the overlap, or from
    ``--from-date``) and, when due, a reconciliation window before it.

    Reconciliation looks behind the previous coverage end, not the current
    clock: a daily run must still read the previous day's late events. It is
    bounded, so an event later than the horizon is not found; traces already
    known are read back from their own start by ``plan_trace_recovery``.
    """
    if min(end_ms, overlap_seconds, reconciliation_seconds) < 0:
        raise ValueError("Read times and lookbacks must be nonnegative")
    if coverage is None:
        if requested_start_ms is None:
            raise ValueError("--from-date is required for first-run coverage")
        start_ms = requested_start_ms
        reconcile = None
    else:
        coverage_start, coverage_end = coverage
        if coverage_start < 0 or coverage_end <= coverage_start:
            raise ValueError("Invalid stored coverage")
        if requested_start_ms is not None and requested_start_ms > coverage_end:
            raise ValueError("Requested start would create a coverage gap")
        start_ms = (
            requested_start_ms
            if requested_start_ms is not None
            else max(coverage_start, coverage_end - overlap_seconds * 1000)
        )
        reconcile_start = max(
            coverage_start, coverage_end - reconciliation_seconds * 1000
        )
        reconcile_end = min(start_ms, end_ms)
        reconcile = (
            ReadWindow(reconcile_start, reconcile_end)
            if reconcile_start < reconcile_end
            else None
        )
    if start_ms < 0 or start_ms >= end_ms:
        raise ValueError("Expected a nonempty read window")
    return ReadPlan(primary=ReadWindow(start_ms, end_ms), reconciliation=reconcile)


def plan_trace_recovery(
    discovered: Iterable[tuple[str, int]], *, read_start_ms: int, end_ms: int
) -> tuple[ReadWindow, ...]:
    """Recover unscanned operation prefixes for every newly discovered root.

    Call this after parsing initial pages and before final readiness decisions,
    even for roots that appear complete: their last model call may be present
    while earlier calls ended before the bounded read window. Pass operation
    START timestamps, not CloudWatch event timestamps (which approximate END).
    The same helper accepts persisted pending traces. Recovery is source-scoped
    by the reader/parser and does not expand the global coverage watermark.

    ``read_start_ms`` is the earliest boundary of the contiguous range actually
    read this run, not merely historical coverage: delayed old events can be
    absent from a range that was scanned in an earlier run.
    """
    if read_start_ms < 0 or end_ms <= read_start_ms:
        raise ValueError("Expected a nonnegative, nonempty completed read range")
    earliest_by_trace: dict[str, int] = {}
    for trace_id, earliest_ms in discovered:
        if len(trace_id) != 32 or any(
            char not in "0123456789abcdefABCDEF" for char in trace_id
        ):
            raise ValueError("Expected a 32-character hexadecimal trace ID")
        if earliest_ms < 0 or earliest_ms >= end_ms:
            raise ValueError("Invalid discovered trace operation start")
        earliest_by_trace[trace_id] = min(
            earliest_by_trace.get(trace_id, earliest_ms), earliest_ms
        )
    return tuple(
        ReadWindow(earliest, read_start_ms, trace)
        for trace, earliest in sorted(earliest_by_trace.items())
        if earliest < read_start_ms
    )
