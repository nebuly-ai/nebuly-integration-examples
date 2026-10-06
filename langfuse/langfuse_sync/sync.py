from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

import httpx

from langfuse_sync.config import (
    Config,
    datetime_to_timestamp_str,
    timestamp_str_to_datetime,
)
from langfuse_sync.converter import interaction_from_langfuse_trace
from langfuse_sync.coverage import Coverage, CoverageState, plan_run
from langfuse_sync.http_retry import RateLimiter
from langfuse_sync.langfuse_client import LangfuseClient
from langfuse_sync.nebuly_client import NebulyClient, SendResult

if TYPE_CHECKING:
    from langfuse_sync.models import LangfuseTrace

logger = logging.getLogger(__name__)


class FirstRunRequiresFromDateError(RuntimeError):
    """Raised when the first sync run is attempted without --from-date."""


class GapAbortedError(RuntimeError):
    """Raised when a gap-creating run is declined or stdin is not interactive."""


@dataclass
class Counts:
    traces_fetched: int = 0
    traces_sent: int = 0
    traces_skipped: int = 0
    traces_failed: int = 0
    traces_deduplicated: int = 0


@dataclass
class SyncSummary:
    totals: Counts = field(default_factory=Counts)


class _Outcome(Enum):
    SENT = "sent"
    TOO_LARGE = "too_large"
    SKIPPED = "skipped"
    FAILED = "failed"


_COMMITTABLE = frozenset({_Outcome.SENT, _Outcome.TOO_LARGE, _Outcome.SKIPPED})


@dataclass(frozen=True)
class _PendingTrace:
    """A deduplicated trace; `payload` is None when the converter skipped it."""

    trace_id: str
    timestamp: datetime
    timestamp_raw: str
    payload: dict[str, Any] | None


def _log_summary(totals: Counts, *, stopped: bool) -> None:
    prefix = "Sync stopped" if stopped else "Sync complete"
    logger.info(
        "%s: traces_fetched=%s traces_sent=%s traces_skipped=%s "
        "traces_failed=%s traces_deduplicated=%s",
        prefix,
        totals.traces_fetched,
        totals.traces_sent,
        totals.traces_skipped,
        totals.traces_failed,
        totals.traces_deduplicated,
    )


def _resolve_requested_from(config: Config, coverage: Coverage) -> datetime:
    if config.from_date is not None:
        return config.from_date
    if coverage.state.coverage_until is not None:
        return coverage.state.coverage_until
    raise FirstRunRequiresFromDateError(
        "First run requires --from-date when no sync coverage exists in the cache"
    )


def _confirm_gap(config: Config) -> bool:
    if config.force:
        return True
    logger.warning(
        "Requested range is disjoint from existing coverage. "
        "Continuing will invalidate the cache and reset coverage."
    )
    if not sys.stdin.isatty():
        raise GapAbortedError(
            "Gap detected but stdin is not interactive. "
            "Re-run with --yes to invalidate coverage and proceed."
        )
    answer = input("Invalidate coverage and proceed? [y/N]: ").strip().lower()
    if answer not in {"y", "yes"}:
        raise GapAbortedError("Gap-creating run aborted by operator.")
    return True


def _iter_day_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=1), end)
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks


def _trace_sort_key(trace: LangfuseTrace) -> tuple[str, str]:
    return (str(trace.get("timestamp") or ""), str(trace.get("id") or ""))


def _dedupe_traces(
    traces: list[LangfuseTrace],
    *,
    boundary_until: datetime | None,
    boundary_ids: frozenset[str],
) -> tuple[list[LangfuseTrace], int]:
    seen: set[str] = set()
    unique: list[LangfuseTrace] = []
    deduped = 0

    for trace in sorted(traces, key=_trace_sort_key):
        trace_id = trace.get("id")
        if not trace_id or trace_id in seen:
            if trace_id:
                deduped += 1
            continue
        seen.add(trace_id)

        trace_ts_raw = trace.get("timestamp")
        if (
            boundary_until is not None
            and trace_ts_raw
            and trace_id in boundary_ids
            and timestamp_str_to_datetime(trace_ts_raw) == boundary_until
        ):
            deduped += 1
            continue

        unique.append(trace)
    return unique, deduped


def _interval_kind(
    prior: CoverageState,
    interval_from: datetime,
    interval_until: datetime,
) -> Literal["backfill", "tail", "full"]:
    if prior.coverage_from is None and prior.coverage_until is None:
        return "full"
    cov_from = prior.coverage_from
    cov_until = prior.coverage_until
    if cov_from is not None and interval_until <= cov_from:
        return "backfill"
    if cov_from is not None and interval_from < cov_from:
        return "backfill"
    if cov_until is not None and interval_from >= cov_until:
        return "tail"
    if (
        cov_from is None
        and cov_until is not None
        and interval_from < cov_until
        and interval_until <= cov_until
    ):
        return "backfill"
    return "tail"


def _finalize_interval(
    coverage: Coverage,
    *,
    kind: Literal["backfill", "tail", "full"],
    interval_from: datetime,
    interval_until: datetime,
    processed_any: bool,
) -> None:
    if kind == "backfill":
        coverage.save(coverage_from=interval_from)
        return
    if kind == "full":
        if not processed_any:
            coverage.save(
                coverage_from=interval_from,
                coverage_until=interval_until,
                coverage_until_ids=frozenset(),
            )
            return
        coverage.save(
            coverage_from=interval_from,
            coverage_until=interval_until,
            coverage_until_ids=coverage.state.coverage_until_ids,
        )
        return
    if not processed_any:
        coverage.save(
            coverage_until=interval_until,
            coverage_until_ids=frozenset(),
        )
        return
    coverage.save(
        coverage_until=interval_until,
        coverage_until_ids=coverage.state.coverage_until_ids,
    )


def _init_outcomes(
    items: list[_PendingTrace], summary: SyncSummary
) -> tuple[list[_Outcome | None], list[tuple[int, dict[str, Any]]]]:
    """Pre-fill converter-skipped items and list the (index, payload) to send."""
    outcomes: list[_Outcome | None] = []
    to_send: list[tuple[int, dict[str, Any]]] = []
    for index, item in enumerate(items):
        if item.payload is None:
            summary.totals.traces_skipped += 1
            outcomes.append(_Outcome.SKIPPED)
        else:
            outcomes.append(None)
            to_send.append((index, item.payload))
    return outcomes, to_send


async def _send_chunk(
    items: list[_PendingTrace],
    *,
    nebuly: NebulyClient,
    coverage: Coverage,
    summary: SyncSummary,
    concurrency: int,
) -> bool:
    """Send items concurrently while keeping the watermark strictly ordered.

    Coverage only advances through the contiguous prefix of items that are
    already done, so a failure never leaves an unsent trace behind the
    watermark. Traces that were in flight when a worker failed may be re-sent
    by the next run.
    """
    outcomes, to_send = _init_outcomes(items, summary)
    next_commit = 0

    def commit_prefix() -> None:
        nonlocal next_commit
        while next_commit < len(items) and outcomes[next_commit] in _COMMITTABLE:
            item = items[next_commit]
            coverage.advance_until(item.timestamp, item.trace_id)
            next_commit += 1

    commit_prefix()

    pending = iter(to_send)
    stop = asyncio.Event()

    async def worker() -> None:
        for index, payload in pending:
            if stop.is_set():
                return
            item = items[index]
            try:
                result = await nebuly.send_interaction(payload)
            except Exception:
                logger.exception(
                    "Failed to send trace id=%s timestamp=%s",
                    item.trace_id,
                    item.timestamp_raw,
                )
                summary.totals.traces_failed += 1
                outcomes[index] = _Outcome.FAILED
                stop.set()
                return
            if result == SendResult.SENT:
                summary.totals.traces_sent += 1
                outcomes[index] = _Outcome.SENT
            else:
                summary.totals.traces_skipped += 1
                outcomes[index] = _Outcome.TOO_LARGE
            commit_prefix()

    async with asyncio.TaskGroup() as group:
        for _ in range(min(concurrency, len(to_send))):
            group.create_task(worker())

    return _Outcome.FAILED not in outcomes


async def _process_chunk(
    *,
    chunk_start: datetime,
    chunk_end: datetime,
    config: Config,
    langfuse: LangfuseClient,
    nebuly: NebulyClient,
    coverage: Coverage,
    summary: SyncSummary,
    boundary_until: datetime | None,
    boundary_ids: frozenset[str],
) -> bool:
    now = datetime.now(UTC)
    obs_start = chunk_start - timedelta(minutes=1)
    obs_end = min(
        chunk_end + timedelta(seconds=config.settle_lag_seconds),
        now,
    )

    logger.info(
        "Fetching Langfuse data for chunk %s → %s",
        datetime_to_timestamp_str(chunk_start),
        datetime_to_timestamp_str(chunk_end),
    )
    traces, observations_by_trace = await asyncio.gather(
        langfuse.get_traces(chunk_start, chunk_end),
        langfuse.get_observations_by_trace_id(obs_start, obs_end),
    )
    logger.info("Fetched %d traces", len(traces))
    traces_to_send, deduped = _dedupe_traces(
        traces,
        boundary_until=boundary_until,
        boundary_ids=boundary_ids,
    )
    if deduped:
        summary.totals.traces_deduplicated += deduped
    summary.totals.traces_fetched += len(traces_to_send)

    if not traces_to_send:
        logger.info("Chunk empty after dedup; advancing")
        return True

    logger.info(
        "Sending %d traces for chunk (%d deduplicated)",
        len(traces_to_send),
        deduped,
    )

    items: list[_PendingTrace] = []
    for trace in traces_to_send:
        trace_id = trace.get("id")
        if not trace_id:
            continue
        trace_ts_raw = trace.get("timestamp")
        if not trace_ts_raw:
            continue

        observations = observations_by_trace.get(trace_id, [])
        interaction = interaction_from_langfuse_trace(trace, observations)
        payload = (
            None
            if interaction is None
            else {
                "interaction": interaction.to_interaction_dict(),
                "traces": [item.to_dict() for item in interaction.traces],
                "user_feedback": [],
                "anonymize": config.anonymize,
            }
        )
        items.append(
            _PendingTrace(
                trace_id=trace_id,
                timestamp=timestamp_str_to_datetime(trace_ts_raw),
                timestamp_raw=trace_ts_raw,
                payload=payload,
            )
        )

    ok = await _send_chunk(
        items,
        nebuly=nebuly,
        coverage=coverage,
        summary=summary,
        concurrency=config.max_concurrency,
    )
    if not ok:
        return False

    logger.info(
        "Chunk complete (running totals: sent=%d skipped=%d failed=%d)",
        summary.totals.traces_sent,
        summary.totals.traces_skipped,
        summary.totals.traces_failed,
    )
    return True


async def _run_intervals(
    config: Config,
    coverage: Coverage,
    prior_state: CoverageState,
    intervals: list[tuple[datetime, datetime]],
    summary: SyncSummary,
) -> int:
    async with httpx.AsyncClient(timeout=60.0) as http_client:
        langfuse = LangfuseClient(
            http_client, config, RateLimiter(config.max_concurrency)
        )
        nebuly = NebulyClient(
            http_client,
            config.nebuly_api_key,
            config.nebuly_endpoint,
            RateLimiter(config.max_concurrency),
            dry_run=config.dry_run,
        )

        for interval_from, interval_until in intervals:
            kind = _interval_kind(prior_state, interval_from, interval_until)
            boundary_until = (
                coverage.state.coverage_until
                if coverage.state.coverage_until == interval_from
                else None
            )
            boundary_ids = (
                coverage.state.coverage_until_ids
                if boundary_until is not None
                else frozenset()
            )
            processed_any = False
            day_chunks = _iter_day_chunks(interval_from, interval_until)
            logger.info(
                "Interval %s → %s (%d day chunk(s), kind=%s)",
                datetime_to_timestamp_str(interval_from),
                datetime_to_timestamp_str(interval_until),
                len(day_chunks),
                kind,
            )

            for chunk_index, (chunk_start, chunk_end) in enumerate(day_chunks, start=1):
                logger.info(
                    "Day chunk %d/%d",
                    chunk_index,
                    len(day_chunks),
                )
                ok = await _process_chunk(
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    config=config,
                    langfuse=langfuse,
                    nebuly=nebuly,
                    coverage=coverage,
                    summary=summary,
                    boundary_until=boundary_until,
                    boundary_ids=boundary_ids,
                )
                if not ok:
                    _log_summary(summary.totals, stopped=True)
                    return 1
                if summary.totals.traces_sent > 0 or summary.totals.traces_skipped > 0:
                    processed_any = True
                boundary_until = coverage.state.coverage_until
                boundary_ids = coverage.state.coverage_until_ids

            _finalize_interval(
                coverage,
                kind=kind,
                interval_from=interval_from,
                interval_until=interval_until,
                processed_any=processed_any,
            )

    _log_summary(summary.totals, stopped=False)
    return 0


def run_sync(config: Config) -> int:
    logging.basicConfig(
        level=logging.DEBUG if config.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    summary = SyncSummary()
    config.cache_dir.mkdir(parents=True, exist_ok=True)
    coverage = Coverage(config.cache_dir, dry_run=config.dry_run)
    prior_state = coverage.load()

    requested_until = config.run_until()
    requested_from = _resolve_requested_from(config, coverage)

    intervals, gap_detected = plan_run(
        prior_state if coverage.has_coverage() else None,
        requested_from,
        requested_until,
    )
    if gap_detected:
        _confirm_gap(config)
        coverage.invalidate()
        intervals = [(requested_from, requested_until)]

    if not intervals:
        logger.info("Requested range is already covered; nothing to sync.")
        _log_summary(summary.totals, stopped=False)
        return 0

    logger.info(
        "Planned sync %s → %s (%d interval(s))",
        datetime_to_timestamp_str(requested_from),
        datetime_to_timestamp_str(requested_until),
        len(intervals),
    )

    return asyncio.run(
        _run_intervals(config, coverage, prior_state, intervals, summary)
    )
