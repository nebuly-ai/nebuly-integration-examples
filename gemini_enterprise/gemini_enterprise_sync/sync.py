from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from time import perf_counter
from typing import TYPE_CHECKING

import httpx
from tqdm import tqdm

from .bigquery_client import BigQueryLoggingClient
from .converter import SkipReason, turn_to_payload
from .coverage import Coverage, plan_run
from .logging_client import LoggingClient, LogRecord
from .nebuly_client import NebulyClient
from .trace_client import TraceClient, TraceData

if TYPE_CHECKING:
    from datetime import datetime

    from .config import Config

logger = logging.getLogger(__name__)


class FirstRunRequiresFromDateError(RuntimeError):
    """Raised when the first sync run is attempted without --from-date."""


class GapAbortedError(RuntimeError):
    """Raised when a gap-creating run is declined or stdin is not interactive."""


@dataclass
class Counts:
    entries_fetched: int = 0
    entries_sent: int = 0
    entries_skipped: int = 0
    entries_failed: int = 0
    entries_conversion_errors: int = 0
    traces_missing_tokens: int = 0
    entries_deduplicated: int = 0


@dataclass
class SyncSummary:
    totals: Counts = field(default_factory=Counts)


def _log_summary(totals: Counts, *, stopped: bool) -> None:
    prefix = "Sync stopped" if stopped else "Sync complete"
    logger.info(
        "%s: entries_fetched=%s entries_sent=%s entries_skipped=%s "
        "entries_failed=%s entries_conversion_errors=%s traces_missing_tokens=%s "
        "entries_deduplicated=%s",
        prefix,
        totals.entries_fetched,
        totals.entries_sent,
        totals.entries_skipped,
        totals.entries_failed,
        totals.entries_conversion_errors,
        totals.traces_missing_tokens,
        totals.entries_deduplicated,
    )


def _resolve_requested_from(config: Config, coverage: Coverage) -> datetime:
    if config.from_date is not None:
        return config.from_date
    if coverage.state.coverage_until is not None:
        return coverage.state.coverage_until
    raise FirstRunRequiresFromDateError(
        "First run requires --from-date when no sync coverage exists in the cache"
    )


def _build_log_client(config: Config) -> LoggingClient | BigQueryLoggingClient:
    if config.log_source == "bigquery":
        return BigQueryLoggingClient(
            config.gcp_project_id,
            config.bigquery_table,  # type: ignore[arg-type]
            location=config.bigquery_location,
        )
    return LoggingClient(config.gcp_project_id, page_size=config.log_page_size)


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


def _process_batch(
    *,
    records: list[LogRecord],
    traces_data: dict[str, TraceData],
    config: Config,
    nebuly: NebulyClient,
    coverage: Coverage,
    summary: SyncSummary,
) -> bool:
    for record in tqdm(records, desc="Sending interactions"):
        trace_data = traces_data.get(record.trace_id) if record.trace_id else None
        if record.trace_id and trace_data is None:
            summary.totals.traces_missing_tokens += 1

        try:
            result = turn_to_payload(
                record,
                trace_data,
                engine_id=config.gcp_engine_id,
                anonymize=config.anonymize,
            )
        except Exception:
            logger.exception(
                "Failed to convert entry insert_id=%s trace_id=%s",
                record.insert_id,
                record.trace_id,
            )
            summary.totals.entries_conversion_errors += 1
            coverage.advance_until(record.timestamp, record.insert_id)
            continue

        if isinstance(result, SkipReason):
            logger.debug(
                "Skipping entry insert_id=%s reason=%s",
                record.insert_id,
                result.name,
            )
            summary.totals.entries_skipped += 1
            coverage.advance_until(record.timestamp, record.insert_id)
            continue

        try:
            nebuly.send_interaction(result)
        except Exception:
            logger.exception(
                "Failed to send entry insert_id=%s trace_id=%s",
                record.insert_id,
                record.trace_id,
            )
            summary.totals.entries_failed += 1
            return False
        summary.totals.entries_sent += 1
        coverage.advance_until(record.timestamp, record.insert_id)
    return True


def _generate_intervals(
    coverage: Coverage,
    requested_from: datetime,
    requested_until: datetime,
    config: Config,
) -> list[tuple[datetime, datetime]]:
    intervals, gap_detected = plan_run(
        coverage.state if coverage.has_coverage() else None,
        requested_from,
        requested_until,
    )

    if gap_detected:
        _confirm_gap(config)
        coverage.invalidate()
        intervals = [(requested_from, requested_until)]

    return intervals


def run_sync(config: Config) -> SyncSummary:  # noqa: C901, PLR0915
    logging.basicConfig(
        level=logging.DEBUG if config.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    summary = SyncSummary()
    config.cache_dir.mkdir(parents=True, exist_ok=True)
    coverage = Coverage(config.cache_dir, dry_run=config.dry_run)
    coverage.load()

    requested_until = config.run_until()
    requested_from = _resolve_requested_from(config, coverage)

    intervals = _generate_intervals(coverage, requested_from, requested_until, config)

    if not intervals:
        logger.info("Requested range is already covered; nothing to sync.")
        return summary

    logging_client = _build_log_client(config)
    trace_client = TraceClient(
        config.gcp_project_id,
        concurrency=config.trace_concurrency,
    )

    with httpx.Client(timeout=60.0) as http_client:
        nebuly = NebulyClient(
            http_client,
            config.nebuly_api_key,
            config.nebuly_endpoint,
            dry_run=config.dry_run,
        )

        for interval_from, interval_until in intervals:
            since = interval_from
            seen_ids: set[str] = set()
            if coverage.state.coverage_until == interval_from:
                seen_ids = set(coverage.state.coverage_until_ids)

            while True:
                start_time = perf_counter()
                logger.info("Started fetching log records")
                records = logging_client.fetch_batch(
                    since=since, until=interval_until, limit=config.log_batch_size
                )
                if not records:
                    break
                end_time = perf_counter()
                logger.info(
                    "Fetched %d log records in %.2f seconds",
                    len(records),
                    end_time - start_time,
                )

                last_ts = records[-1].timestamp
                new_records = [r for r in records if r.insert_id not in seen_ids]
                deduped = len(records) - len(new_records)
                if deduped:
                    summary.totals.entries_deduplicated += deduped
                if not new_records:
                    if len(records) >= config.log_batch_size and last_ts == since:
                        logger.warning(
                            "Batch of %d records all share timestamp %s; "
                            "some may be skipped. Increase --batch-size / "
                            "GCP_LOG_BATCH_SIZE.",
                            len(records),
                            last_ts,
                        )
                    break

                summary.totals.entries_fetched += len(new_records)

                start_time = perf_counter()
                logger.info("Started fetching traces")
                traces_data = trace_client.fetch_traces(
                    {r.trace_id for r in new_records if r.trace_id}
                )
                end_time = perf_counter()
                logger.info(
                    "Fetched %d traces in %.2f seconds",
                    len(traces_data),
                    end_time - start_time,
                )

                if not _process_batch(
                    records=new_records,
                    traces_data=traces_data,
                    config=config,
                    nebuly=nebuly,
                    coverage=coverage,
                    summary=summary,
                ):
                    _log_summary(summary.totals, stopped=True)
                    return summary

                seen_ids = (
                    {r.insert_id for r in records if r.timestamp == last_ts}
                    if last_ts != since
                    else seen_ids
                    | {r.insert_id for r in records if r.timestamp == last_ts}
                )
                since = last_ts
                if len(records) < config.log_batch_size:
                    break

            coverage.save(coverage_from=interval_from)

    _log_summary(summary.totals, stopped=False)
    return summary
