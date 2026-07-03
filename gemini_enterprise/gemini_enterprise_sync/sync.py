from __future__ import annotations

import logging
from dataclasses import dataclass, field
from time import perf_counter
from typing import TYPE_CHECKING

import httpx
from tqdm import tqdm

from .converter import SkipReason, turn_to_payload
from .cursor import Cursor
from .logging_client import LoggingClient
from .nebuly_client import NebulyClient
from .trace_client import TraceClient

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


@dataclass
class Counts:
    entries_fetched: int = 0
    entries_sent: int = 0
    entries_skipped: int = 0
    entries_held_back: int = 0
    entries_failed: int = 0
    traces_missing_tokens: int = 0


@dataclass
class SyncSummary:
    totals: Counts = field(default_factory=Counts)


def run_sync(config: Config) -> SyncSummary:
    logging.basicConfig(
        level=logging.DEBUG if config.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    summary = SyncSummary()
    config.cache_dir.mkdir(parents=True, exist_ok=True)
    cursor = Cursor(config.cache_dir / "cursor.json", dry_run=config.dry_run)
    state = cursor.load()
    since = config.from_date or state.last_timestamp
    until = config.run_until()

    logging_client = LoggingClient(config.gcp_project_id)
    trace_client = TraceClient(
        config.gcp_project_id, max_workers=config.trace_max_workers
    )

    with httpx.Client(timeout=60.0) as http_client:
        nebuly = NebulyClient(
            http_client,
            config.nebuly_api_key,
            config.nebuly_endpoint,
            dry_run=config.dry_run,
        )

        while True:
            start_time = perf_counter()
            logger.info("Started fetching log records")
            records = logging_client.fetch_batch(
                since=since, until=until, limit=config.log_batch_size
            )
            if not records:
                break
            end_time = perf_counter()
            logger.info(
                "Fetched %d log records in %.2f seconds",
                len(records),
                end_time - start_time,
            )

            summary.totals.entries_fetched += len(records)

            start_time = perf_counter()
            logger.info("Started fetching traces")
            traces_data = trace_client.fetch_tokens(
                {r.trace_id for r in records if r.trace_id}
            )
            end_time = perf_counter()
            logger.info(
                "Fetched %d traces in %.2f seconds",
                len(traces_data),
                end_time - start_time,
            )

            for record in tqdm(records, desc="Sending interactions"):
                trace_data = (
                    traces_data.get(record.trace_id) if record.trace_id else None
                )
                if record.trace_id and trace_data is None:
                    summary.totals.traces_missing_tokens += 1

                result = turn_to_payload(
                    record,
                    trace_data,
                    engine_id=config.gcp_engine_id,
                    anonymize=config.anonymize,
                )
                if isinstance(result, SkipReason):
                    summary.totals.entries_skipped += 1
                    cursor.advance(
                        timestamp=record.timestamp, insert_id=record.insert_id
                    )
                    continue

                try:
                    nebuly.send_interaction(result)
                    summary.totals.entries_sent += 1
                    cursor.advance(
                        timestamp=record.timestamp, insert_id=record.insert_id
                    )
                except Exception:
                    logger.exception(
                        "Failed to send entry insert_id=%s trace_id=%s",
                        record.insert_id,
                        record.trace_id,
                    )
                    summary.totals.entries_failed += 1
                    totals = summary.totals
                    logger.info(
                        "Sync stopped: entries_fetched=%s entries_sent=%s "
                        "entries_skipped=%s entries_held_back=%s entries_failed=%s "
                        "traces_missing_tokens=%s",
                        totals.entries_fetched,
                        totals.entries_sent,
                        totals.entries_skipped,
                        totals.entries_held_back,
                        totals.entries_failed,
                        totals.traces_missing_tokens,
                    )
                    return summary

            since = cursor.state.last_timestamp
            if len(records) < config.log_batch_size:
                break

    totals = summary.totals
    logger.info(
        "Sync complete: entries_fetched=%s entries_sent=%s entries_skipped=%s "
        "entries_held_back=%s entries_failed=%s traces_missing_tokens=%s",
        totals.entries_fetched,
        totals.entries_sent,
        totals.entries_skipped,
        totals.entries_held_back,
        totals.entries_failed,
        totals.traces_missing_tokens,
    )
    return summary
