"""One sync run: read the new Bedrock log files, close interactions, deliver.

1. Plan the window ``since -> until`` from the cursor and the CLI flags.
2. Put back the calls of the interactions left open (read again from S3).
3. Read the log files in key order: parse, group by triplet, close.
4. At each new hour of files and at the end, checkpoint: cursor, open calls,
   closed interactions and their payloads, in one transaction.
5. Deliver the outbox with the rules shared with AgentCore, then compact the
   payloads older than the content retention.

A failure while reading stops the run before the next checkpoint: the next
run starts again from the last one and rebuilds the same interactions.
The command line (``__main__``) prints the returned summary as JSON.
"""

from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

from bedrock_sync.shared.delivery import DeliveryReport, SafeNebulyClient, deliver
from bedrock_sync.shared.errors import ConnectorError

from . import user_defined
from .converter import InteractionSkip, interaction_to_payload
from .grouping import Grouper, GroupingStats
from .models import Invocation
from .parser import parse_record
from .s3_client import LogSource, build_s3_store
from .state import QUEUED, Closure, Cursor, RuntimeState

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    import httpx

    from .config import Config
    from .grouping import Interaction
    from .s3_client import LogObject, LogRecord, ObjectStore

logger = logging.getLogger(__name__)

CONVERSION_ERROR = "conversion_error"


class FirstRunRequiresFromDateError(ConnectorError):
    """Raised when the first sync run is attempted without --from-date."""


class GapAbortedError(ConnectorError):
    """Raised when a gap-creating run is declined or stdin is not interactive."""


@dataclass
class Counts:
    files_read: int = 0
    records_fetched: int = 0
    records_skipped: dict[str, int] = field(default_factory=dict)
    """Records the parser could not use, by reason."""
    interactions_queued: int = 0
    interactions_skipped: dict[str, int] = field(default_factory=dict)
    """Closed interactions not queued, by reason."""
    conversion_errors: int = 0
    open_interactions: int = 0


def _count(counter: dict[str, int], reason: str) -> None:
    # A plain dict, not a Counter: dataclasses.asdict mangles Counter.
    counter[reason] = counter.get(reason, 0) + 1


@dataclass
class SyncSummary:
    totals: Counts = field(default_factory=Counts)
    grouping: GroupingStats = field(default_factory=GroupingStats)
    delivery: DeliveryReport = field(default_factory=DeliveryReport)
    outbox: dict[str, int] = field(default_factory=dict)

    @property
    def incomplete(self) -> bool:
        """Something closed was not confirmed by Nebuly, or waits for a retry."""
        delivery = self.delivery
        return bool(
            delivery.uncertain
            or delivery.rejected
            or delivery.retryable
            or delivery.paused
        )


@dataclass(frozen=True)
class _Window:
    since: datetime
    until: datetime
    coverage_from: datetime
    after_key: str | None


def _confirm_gap(config: Config) -> None:
    if config.force:
        return
    logger.warning(
        "Requested range is disjoint from existing coverage. "
        "Continuing will reset coverage and the open interactions."
    )
    if not sys.stdin.isatty():
        raise GapAbortedError(
            "Gap detected but stdin is not interactive. "
            "Re-run with --yes to invalidate coverage and proceed."
        )
    answer = input("Invalidate coverage and proceed? [y/N]: ").strip().lower()
    if answer not in {"y", "yes"}:
        raise GapAbortedError("Gap-creating run aborted by operator.")


def plan_window(config: Config, state: RuntimeState, until: datetime) -> _Window:
    """Where this run starts. May reset the cursor (a gap, or a backfill before
    the coverage); closed interactions are kept, so none is queued twice."""
    cursor = state.cursor()
    from_date = config.from_date
    if cursor is None:
        if from_date is None:
            raise FirstRunRequiresFromDateError(
                "The first run needs --from-date: the state file has no cursor yet"
            )
        return _Window(from_date, until, from_date, None)

    if from_date is not None and (
        from_date > cursor.coverage_until or until < cursor.coverage_from
    ):
        _confirm_gap(config)
        state.reset()
        return _Window(from_date, until, from_date, None)
    if from_date is not None and from_date < cursor.coverage_from:
        logger.warning(
            "--from-date %s is before the synced range (from %s): reading again "
            "from it; interactions already closed are skipped",
            from_date.isoformat(),
            cursor.coverage_from.isoformat(),
        )
        state.reset()
        return _Window(from_date, until, from_date, None)
    return _Window(cursor.coverage_until, until, cursor.coverage_from, cursor.last_key)


class _Run:
    def __init__(
        self,
        config: Config,
        state: RuntimeState,
        source: LogSource,
        summary: SyncSummary,
        context: Mapping[str, object],
    ) -> None:
        self._config = config
        self._state = state
        self._source = source
        self._totals = summary.totals
        self._context = context
        self.grouper = Grouper(
            idle=timedelta(minutes=config.interaction_idle_minutes),
            delivery_margin=timedelta(seconds=config.delivery_margin_seconds),
            closed_owner=state.closed_owner,
            conversation_owner=state.conversation_owner,
        )
        summary.grouping = self.grouper.stats

    def restore(self) -> None:
        records = self._source.read_requests(self._state.pending_calls())
        self.grouper.restore(
            parsed
            for record in records
            if isinstance(
                parsed := parse_record(record.record, source_key=record.key), Invocation
            )
        )

    def read(self, obj: LogObject, records: list[LogRecord]) -> None:
        self._totals.files_read += 1
        for record in records:
            self._totals.records_fetched += 1
            parsed = parse_record(record.record, source_key=record.key)
            if isinstance(parsed, Invocation):
                self.grouper.add(parsed)
            else:
                _count(self._totals.records_skipped, parsed.reason.value)
        self.grouper.advance(obj.delivered_at)

    def checkpoint(self, cursor: Cursor) -> None:
        closures = [self._closure(i) for i in self.grouper.drain_closed()]
        self._state.checkpoint(
            cursor,
            self.grouper.open_refs(),
            closures,
            self.grouper.conversation_owners(),
        )

    def _closure(self, interaction: Interaction) -> Closure:
        triplet = interaction.triplet
        try:
            result = interaction_to_payload(
                interaction,
                anonymize=self._config.anonymize,
                send_plain_end_user=self._config.send_plain_end_user,
                user_hash_secret=self._config.user_hash_secret,
                context=self._context,
            )
        except Exception:
            logger.exception(
                "Failed to convert interaction of request %s (%s)",
                interaction.first.request_id,
                interaction.first.source_key,
            )
            self._totals.conversion_errors += 1
            return Closure(triplet.interaction_id, triplet.owner, CONVERSION_ERROR)
        if isinstance(result, InteractionSkip):
            _count(self._totals.interactions_skipped, result.value)
            return Closure(triplet.interaction_id, triplet.owner, result.value)
        self._totals.interactions_queued += 1
        return Closure(triplet.interaction_id, triplet.owner, QUEUED, result)


def _hour(obj: LogObject) -> datetime:
    return obj.delivered_at.replace(minute=0, second=0, microsecond=0)


def _read_window(run: _Run, source: LogSource, window: _Window) -> None:
    objects = source.list_objects(
        since=window.since, until=window.until, after_key=window.after_key
    )
    logger.info(
        "Reading %d log files delivered from %s to %s",
        len(objects),
        window.since.isoformat(),
        window.until.isoformat(),
    )
    last: LogObject | None = None
    for obj, records in source.iter_records(objects):
        if last is not None and _hour(obj) != _hour(last):
            run.checkpoint(Cursor(window.coverage_from, last.delivered_at, last.key))
            logger.info("Synced logs up to %s", last.delivered_at.isoformat())
        run.read(obj, records)
        last = obj
    # Move on even without new files: idle interactions must close.
    run.grouper.advance(window.until)
    last_key = last.key if last is not None else window.after_key
    run.checkpoint(Cursor(window.coverage_from, window.until, last_key))


def sync(
    config: Config,
    store: ObjectStore,
    *,
    transport: httpx.BaseTransport | None = None,
    now: datetime | None = None,
) -> SyncSummary:
    """The whole run, given where to read the logs."""
    summary = SyncSummary()
    with RuntimeState(
        config.state_db,
        config.log_source,
        dry_run=config.dry_run,
        now_ms=None if now is None else int(now.timestamp() * 1000),
    ) as state:
        until = config.run_until(now)
        window = plan_window(config, state, until)
        source = LogSource(store, root_prefix=config.logs_root_prefix())
        if window.since < until:
            run = _Run(config, state, source, summary, user_defined.load_context())
            run.restore()
            if state.cursor() is None:
                # A fresh start: save where it began, so a run that stops before
                # its first checkpoint can be resumed without --from-date.
                run.checkpoint(Cursor(window.coverage_from, window.since, None))
            _read_window(run, source, window)
            summary.totals.open_interactions = run.grouper.open_count
        else:
            logger.info(
                "Nothing to read: logs are synced up to %s", window.since.isoformat()
            )
        if not config.dry_run:
            with SafeNebulyClient(
                config.nebuly_api_key,
                config.nebuly_endpoint,
                transport=transport,
                retry_http_429=config.retry_http_429,
            ) as client:
                summary.delivery = deliver(
                    state,
                    client,
                    state.pending_deliveries(),
                    clock_ms=None
                    if now is not None
                    else lambda: int(time.time() * 1000),
                )
            state.prune_content(config.content_retention_seconds)
        summary.outbox = state.delivery_counts()
    return summary


def run_sync(config: Config) -> SyncSummary:
    logger.info(
        "Bedrock runtime sync for %s%s",
        config.log_source,
        " (dry run)" if config.dry_run else "",
    )
    return sync(config, build_s3_store(config))
