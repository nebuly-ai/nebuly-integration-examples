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

from bedrock_sync.shared.cli import format_exception_stack
from bedrock_sync.shared.delivery import DeliveryReport, SafeNebulyClient, deliver
from bedrock_sync.shared.errors import ConnectorError
from bedrock_sync.shared.outbox import serialize

from . import user_defined
from .converter import InteractionSkip, interaction_to_payload
from .grouping import CallRef, Grouper, GroupingStats, triplet_of
from .models import Invocation
from .parser import parse_record
from .s3_client import LogSource, build_s3_store
from .state import CONVERSION_ERROR, QUEUED, Closure, Cursor, RuntimeState

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    import httpx

    from .config import Config
    from .grouping import Interaction
    from .s3_client import LogObject, LogRecord, ObjectStore

logger = logging.getLogger(__name__)


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
    unreadable_files: int = 0
    """Log files skipped as corrupt or not UTF-8: their calls are lost."""
    unreadable_lines: int = 0
    """Log lines skipped as not JSON: their calls are lost."""
    restore_missing_calls: int = 0
    """Saved calls unavailable or unusable when restoring open turns."""
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
    waiting_for_retry: bool = False

    @property
    def incomplete(self) -> bool:
        """Something closed was not confirmed by Nebuly, waits for a retry,
        could not be converted, or could not be read from S3."""
        delivery = self.delivery
        return bool(
            delivery.rejected
            or delivery.retryable
            or delivery.paused
            or self.waiting_for_retry
            or self.totals.conversion_errors
            or self.totals.unreadable_files
            or self.totals.unreadable_lines
            or self.totals.restore_missing_calls
        )


@dataclass(frozen=True)
class _Window:
    since: datetime
    until: datetime
    coverage_from: datetime
    after_key: str | None
    resume_at: datetime | None = None


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
    """Where this run starts. A gap can reset the cursor. A backfill reads
    again from the requested date; closed interactions remain deduplicated."""
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
        return _Window(from_date, until, from_date, None)
    if from_date is not None and from_date <= cursor.coverage_until:
        # A backfill inside the synced range, for example once a broken hook
        # is fixed: read again from it. Open interactions keep their calls
        # (read twice, they are duplicates); keep the original coverage start.
        logger.warning(
            "--from-date %s is inside the synced range: reading again from it; "
            "closed interactions are skipped except failed conversions whose "
            "first call is read again",
            from_date.isoformat(),
        )
        return _Window(from_date, until, cursor.coverage_from, None)
    # A file can appear after an earlier listing, even if the delivery time in
    # its key is before the cursor. Re-list a bounded interval and filter out
    # only keys actually processed. Explicit backfills above always reread.
    if until >= cursor.coverage_until and config.delivery_margin_seconds:
        since = max(
            cursor.coverage_from,
            cursor.coverage_until - timedelta(seconds=config.delivery_margin_seconds),
        )
        return _Window(
            since, until, cursor.coverage_from, cursor.last_key, cursor.coverage_until
        )
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
        self._read_objects: list[tuple[str, datetime]] = []
        self._held_calls: dict[str, CallRef] = {}
        self._held_turns: set[str] = set()
        self._reopened_turns: set[str] = set()
        self._replay_keys: set[str] = set()
        self._before_failed_start: dict[str, dict[CallRef, list[Invocation]]] = {}
        self.grouper = Grouper(
            idle=timedelta(minutes=config.interaction_idle_minutes),
            delivery_margin=timedelta(seconds=config.delivery_margin_seconds),
            is_closed=lambda key: (
                key not in self._reopened_turns and state.is_closed(key)
            ),
        )
        summary.grouping = self.grouper.stats

    def restore(self) -> None:
        calls = self._state.pending_calls()
        restored: dict[CallRef, Invocation] = {}
        for record in self._source.read_requests(calls):
            parsed = parse_record(record.record, source_key=record.key)
            if isinstance(parsed, Invocation) and (triplet := triplet_of(parsed)):
                restored[CallRef(parsed.request_id, record.key, triplet.turn_key)] = (
                    parsed
                )
        missing = set(calls) - restored.keys()
        self._held_turns = {call.turn_key for call in missing}
        self._held_calls = {
            call.request_id: call for call in calls if call.turn_key in self._held_turns
        }
        self._totals.restore_missing_calls = len(missing)
        if missing:
            logger.warning(
                "%d stored call(s) could not be restored; holding %d open "
                "interaction(s) until their S3 records are available",
                len(missing),
                len(self._held_turns),
            )
        self.grouper.restore(
            restored[call] for call in calls if call.turn_key not in self._held_turns
        )

    @property
    def open_count(self) -> int:
        return self.grouper.open_count + len(self._held_turns)

    def start_reading(self, objects: list[LogObject]) -> None:
        if self._config.from_date is not None:
            self._replay_keys = {obj.key for obj in objects}

    def _add(self, invocation: Invocation) -> None:
        if invocation.request_id in self._held_calls:
            return
        if (self._held_turns or self._config.from_date is not None) and (
            triplet := triplet_of(invocation)
        ) is not None:
            ref = CallRef(
                invocation.request_id, invocation.source_key, triplet.turn_key
            )
            if ref.turn_key in self._held_turns:
                self._held_calls[ref.request_id] = ref
                return
            if self._config.from_date is not None and self._retry_failed_turn(
                ref, invocation
            ):
                return
        self.grouper.add(invocation)

    def _retry_failed_turn(self, ref: CallRef, invocation: Invocation) -> bool:
        if ref.turn_key in self._reopened_turns:
            return False
        original = self._state.failed_turn_start(ref.turn_key)
        if original is None or original.source_key not in self._replay_keys:
            return False
        if ref == original:
            self._reopened_turns.add(ref.turn_key)
            self.grouper.add(invocation)
            waiting = self._before_failed_start.get(original.source_key, {})
            for earlier_delivery in waiting.pop(original, []):
                self.grouper.add(earlier_delivery)
        else:
            # S3 delivery order can put a continuation before the original call.
            waiting = self._before_failed_start.setdefault(original.source_key, {})
            waiting.setdefault(original, []).append(invocation)
        return True

    def read(self, obj: LogObject, records: list[LogRecord]) -> None:
        self._totals.files_read += 1
        for record in records:
            self._totals.records_fetched += 1
            parsed = parse_record(record.record, source_key=record.key)
            if isinstance(parsed, Invocation):
                self._add(parsed)
            else:
                _count(self._totals.records_skipped, parsed.reason.value)
        self._replay_keys.discard(obj.key)
        # A listed file may be unreadable or lack the original call. Its
        # waiting continuations still belong to a closed turn.
        for calls in self._before_failed_start.pop(obj.key, {}).values():
            for invocation in calls:
                self.grouper.add(invocation)
        self._read_objects.append((obj.key, obj.delivered_at))

    def checkpoint(self, cursor: Cursor) -> None:
        closures = [self._closure(i) for i in self.grouper.drain_closed()]
        self._state.checkpoint(
            cursor,
            [*self.grouper.open_refs(), *self._held_calls.values()],
            closures,
            read_objects=self._read_objects,
            forget_objects_before=cursor.coverage_until
            - timedelta(seconds=self._config.delivery_margin_seconds),
            reopened_turns=self._reopened_turns,
        )
        self._read_objects.clear()
        self._reopened_turns.clear()

    def read_object_keys(self, since: datetime, until: datetime) -> set[str]:
        return self._state.read_object_keys(since, until)

    def _closure(self, interaction: Interaction) -> Closure:
        triplet = interaction.triplet
        try:
            result = interaction_to_payload(
                interaction,
                anonymize=self._config.anonymize,
                pseudonymize_end_user=self._config.pseudonymize_end_user,
                user_hash_secret=self._config.user_hash_secret,
                context=self._context,
            )
            if not isinstance(result, InteractionSkip):
                serialize(result)
        except Exception as exc:  # noqa: BLE001 - a hook bug must not stop the run
            # Only the type: the message may hold prompt text or secrets.
            logger.error(  # noqa: TRY400
                "Could not convert the interaction of request %s (%s): %s. If the "
                "error is in user_defined.py, fix it there (--verbose shows where "
                "it happened), then backfill with --from-date to send it",
                interaction.first.request_id,
                interaction.first.source_key,
                type(exc).__name__,
            )
            logger.debug(
                "Conversion error of request %s:\n%s",
                interaction.first.request_id,
                format_exception_stack(exc),
            )
            self._totals.conversion_errors += 1
            first = CallRef(
                interaction.first.request_id,
                interaction.first.source_key,
                triplet.turn_key,
            )
            return Closure(triplet.turn_key, CONVERSION_ERROR, first_call=first)
        if isinstance(result, InteractionSkip):
            _count(self._totals.interactions_skipped, result.value)
            return Closure(triplet.turn_key, result.value)
        self._totals.interactions_queued += 1
        return Closure(triplet.turn_key, QUEUED, result)


def _hour(obj: LogObject) -> datetime:
    return obj.delivered_at.replace(minute=0, second=0, microsecond=0)


def _read_window(run: _Run, source: LogSource, window: _Window) -> None:
    seen = (
        run.read_object_keys(window.since, window.resume_at)
        if window.resume_at is not None
        else set()
    )
    objects = source.list_objects(
        since=window.since,
        until=window.until,
        after_key=window.after_key if window.resume_at is None else None,
    )
    objects = [obj for obj in objects if obj.key not in seen]
    run.start_reading(objects)
    logger.info(
        "Reading %d log files delivered from %s to %s",
        len(objects),
        window.since.isoformat(),
        window.until.isoformat(),
    )
    last: LogObject | None = None
    latest_key = window.after_key
    for obj, records in source.iter_records(objects):
        if last is not None and obj.delivered_at > last.delivered_at:
            run.grouper.advance(last.delivered_at)
        if last is not None and _hour(obj) != _hour(last):
            covered = max(last.delivered_at, window.resume_at or last.delivered_at)
            run.checkpoint(Cursor(window.coverage_from, covered, latest_key))
            logger.info("Synced logs up to %s", covered.isoformat())
        run.read(obj, records)
        last = obj
        latest_key = max(latest_key or obj.key, obj.key)
    # Move on even without new files: idle interactions must close.
    run.grouper.advance(window.until)
    run.checkpoint(Cursor(window.coverage_from, window.until, latest_key))


def _sync_logs(
    state: RuntimeState,
    config: Config,
    store: ObjectStore,
    summary: SyncSummary,
    now: datetime | None,
) -> None:
    until = config.run_until(now)
    window = plan_window(config, state, until)
    if window.since >= until:
        logger.info(
            "Nothing to read: logs are synced up to %s", window.since.isoformat()
        )
        return
    source = LogSource(store, root_prefix=config.logs_root_prefix())
    run = _Run(config, state, source, summary, user_defined.load_context())
    run.restore()
    if state.cursor() is None:
        # A fresh start: save where it began, so a run that stops before
        # its first checkpoint can be resumed without --from-date.
        run.checkpoint(Cursor(window.coverage_from, window.since, None))
    _read_window(run, source, window)
    summary.totals.open_interactions = run.open_count
    summary.totals.unreadable_files = source.problems.unreadable_files
    summary.totals.unreadable_lines = source.problems.unreadable_lines


def sync(
    config: Config,
    store: ObjectStore | None = None,
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
        recovery_key = config.retry_given_up
        if recovery_key is None:
            _sync_logs(
                state,
                config,
                store if store is not None else build_s3_store(config),
                summary,
                now,
            )
        else:
            state.retry_given_up(recovery_key)
        if not config.dry_run:
            with SafeNebulyClient(
                config.nebuly_api_key,
                config.nebuly_endpoint,
                transport=transport,
            ) as client:
                summary.delivery = deliver(
                    state,
                    client,
                    state.pending_deliveries()
                    if recovery_key is None
                    else state.pending_delivery(recovery_key),
                    clock_ms=None
                    if now is not None
                    else lambda: int(time.time() * 1000),
                )
            if recovery_key is None:
                state.prune_content(config.content_retention_seconds)
                state.prune_history(config.history_retention_seconds)
            summary.waiting_for_retry = state.delivery_waiting_for_retry(recovery_key)
        summary.outbox = state.delivery_counts()
    return summary


def run_sync(config: Config) -> SyncSummary:
    logger.info(
        "Bedrock runtime sync for %s%s",
        config.log_source,
        " (dry run)" if config.dry_run else "",
    )
    return sync(config)
