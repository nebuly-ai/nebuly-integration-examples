"""One sync run: read the new Bedrock log files, rebuild the turns, send them.

1. Plan the window ``since -> until`` from the cursor and the CLI flags.
2. Rebuild the ``Grouper`` as the last run left it (``restore``).
3. Read the log files in key order: parse, group, send the finished turns.
4. Save a checkpoint at each new hour of files and at the end.

A turn is sent once: the registry is checked before each POST and written
right after it. If a POST still fails after its retries the run stops without
a checkpoint, and the next run starts again from the last one.
"""

from __future__ import annotations

import logging
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol

import httpx

from .converter import SkipReason, turn_to_payload
from .grouping import Grouper, GroupingStats
from .models import Invocation
from .nebuly_client import NebulyClient, PermanentRejectionError
from .parser import parse_record
from .s3_client import LogSource, build_s3_store
from .state import DB_FILENAME, Cursor, SyncState

if TYPE_CHECKING:
    from datetime import datetime
    from typing import Any

    from .config import Config
    from .grouping import GrouperSnapshot, Turn
    from .s3_client import LogObject, LogRecord, ObjectStore

logger = logging.getLogger(__name__)


class FirstRunRequiresFromDateError(RuntimeError):
    """Raised when the first sync run is attempted without --from-date."""


class GapAbortedError(RuntimeError):
    """Raised when a gap-creating run is declined or stdin is not interactive."""


class _SendFailedError(RuntimeError):
    """A POST failed after its retries: stop without a checkpoint."""


class InteractionSender(Protocol):
    def send_interaction(self, payload: dict[str, Any]) -> None: ...


@dataclass
class Counts:
    files_read: int = 0
    records_fetched: int = 0
    records_skipped: Counter[str] = field(default_factory=Counter)
    """Records the parser could not use, by reason."""
    interactions_sent: int = 0
    interactions_rejected: int = 0
    """Refused by Nebuly with a permanent 4xx; not sent again."""
    interactions_already_sent: int = 0
    interactions_skipped: Counter[str] = field(default_factory=Counter)
    """Turns the converter does not send, by reason."""
    interactions_failed: int = 0
    conversion_errors: int = 0


@dataclass
class SyncSummary:
    totals: Counts = field(default_factory=Counts)
    grouping: GroupingStats = field(default_factory=GroupingStats)
    stopped: bool = False


@dataclass(frozen=True)
class _Window:
    since: datetime
    until: datetime
    coverage_from: datetime
    after_key: str | None


def _configure_logging(*, verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    for noisy in ("httpx", "httpcore", "botocore", "boto3", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _counter_str(counter: Counter[str]) -> str:
    return ",".join(f"{k}:{v}" for k, v in sorted(counter.items())) or "0"


def _log_summary(summary: SyncSummary) -> None:
    totals, grouping = summary.totals, summary.grouping
    logger.info(
        "%s: files_read=%s records_fetched=%s records_skipped=%s duplicates=%s "
        "interactions_sent=%s interactions_rejected=%s "
        "interactions_already_sent=%s interactions_skipped=%s "
        "interactions_failed=%s conversion_errors=%s partial_starts=%s "
        "timed_out_turns=%s ambiguous_links=%s",
        "Sync stopped" if summary.stopped else "Sync complete",
        totals.files_read,
        totals.records_fetched,
        _counter_str(totals.records_skipped),
        grouping.duplicates,
        totals.interactions_sent,
        totals.interactions_rejected,
        totals.interactions_already_sent,
        _counter_str(totals.interactions_skipped),
        totals.interactions_failed,
        totals.conversion_errors,
        grouping.partial_starts,
        grouping.timed_out_turns,
        grouping.ambiguous_links,
    )


def _confirm_gap(config: Config) -> None:
    if config.force:
        return
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


def plan_window(config: Config, state: SyncState, until: datetime) -> _Window:
    """Where this run starts. May reset the state (a gap, or a backfill
    before the coverage); the sent registry is kept, so turns already sent
    are skipped."""
    cursor = state.get_cursor()
    from_date = config.from_date
    if cursor is None:
        if from_date is None:
            raise FirstRunRequiresFromDateError(
                "First run requires --from-date when no sync state exists in the cache"
            )
        return _Window(from_date, until, from_date, None)

    if from_date is not None and (
        from_date > cursor.coverage_until or until < cursor.coverage_from
    ):
        # A range that does not touch the coverage, after or before it: the
        # coverage is reset to it, as Gemini does once the gap is confirmed.
        _confirm_gap(config)
        state.reset()
        return _Window(from_date, until, from_date, None)
    if from_date is not None and from_date < cursor.coverage_from:
        # The Grouper only moves forward: read again from --from-date rather
        # than the missing piece alone, which would split its conversations.
        logger.warning(
            "--from-date %s is before the synced range (from %s): reading again "
            "from it; interactions already sent are skipped",
            from_date.isoformat(),
            cursor.coverage_from.isoformat(),
        )
        state.reset()
        return _Window(from_date, until, from_date, None)
    return _Window(cursor.coverage_until, until, cursor.coverage_from, cursor.last_key)


def reload_calls(source: LogSource, snapshot: GrouperSnapshot) -> dict[str, Invocation]:
    """The calls left half done at the last checkpoint, read again from S3
    and parsed, by request id, for ``Grouper.restore``."""
    refs = snapshot.call_refs()
    records = source.read_requests((ref.source_key, ref.request_id) for ref in refs)
    calls: dict[str, Invocation] = {}
    for record in records:
        parsed = parse_record(record.record, source_key=record.key)
        if isinstance(parsed, Invocation):
            calls[parsed.request_id] = parsed
    return calls


class _Run:
    def __init__(
        self,
        config: Config,
        state: SyncState,
        source: LogSource,
        nebuly: InteractionSender,
        summary: SyncSummary,
    ) -> None:
        self._config = config
        self._state = state
        self._source = source
        self._nebuly = nebuly
        self._totals = summary.totals
        self.grouper = Grouper(
            chain_ttl=timedelta(days=config.conversation_chain_ttl_days)
        )
        summary.grouping = self.grouper.stats

    def restore(self) -> None:
        snapshot = self._state.load_snapshot()
        self.grouper.restore(snapshot, reload_calls(self._source, snapshot))

    def read(self, obj: LogObject, records: list[LogRecord]) -> None:
        self._totals.files_read += 1
        for record in records:
            self._totals.records_fetched += 1
            parsed = parse_record(record.record, source_key=record.key)
            if isinstance(parsed, Invocation):
                self.grouper.add(parsed)
            else:
                self._totals.records_skipped[parsed.reason.value] += 1
        self.grouper.advance(obj.delivered_at)
        self.send_ready()

    def send_ready(self) -> None:
        for turn in self.grouper.drain_complete():
            self._send(turn)

    def checkpoint(self, cursor: Cursor) -> None:
        self._state.save_checkpoint(cursor, self.grouper.snapshot())

    def _send(self, turn: Turn) -> None:
        key = turn.first.request_id
        if self._state.is_emitted(key):
            self._totals.interactions_already_sent += 1
            return
        try:
            result = turn_to_payload(
                turn,
                anonymize=self._config.anonymize,
                send_plain_end_user=self._config.send_plain_end_user,
                user_hash_secret=self._config.user_hash_secret,
            )
        except Exception:
            logger.exception(
                "Failed to convert turn of request %s (%s)", key, turn.first.source_key
            )
            self._totals.conversion_errors += 1
            return
        if isinstance(result, SkipReason):
            logger.debug("Skipping turn of request %s: %s", key, result.value)
            self._totals.interactions_skipped[result.value] += 1
            return
        try:
            self._nebuly.send_interaction(result)
            status = "sent"
            self._totals.interactions_sent += 1
        except PermanentRejectionError:
            status = "rejected"
            self._totals.interactions_rejected += 1
        except (httpx.HTTPError, OSError) as exc:
            logger.exception("Failed to send turn of request %s", key)
            self._totals.interactions_failed += 1
            raise _SendFailedError(key) from exc
        self._state.record_emitted(turn.conversation_id, key, status)
        self._state.commit()


def _hour(obj: LogObject) -> datetime:
    return obj.delivered_at.replace(minute=0, second=0, microsecond=0)


def sync(
    config: Config,
    store: ObjectStore,
    nebuly: InteractionSender,
    *,
    now: datetime | None = None,
) -> SyncSummary:
    """The whole run, given where to read the logs and where to send them."""
    summary = SyncSummary()
    log_source = f"s3://{config.s3_bucket}/{config.logs_root_prefix()}"
    state = SyncState(
        config.cache_dir / DB_FILENAME, log_source, dry_run=config.dry_run
    )
    try:
        until = config.run_until(now)
        window = plan_window(config, state, until)
        if window.since >= until:
            logger.info(
                "Nothing to sync: logs are synced up to %s", window.since.isoformat()
            )
            return summary

        source = LogSource(store, root_prefix=config.logs_root_prefix())
        run = _Run(config, state, source, nebuly, summary)
        run.restore()
        if state.get_cursor() is None:
            # A fresh start: save where it began, so a run that stops before
            # its first checkpoint can be resumed without --from-date.
            run.checkpoint(Cursor(window.coverage_from, window.since, None))

        objects = source.list_objects(
            since=window.since, until=until, after_key=window.after_key
        )
        logger.info(
            "Reading %d log files delivered from %s to %s",
            len(objects),
            window.since.isoformat(),
            until.isoformat(),
        )
        last: LogObject | None = None
        try:
            for obj, records in source.iter_records(objects):
                if last is not None and _hour(obj) != _hour(last):
                    run.checkpoint(
                        Cursor(window.coverage_from, last.delivered_at, last.key)
                    )
                    logger.info("Synced logs up to %s", last.delivered_at.isoformat())
                run.read(obj, records)
                last = obj
            # Move on even without new files: buffered calls and open turns
            # must not wait for the next file to be released or timed out.
            run.grouper.advance(until)
            run.send_ready()
        except _SendFailedError:
            summary.stopped = True
            logger.error(  # noqa: TRY400 - the traceback is logged by _send
                "Stopping: the next run starts again from the last checkpoint"
            )
            _log_summary(summary)
            return summary
        last_key = last.key if last is not None else window.after_key
        run.checkpoint(Cursor(window.coverage_from, until, last_key))
        _log_summary(summary)
        return summary
    finally:
        state.close()


def run_sync(config: Config) -> SyncSummary:
    _configure_logging(verbose=config.verbose)
    logger.info(
        "Bedrock sync for s3://%s/%s%s",
        config.s3_bucket,
        config.logs_root_prefix(),
        " (dry run)" if config.dry_run else "",
    )
    with httpx.Client(timeout=60.0) as http_client:
        nebuly = NebulyClient(
            http_client,
            config.nebuly_api_key,
            config.nebuly_endpoint,
            dry_run=config.dry_run,
        )
        return sync(config, build_s3_store(config), nebuly)
