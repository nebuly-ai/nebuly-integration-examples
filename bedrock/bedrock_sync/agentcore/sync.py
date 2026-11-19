"""One AgentCore sync run: read CloudWatch, assemble traces, stage, deliver.

1. Plan the reading windows from the coverage, the flags and, once a day,
   a reconciliation that reads again for late events.
2. Read every page of every log group into the state, then mark the window
   complete.
3. Parse the new records and mark their traces to assemble.
4. Read back the start of traces that began before the windows (recovery).
5. Assemble each trace, oldest first; stage the ready ones in the outbox.
   A session with traces of several actors is legal (Nebuly shows one
   conversation per actor) but unusual: it is logged and counted.
6. Deliver the outbox with the rules shared with the runtime connector, then
   erase content older than the retention.

The command line (``__main__``) prints the returned report as JSON.
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from bedrock_sync.shared import outbox
from bedrock_sync.shared.cli import format_exception_stack
from bedrock_sync.shared.delivery import SafeNebulyClient, deliver
from bedrock_sync.shared.errors import ConfigError
from bedrock_sync.shared.keys import digest

from . import user_defined
from .assembler import STRANDS, assemble
from .cloudwatch_client import CloudWatchLogReader, create_logs_client
from .converter import snapshot_to_payload
from .models import IdentityLog
from .parser import parse_record
from .reading import ReadWindow, datetime_to_epoch_ms, plan_reads, plan_trace_recovery
from .records import SourceScope
from .state import AgentCoreState

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    import httpx

    from bedrock_sync.shared.delivery import Delivery

    from .cloudwatch_client import LogsAPI
    from .config import AgentCoreConfig
    from .models import Diagnostic, InteractionSnapshot, Span

logger = logging.getLogger(__name__)


@dataclass
class SyncReport:
    dry_run: bool = False
    records_read: int = 0
    new_records: int = 0
    ready: int = 0
    open: int = 0
    blocked: int = 0
    dropped: int = 0
    sessions_with_several_actors: int = 0
    """Sessions with traces of several actors in this run: each actor's traces
    are sent, and Nebuly shows one conversation per actor."""
    sent: int = 0
    rejected: int = 0
    retryable: int = 0
    expired: int = 0
    expired_reasons: dict[str, int] = field(default_factory=dict)
    degraded: dict[str, int] = field(default_factory=dict)
    """Interactions sent with warnings, by warning code."""
    delivery_paused: bool = False
    delivery_waiting_for_retry: bool = False
    pruned_records: int = 0
    forgotten_traces: int = 0
    diagnostics: dict[str, int] = field(default_factory=dict)
    outbox_statuses: dict[str, int] = field(default_factory=dict)

    def diagnose(self, code: str) -> None:
        self.diagnostics[code] = self.diagnostics.get(code, 0) + 1

    @property
    def incomplete(self) -> bool:
        """Something staged was not confirmed by Nebuly, waits for a retry, a
        trace expired before it could be sent, or one could not be converted."""
        return bool(
            self.rejected
            or self.retryable
            or self.delivery_paused
            or self.delivery_waiting_for_retry
            or self.expired
            or self.diagnostics.get("conversion_failed")
        )


def _read_window(
    state: AgentCoreState,
    reader: CloudWatchLogReader,
    window: ReadWindow,
    report: SyncReport,
    *,
    coverage: bool,
) -> None:
    window_id = state.begin_window(window.start_ms, window.end_ms) if coverage else None
    for page in reader.iter_pages(
        window.start_ms, window.end_ms, trace_id=window.trace_id
    ):
        report.records_read += len(page.records)
        report.new_records += (
            state.ingest_page(window_id, page)
            if window_id is not None
            else state.ingest_recovery_page(page)
        )
    if window_id is not None:
        state.complete_window(window_id)


def _mark_dirty(state: AgentCoreState, trace_id: str, earliest_ms: int) -> None:
    previous = state.trace(trace_id)
    snapshot = dict(previous.snapshot) if previous is not None else {}
    snapshot["needs_assembly"] = True
    # This marker precedes normalization so a crash cannot strand processed input.
    state.save_trace(trace_id, "blocked", earliest_ms, snapshot)


def _normalize_new(
    state: AgentCoreState, report: SyncReport, config: AgentCoreConfig
) -> set[str]:
    """Parse the records not processed yet; return the traces they touch."""
    touched: set[str] = set()
    for raw in state.unprocessed_records():
        parsed = parse_record(
            raw,
            state.source,
            actor_namespace_templates=config.actor_namespace_templates,
        )
        for item in parsed.diagnostics:
            report.diagnose(item.code)
        # The parser returns a span, an identity log, or one diagnostic.
        record = parsed.span or parsed.identity
        issue = parsed.diagnostics[0] if parsed.diagnostics else None
        unsupported = issue is not None and issue.code == "unsupported_log"
        trace_id = (
            record.trace_id
            if record is not None
            else (issue.trace_id if issue is not None else None)
        )
        if trace_id is not None and not unsupported:
            if state.is_compacted(trace_id):
                report.diagnose("late_record_for_compacted_trace")
            else:
                earliest = (
                    parsed.span.start_ns // 1_000_000
                    if parsed.span is not None
                    else raw.timestamp_ms
                )
                _mark_dirty(state, trace_id, earliest)
                touched.add(trace_id)
        # A routine log keeps its trace link without making the trace pending.
        state.mark_processed(raw, trace_id=trace_id, unsupported=unsupported)
    return touched


def _trace_evidence(
    state: AgentCoreState,
    trace_id: str,
    config: AgentCoreConfig,
) -> tuple[list[Span | IdentityLog], list[Diagnostic], int]:
    """The parsed records of a trace, their diagnostics and the earliest start."""
    records: list[Span | IdentityLog] = []
    diagnostics: list[Diagnostic] = []
    earliest: list[int] = []
    for raw in state.records(trace_id=trace_id):
        parsed = parse_record(
            raw,
            state.source,
            actor_namespace_templates=config.actor_namespace_templates,
        )
        record = parsed.span or parsed.identity
        if record is not None:
            records.append(record)
        diagnostics.extend(parsed.diagnostics)
        earliest.append(
            parsed.span.start_ns // 1_000_000 if parsed.span else raw.timestamp_ms
        )
    if not earliest:
        raise ValueError("Persisted trace has no ingestion evidence")
    return records, diagnostics, min(earliest)


def _recover_prefixes(
    state: AgentCoreState,
    reader: CloudWatchLogReader,
    candidates: set[str],
    read_start: int,
    end: int,
    report: SyncReport,
    config: AgentCoreConfig,
) -> dict[str, int]:
    bounds = {
        trace: min(
            read_start,
            recovered
            if (recovered := state.recovered_start(trace)) is not None
            else read_start,
        )
        for trace in candidates
    }
    # Ancestors discovered during recovery can start earlier than the initial root.
    # Bound repeated expansion and leave the trace blocked if it cannot converge.
    for _ in range(8):
        windows: list[ReadWindow] = []
        for trace_id in sorted(candidates):
            earliest = _trace_evidence(state, trace_id, config)[2]
            try:
                windows.extend(
                    plan_trace_recovery(
                        [(trace_id, earliest)],
                        read_start_ms=bounds[trace_id],
                        end_ms=end,
                    )
                )
            except ValueError:
                report.diagnose("invalid_trace_recovery_bounds")
                bounds[trace_id] = end
        if not windows:
            break
        for window in windows:
            if window.trace_id is None:
                raise ValueError("Recovery must be trace-filtered")
            _read_window(state, reader, window, report, coverage=False)
            bounds[window.trace_id] = window.start_ms
            state.mark_recovered(window.trace_id, window.start_ms)
        discovered = _normalize_new(state, report, config)
        for trace_id in discovered - candidates:
            bounds[trace_id] = read_start
        candidates.update(discovered)
    for trace_id, bound in bounds.items():
        state.mark_recovered(trace_id, bound)
    return bounds


def _stage(
    state: AgentCoreState,
    interaction: InteractionSnapshot,
    config: AgentCoreConfig,
    context: Mapping[str, object],
    report: SyncReport,
    actors: dict[str, set[str]],
) -> None:
    try:
        payload = snapshot_to_payload(
            interaction, source=state.source, config=config, context=context
        )
        outbox.serialize(payload)
    except Exception as exc:  # noqa: BLE001 - a hook bug must not stop other traces
        # Only the type: the message may hold prompt text or secrets.
        logger.error(  # noqa: TRY400
            "Could not convert trace %s (%s): it is retried every run until it "
            "expires; if the error is in user_defined.py, fix it there "
            "(--verbose shows where it happened)",
            interaction.trace_id,
            type(exc).__name__,
        )
        logger.debug(
            "Conversion error of trace %s (stack without message):\n%s",
            interaction.trace_id,
            format_exception_stack(exc),
        )
        state.save_trace(
            interaction.trace_id,
            "blocked",
            interaction.earliest_timestamp_ms,
            {**asdict(interaction), "conversion_error": True},
        )
        report.diagnose("conversion_failed")
        return
    _note_actor(interaction, report, actors)
    existing = state.find_delivery(interaction.interaction_key)
    if existing is not None:
        if existing.payload == payload or state.delivery_gave_up(
            interaction.interaction_key
        ):
            _mark_ready(state, interaction)
            return
        was_attempted = existing.status != "pending" or state.delivery_was_attempted(
            interaction.interaction_key
        )
        if was_attempted and not _same_nebuly_interaction(existing.payload, payload):
            report.diagnose("late_delivery_discrepancy")
            logger.warning(
                "Trace %s changed after it was attempted and its Nebuly deduplication "
                "key differs or is unavailable; keeping the previous payload",
                interaction.trace_id,
            )
            _mark_ready(state, interaction)
            return
        if existing.status != "pending":
            state.requeue_payload(
                interaction.interaction_key, payload, end_ns=interaction.end_ns
            )
            report.diagnose("updated_after_delivery")
            _warn_degraded(interaction, report, "Updating")
            _mark_ready(state, interaction)
            return
    if existing is None:
        _supersede(state, interaction, report)
        _warn_degraded(interaction, report, "Sending")
    state.put_payload(
        interaction.trace_id,
        interaction.root_span_id,
        payload,
        end_ns=interaction.end_ns,
    )
    _mark_ready(state, interaction)


def _mark_ready(
    state: AgentCoreState,
    interaction: InteractionSnapshot,
) -> None:
    state.save_trace(
        interaction.trace_id,
        "ready",
        interaction.earliest_timestamp_ms,
        asdict(interaction),
    )


def _supersede(
    state: AgentCoreState, interaction: InteractionSnapshot, report: SyncReport
) -> None:
    """A sub-agent sent as the turn, after --root-wait-seconds, before its
    caller arrived (a job longer than the wait): the real turn is sent too."""
    held = state.supersede_roots(interaction.trace_id, interaction.root_span_id)
    if held:
        report.diagnose("sub_agent_sent_as_turn")
        logger.warning(
            "Trace %s: a sub-agent was sent as the turn before its caller "
            "arrived; the full turn is sent too, so Nebuly also shows the "
            "sub-agent as an interaction. A longer --root-wait-seconds avoids it",
            interaction.trace_id,
        )


def _note_actor(
    interaction: InteractionSnapshot,
    report: SyncReport,
    actors: dict[str, set[str]],
) -> None:
    """Warn once per run when a session has traces of several actors: legal,
    but usually a bug in how the agent sets the actor."""
    session = digest("agentcore.session", 1, interaction.conversation_id)
    seen = actors.setdefault(session, set())
    actor = digest("agentcore.actor", 1, str(interaction.actor_id))
    if actor in seen:
        return
    seen.add(actor)
    if len(seen) == 2:
        report.sessions_with_several_actors += 1
        logger.warning(
            "The session of trace %s has traces of several actors: Nebuly "
            "shows one conversation per actor",
            interaction.trace_id,
        )


_NEBULY_MATCH = ("input", "end_user", "time_start")
"""Nebuly updates an interaction that arrives again with these values."""


def _same_nebuly_interaction(
    previous: Mapping[str, object], replacement: Mapping[str, object]
) -> bool:
    before, after = previous.get("interaction"), replacement.get("interaction")
    return (
        isinstance(before, dict)
        and isinstance(after, dict)
        and all(
            name in before and before[name] == after.get(name) for name in _NEBULY_MATCH
        )
    )


def _warn_degraded(
    interaction: InteractionSnapshot, report: SyncReport, action: str
) -> None:
    """Count and log what an interaction is sent without."""
    codes = interaction.warnings
    if not codes:
        return
    for code in codes:
        report.degraded[code] = report.degraded.get(code, 0) + 1
    logger.warning(
        "%s trace %s with warnings: %s", action, interaction.trace_id, ", ".join(codes)
    )


def _assemble_candidates(
    state: AgentCoreState,
    candidates: Iterable[str],
    bounds: Mapping[str, int],
    config: AgentCoreConfig,
    context: Mapping[str, object],
    report: SyncReport,
    end_ms: int,
) -> None:
    assembled = []
    for trace_id in sorted(candidates):
        records, diagnostics, earliest = _trace_evidence(state, trace_id, config)
        result = assemble(
            state.source,
            records,
            actor_id_attribute=config.actor_id_attribute,
            parse_diagnostics=diagnostics,
            read_start_ms=bounds[trace_id],
        )
        start = result.interactions[0].start_ns if result.interactions else 0
        parse_codes = Counter(item.code for item in diagnostics)
        unrelated = "malformed_record" not in parse_codes and not any(
            isinstance(record, IdentityLog) or record.scope == STRANDS
            for record in records
        )
        assembled.append((start, trace_id, earliest, result, parse_codes, unrelated))
    # Oldest first, so the outbox sends the traces of a session in order.
    actors: dict[str, set[str]] = {}
    assembled.sort(key=lambda item: (item[0], item[1]))
    for _, trace_id, earliest, result, parse_codes, unrelated in assembled:
        # Parse diagnostics were counted when their records were first read.
        new_codes = Counter(item.code for item in result.diagnostics) - parse_codes
        for code, count in new_codes.items():
            report.diagnostics[code] = report.diagnostics.get(code, 0) + count
        if not result.interactions:
            state.save_trace(
                trace_id,
                "blocked",
                earliest,
                {
                    "diagnostics": [asdict(item) for item in result.diagnostics],
                    "unrelated_rootless": unrelated,
                },
            )
            report.blocked += 1
            continue
        interaction = result.interactions[0]
        if interaction.end_ns is not None and interaction.end_ns >= end_ms * 1_000_000:
            state.save_trace(
                trace_id,
                "blocked",
                earliest,
                {**asdict(interaction), "outside_run_end": True},
            )
            report.diagnose("outside_run_end")
            report.blocked += 1
            continue
        if (
            interaction.readiness == "ready"
            and not interaction.anchored
            and interaction.end_ns is not None
            and interaction.end_ns
            > (state.now_ms - config.root_wait_seconds * 1000) * 1_000_000
        ):
            # Maybe a sub-agent whose caller is not exported yet: wait for the
            # spans above it, or for --root-wait-seconds, before sending.
            state.save_trace(
                trace_id,
                "open",
                interaction.earliest_timestamp_ms,
                {**asdict(interaction), "waiting_for_parent": True},
            )
            report.diagnose("waiting_for_parent")
            report.open += 1
            continue
        # Persist staging before clearing the dirty marker. After a crash, a
        # blocked trace is assembled again even when its previous POST was sent.
        if interaction.readiness == "ready":
            _stage(state, interaction, config, context, report, actors)
        else:
            state.save_trace(
                trace_id,
                interaction.readiness,
                interaction.earliest_timestamp_ms,
                asdict(interaction),
            )
        final = state.trace(trace_id)
        if final is not None:
            setattr(report, final.status, getattr(report, final.status) + 1)


@dataclass(frozen=True)
class _Plan:
    windows: tuple[ReadWindow, ...]
    start_ms: int
    end_ms: int
    reconcile: bool


def _plan(state: AgentCoreState, config: AgentCoreConfig, now: datetime) -> _Plan:
    end = datetime_to_epoch_ms(min(config.to_date, now) if config.to_date else now)
    start = datetime_to_epoch_ms(config.from_date) if config.from_date else None
    coverage = state.coverage
    if (
        coverage is not None
        and start is None
        and config.overlap_seconds == 0
        and end == coverage[1]
    ):
        # Recheck a single millisecond rather than failing a valid zero-overlap run.
        start = max(coverage[0], end - 1)
    if coverage is not None and start is None:
        resume = max(coverage[0], coverage[1] - config.overlap_seconds * 1000)
        if end <= resume:
            # --to-date (or the clock) is before where this run would start.
            return _Plan((), end, end, reconcile=False)
    last_reconciliation = state.last_reconciliation()
    reconcile = last_reconciliation is None or (
        end >= last_reconciliation + config.reconciliation_interval_seconds * 1000
    )
    try:
        windows = plan_reads(
            coverage=coverage,
            requested_start_ms=start,
            end_ms=end,
            overlap_seconds=config.overlap_seconds,
            reconciliation_seconds=config.reconciliation_seconds if reconcile else 0,
            last_reconciliation_ms=last_reconciliation,
        ).windows
    except ValueError as exc:
        raise ConfigError(f"Cannot plan the CloudWatch read: {exc}") from None
    return _Plan(
        windows,
        min(window.start_ms for window in windows),
        max(window.end_ms for window in windows),
        reconcile,
    )


def _read_and_assemble(
    state: AgentCoreState,
    reader: CloudWatchLogReader,
    plan: _Plan,
    config: AgentCoreConfig,
    context: Mapping[str, object],
    report: SyncReport,
) -> None:
    for window in plan.windows:
        _read_window(state, reader, window, report, coverage=True)
    if plan.reconcile:
        state.mark_reconciliation(plan.end_ms)
    touched = _normalize_new(state, report, config)
    candidates = (
        touched
        | {trace.trace_id for trace in state.pending_traces()}
        | {trace.trace_id for trace in state.ready_traces(unattempted_only=True)}
    )
    bounds = _recover_prefixes(
        state, reader, candidates, plan.start_ms, plan.end_ms, report, config
    )
    _assemble_candidates(
        state, candidates, bounds, config, context, report, plan.end_ms
    )


def _deliver(
    state: AgentCoreState,
    config: AgentCoreConfig,
    report: SyncReport,
    pending: Iterable[Delivery],
    transport: httpx.BaseTransport | None,
    clock_ms: Callable[[], int],
) -> None:
    with SafeNebulyClient(
        config.nebuly_api_key, config.nebuly_endpoint, transport=transport
    ) as client:
        result = deliver(state, client, pending, clock_ms=clock_ms)
    report.sent, report.rejected = result.sent, result.rejected
    report.retryable = result.retryable
    report.delivery_paused = result.paused


def run_sync(
    config: AgentCoreConfig,
    *,
    logs_client: LogsAPI | None = None,
    transport: httpx.BaseTransport | None = None,
    now: datetime | None = None,
) -> SyncReport:
    """Execute one source-scoped run; injected transports make replay offline."""
    clock = now or datetime.now(UTC)
    started = time.monotonic()
    clock = clock.replace(tzinfo=UTC) if clock.tzinfo is None else clock.astimezone(UTC)
    clock_start_ms = datetime_to_epoch_ms(clock)
    source = SourceScope(config.account_id, config.region, config.source_id)
    report = SyncReport(dry_run=config.dry_run)

    def delivery_clock() -> int:
        elapsed_ms = int((time.monotonic() - started) * 1000) if now is None else 0
        return clock_start_ms + elapsed_ms

    with AgentCoreState(
        config.state_db,
        source,
        config.log_groups,
        dry_run=config.dry_run,
        now_ms=clock_start_ms,
    ) as state:
        if config.retry_given_up is not None:
            key = config.retry_given_up
            state.retry_given_up(key)
            pending = state.pending_delivery(key)
            _deliver(state, config, report, pending, transport, delivery_clock)
            report.delivery_waiting_for_retry = state.delivery_waiting_for_retry(key)
            report.outbox_statuses = state.delivery_counts()
            return report
        state.configure_parser(config.actor_namespace_templates)
        plan = _plan(state, config, clock)
        context = user_defined.load_context()
        reader = CloudWatchLogReader(
            logs_client if logs_client is not None else create_logs_client(config),
            config.log_groups,
        )
        if plan.windows:
            _read_and_assemble(state, reader, plan, config, context, report)
        else:
            logger.info(
                "Nothing to read: logs are read up to %s",
                datetime.fromtimestamp(plan.end_ms / 1000, UTC).isoformat(),
            )
        if not config.dry_run:
            ended_before_ns = plan.end_ms * 1_000_000
            _deliver(
                state,
                config,
                report,
                state.pending_deliveries(ended_before_ns=ended_before_ns),
                transport,
                delivery_clock,
            )
            report.delivery_waiting_for_retry = (
                state.delivery_waiting_for_eligible_retry(
                    ended_before_ns=ended_before_ns
                )
            )
        expired, reasons, ignored = state.expire_traces(
            max(
                0,
                plan.end_ms
                - (config.reconciliation_seconds + config.content_retention_seconds)
                * 1000,
            )
        )
        if ignored:
            report.diagnostics["ignored_rootless_trace"] = ignored
        if expired:
            report.expired = expired
            report.expired_reasons = dict(sorted(reasons.items()))
            report.diagnostics["expired_trace"] = expired
            logger.error(
                "%d trace(s) expired before they could be sent (%s); "
                "see Known limitations in the AgentCore README",
                expired,
                ", ".join(f"{code}: {n}" for code, n in sorted(reasons.items())),
            )
        report.pruned_records = state.prune_content(config.content_retention_seconds)
        report.forgotten_traces = state.prune_history(config.history_retention_seconds)
        report.outbox_statuses = state.delivery_counts()
    return report
