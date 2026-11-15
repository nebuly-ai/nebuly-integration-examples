"""One AgentCore sync run: read CloudWatch, assemble traces, stage, deliver.

1. Plan the reading windows from the coverage, the flags and, once a day,
   a reconciliation that reads the last day again for late events.
2. Read every page of every log group into the state, then mark the window
   complete.
3. Parse the new records and mark their traces to assemble.
4. Read back the start of traces that began before the windows (recovery).
5. Assemble each trace, oldest first; stage the ready ones in the outbox,
   unless their session belongs to another actor (then ``dropped``).
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

from bedrock_sync.shared.delivery import DeliveryReport, SafeNebulyClient, deliver

from . import user_defined
from .assembler import assemble
from .cloudwatch_client import CloudWatchLogReader, create_logs_client
from .converter import snapshot_to_payload
from .parser import parse_record
from .reading import ReadWindow, datetime_to_epoch_ms, plan_reads, plan_trace_recovery
from .records import SourceScope
from .state import AgentCoreState

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import httpx

    from .cloudwatch_client import LogsAPI
    from .config import AgentCoreConfig
    from .models import Diagnostic, IdentityLog, InteractionSnapshot, Span

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
    sent: int = 0
    rejected: int = 0
    uncertain: int = 0
    retryable: int = 0
    delivery_paused: bool = False
    pruned_records: int = 0
    diagnostics: dict[str, int] = field(default_factory=dict)
    outbox_statuses: dict[str, int] = field(default_factory=dict)

    def diagnose(self, code: str) -> None:
        self.diagnostics[code] = self.diagnostics.get(code, 0) + 1

    @property
    def incomplete(self) -> bool:
        """Something staged was not confirmed by Nebuly, or waits for a retry."""
        return bool(
            self.uncertain or self.rejected or self.retryable or self.delivery_paused
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
    for raw in state.records(unprocessed_only=True):
        parsed = parse_record(
            raw, state.source, actor_namespace_template=config.actor_namespace_template
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
            raw, state.source, actor_namespace_template=config.actor_namespace_template
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
) -> None:
    try:
        payload = snapshot_to_payload(
            interaction, source=state.source, config=config, context=context
        )
    except (ValueError, TypeError, OverflowError):
        state.save_trace(
            interaction.trace_id,
            "blocked",
            interaction.earliest_timestamp_ms,
            {**asdict(interaction), "conversion_error": True},
        )
        report.diagnose("conversion_failed")
        return
    existing = state.find_delivery(interaction.interaction_key)
    if existing is not None and existing.status != "pending":
        if existing.payload != payload:
            report.diagnose("late_delivery_discrepancy")
        return
    # snapshot_to_payload accepted it, so both ids are present.
    if not state.claim_conversation(
        str(interaction.session_id), str(interaction.actor_id)
    ):
        logger.warning(
            "Dropping trace %s: its session belongs to another actor",
            interaction.trace_id,
        )
        # Final until new records of the trace arrive: not assembled every run.
        state.save_trace(
            interaction.trace_id,
            "dropped",
            interaction.earliest_timestamp_ms,
            {"conversation_user_conflict": True},
        )
        report.diagnose("conversation_user_conflict")
        return
    state.put_payload(interaction.trace_id, interaction.root_span_id, payload)


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
        assembled.append((start, trace_id, earliest, result, parse_codes))
    # Oldest first, so among the traces staged in this run the session goes to
    # the actor of the oldest one (``claim_conversation``: first staged wins).
    assembled.sort(key=lambda item: (item[0], item[1]))
    for _, trace_id, earliest, result, parse_codes in assembled:
        # Parse diagnostics were counted when their records were first read.
        new_codes = Counter(item.code for item in result.diagnostics) - parse_codes
        for code, count in new_codes.items():
            report.diagnostics[code] = report.diagnostics.get(code, 0) + count
        if not result.interactions:
            state.save_trace(
                trace_id,
                "blocked",
                earliest,
                {"diagnostics": [asdict(item) for item in result.diagnostics]},
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
        state.save_trace(
            trace_id,
            interaction.readiness,
            interaction.earliest_timestamp_ms,
            asdict(interaction),
        )
        if interaction.readiness == "ready":
            _stage(state, interaction, config, context, report)
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
    reconcile = state.reconciliation_due(end, config.reconciliation_interval_seconds)
    windows = plan_reads(
        coverage=coverage,
        requested_start_ms=start,
        end_ms=end,
        overlap_seconds=config.overlap_seconds,
        reconciliation_seconds=config.reconciliation_seconds if reconcile else 0,
    ).windows
    return _Plan(
        windows,
        min(window.start_ms for window in windows),
        max(window.end_ms for window in windows),
        reconcile,
    )


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
    source = SourceScope(config.account_id, config.region, config.source_id)
    report = SyncReport(dry_run=config.dry_run)
    with AgentCoreState(
        config.state_db,
        source,
        config.log_groups,
        dry_run=config.dry_run,
        now_ms=datetime_to_epoch_ms(clock),
    ) as state:
        state.configure_parser(config.actor_namespace_template)
        plan = _plan(state, config, clock)
        context = user_defined.load_context()
        reader = CloudWatchLogReader(
            logs_client if logs_client is not None else create_logs_client(config),
            config.log_groups,
        )
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
        delivery_report = DeliveryReport()
        if not config.dry_run:
            with SafeNebulyClient(
                config.nebuly_api_key,
                config.nebuly_endpoint,
                transport=transport,
                retry_http_429=config.retry_http_429,
            ) as client:
                delivery_report = deliver(
                    state,
                    client,
                    state.pending_deliveries(ended_before_ns=plan.end_ms * 1_000_000),
                    clock_ms=lambda: (
                        datetime_to_epoch_ms(clock)
                        + (
                            int((time.monotonic() - started) * 1000)
                            if now is None
                            else 0
                        )
                    ),
                )
        expired = state.expire_traces(
            plan.end_ms
            - (config.reconciliation_seconds + config.content_retention_seconds) * 1000
        )
        if expired:
            report.diagnostics["expired_trace"] = expired
        report.pruned_records = state.prune_content(config.content_retention_seconds)
        report.outbox_statuses = state.delivery_counts()
        report.sent, report.rejected, report.uncertain = (
            delivery_report.sent,
            delivery_report.rejected,
            delivery_report.uncertain,
        )
        report.retryable = delivery_report.retryable
        report.delivery_paused = delivery_report.paused
    return report
