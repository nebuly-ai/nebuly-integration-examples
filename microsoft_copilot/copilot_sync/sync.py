from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import httpx
from httpx import HTTPStatusError

from .cache import SyncCache, UserCoverage
from .converter import InteractionTurn, SkipReason, group_interactions, turn_to_payload
from .graph_client import AuditFetchProgress, AuditQueryError, GraphClient
from .models import AiInteraction, CopilotAuditRecord, CopilotUser
from .nebuly_client import NebulyClient

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class FirstRunRequiresFromDateError(RuntimeError):
    """Raised when the first sync run is attempted without --from-date."""


@dataclass
class Counts:
    fetched: int = 0
    sent: int = 0
    skipped: int = 0
    empty: int = 0
    failed: int = 0
    users_failed: int = 0
    users_skipped: int = 0
    enriched: int = 0


_AUDIT_RETENTION_DAYS = 180
_AUDIT_QUERY_CONCURRENCY = 5


@dataclass
class SyncSummary:
    users_processed: int = 0
    totals: Counts = field(default_factory=Counts)


def _configure_logging(*, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.DEBUG if verbose else logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)


def _resolve_requested_from(
    config: Config,
    cache: SyncCache,
    coverage: UserCoverage | None,
) -> datetime:
    if config.from_date is not None:
        return config.from_date
    if coverage is not None and coverage.coverage_until is not None:
        return coverage.coverage_until
    min_from = cache.min_coverage_from()
    if min_from is not None:
        return min_from
    raise FirstRunRequiresFromDateError(
        "First run requires --from-date when no sync coverage exists in the cache",
    )


def _eligible_sync_users(
    users: list[CopilotUser],
    cache: SyncCache,
) -> list[CopilotUser]:
    eligible: list[CopilotUser] = []
    for user in users:
        if cache.is_interaction_denied(user.id):
            logger.debug("Skipping denied user %s for sync planning", user.id)
            continue
        eligible.append(user)
    return eligible


def _turn_audit_message_ids(turn: InteractionTurn) -> list[str]:
    ids: list[str] = []
    final = turn.final_response
    if final is not None:
        ids.append(final.id)
        ids.extend(
            response.id for response in turn.responses if response.id != final.id
        )
    ids.append(turn.prompt.id)
    return ids


def _audit_day_chunks(gte: datetime, lte: datetime) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    cursor = gte
    while cursor <= lte:
        chunk_end = min(cursor + timedelta(days=1) - timedelta(microseconds=1), lte)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(microseconds=1)
    return chunks


def _audit_chunks_to_fetch(
    chunks: list[tuple[datetime, datetime]],
    *,
    cache: SyncCache,
    settle_edge: datetime,
) -> list[tuple[datetime, datetime]]:
    pending: list[tuple[datetime, datetime]] = []
    for chunk_gte, chunk_lte in chunks:
        if chunk_lte > settle_edge:
            pending.append((chunk_gte, chunk_lte))
            continue
        if not cache.has_audit_day_chunk(chunk_gte):
            pending.append((chunk_gte, chunk_lte))
    return pending


def _plan_audit_window(
    users: list[CopilotUser],
    config: Config,
    cache: SyncCache,
    run_until: datetime,
) -> tuple[datetime, datetime] | None:
    gte_min: datetime | None = None
    lte_max: datetime | None = None
    for user in users:
        if cache.is_interaction_denied(user.id):
            continue
        coverage = cache.get_user_coverage(user.id)
        requested_from = _resolve_requested_from(config, cache, coverage)
        for interval in cache.plan_intervals(coverage, requested_from, run_until):
            if gte_min is None or interval.gte < gte_min:
                gte_min = interval.gte
            if lte_max is None or interval.lte > lte_max:
                lte_max = interval.lte
    if gte_min is None or lte_max is None:
        return None
    retention_floor = run_until - timedelta(days=_AUDIT_RETENTION_DAYS)
    if gte_min < retention_floor:
        logger.warning(
            "Audit window start %s clamped to %s (180-day retention)",
            gte_min.isoformat(),
            retention_floor.isoformat(),
        )
        gte_min = retention_floor
    if gte_min > lte_max:
        return None
    return gte_min, lte_max


async def _load_audit_records(
    graph: GraphClient,
    cache: SyncCache,
    config: Config,
    users: list[CopilotUser],
    run_until: datetime,
) -> None:
    window = _plan_audit_window(users, config, cache, run_until)
    if window is None:
        return
    gte, lte = window
    all_chunks = _audit_day_chunks(gte, lte)
    settle_edge = run_until - timedelta(seconds=config.audit_settle_lag_seconds)
    chunks = _audit_chunks_to_fetch(
        all_chunks,
        cache=cache,
        settle_edge=settle_edge,
    )
    if not chunks:
        logger.info(
            "Audit cache covers %s to %s (%d day-chunk(s); tail after %s may refresh)",
            gte.isoformat(),
            lte.isoformat(),
            len(all_chunks),
            settle_edge.isoformat(),
        )
        return

    progress = AuditFetchProgress()
    progress.mark_started(asyncio.get_running_loop())
    logger.info(
        "Loading Copilot audit records for %s to %s (%d/%d day-chunk(s), "
        "up to %d concurrent)",
        gte.isoformat(),
        lte.isoformat(),
        len(chunks),
        len(all_chunks),
        _AUDIT_QUERY_CONCURRENCY,
    )
    sem = asyncio.Semaphore(_AUDIT_QUERY_CONCURRENCY)

    async def fetch_chunk(
        chunk_gte: datetime,
        chunk_lte: datetime,
    ) -> list[CopilotAuditRecord]:
        async with sem:
            return await graph.fetch_copilot_audit_records(
                chunk_gte,
                chunk_lte,
                poll_interval=float(config.audit_poll_interval_seconds),
                query_timeout_seconds=float(config.audit_query_timeout_seconds),
                progress=progress,
            )

    results = await asyncio.gather(
        *[fetch_chunk(c_gte, c_lte) for c_gte, c_lte in chunks],
        return_exceptions=True,
    )
    loaded_records = 0
    failed_chunks = 0
    for chunk, result in zip(chunks, results, strict=True):
        if isinstance(result, BaseException):
            failed_chunks += 1
            logger.warning(
                "Audit fetch failed for %s-%s: %s",
                chunk[0].isoformat(),
                chunk[1].isoformat(),
                result,
            )
            continue
        cache.upsert_audit_records(result)
        if chunk[1] <= settle_edge:
            cache.mark_audit_day_chunk(chunk[0], chunk[1])
        loaded_records += len(result)
    cache.commit()
    logger.info(
        "Audit enrichment load finished: %d record(s) cached, %d/%d chunk(s) ok",
        loaded_records,
        len(chunks) - failed_chunks,
        len(chunks),
    )


async def _try_audit_enrichment(
    graph: GraphClient,
    cache: SyncCache,
    config: Config,
    users: list[CopilotUser],
    run_until: datetime,
) -> None:
    if not config.audit_enrichment:
        return
    try:
        await _load_audit_records(
            graph,
            cache,
            config,
            users,
            run_until,
        )
    except HTTPStatusError as exc:
        if exc.response.status_code == 403:
            logger.warning(
                "Audit enrichment disabled: missing AuditLogsQuery.Read.All",
            )
        else:
            logger.warning("Audit enrichment skipped: %s", exc)
    except AuditQueryError as exc:
        logger.warning("Audit enrichment skipped: %s", exc)
    except Exception:
        logger.exception("Audit enrichment skipped due to unexpected error")


async def _send_turns(
    turns: list[InteractionTurn],
    *,
    user: CopilotUser,
    nebuly: NebulyClient,
    config: Config,
    cache: SyncCache,
    counts: Counts,
    is_tail: bool,
    settle_edge: datetime,
) -> tuple[list[datetime], list[datetime]]:
    """Send settled turns and return deferred in-flight and failed turn start times."""
    in_flight_starts: list[datetime] = []
    failed_starts: list[datetime] = []
    for turn in turns:
        if is_tail and turn.time_end > settle_edge:
            # Hold back to time_start (not time_end): re-fetch filters on the prompt's
            # created_datetime, and group_interactions needs that prompt to rebuild the
            # turn.
            in_flight_starts.append(turn.time_start)
            continue
        counts.fetched += 1
        audit = None
        if config.audit_enrichment:
            audit = cache.find_audit_record(_turn_audit_message_ids(turn))
            if audit is not None:
                counts.enriched += 1
        result = turn_to_payload(
            turn,
            user=user,
            anonymize=config.anonymize,
            audit=audit,
        )
        if isinstance(result, SkipReason):
            if result is SkipReason.EMPTY_OUTPUT:
                counts.empty += 1
            else:
                counts.skipped += 1
            continue
        if config.dry_run:
            counts.sent += 1
            continue
        try:
            await nebuly.send_interaction(result)
            counts.sent += 1
        except Exception:
            logger.exception("Failed to send turn for user %s", user.id)
            counts.failed += 1
            failed_starts.append(turn.time_start)
    return in_flight_starts, failed_starts


async def _sync_user(
    user: CopilotUser,
    *,
    config: Config,
    graph: GraphClient,
    nebuly: NebulyClient,
    cache: SyncCache,
    run_until: datetime,
) -> Counts:
    counts = Counts()
    if cache.is_interaction_denied(user.id):
        return counts
    coverage = cache.get_user_coverage(user.id)
    requested_from = _resolve_requested_from(config, cache, coverage)
    intervals = cache.plan_intervals(coverage, requested_from, run_until)

    if not intervals:
        logger.debug("No intervals for user %s", user.id)
        return counts

    logger.info("Processing user %s (%d interval(s))", user.id, len(intervals))

    run_hold_back: list[datetime] = []

    for interval in intervals:
        try:
            raw = await graph.fetch_interactions(
                user_id=user.id, gte=interval.gte, lte=interval.lte
            )
        except HTTPStatusError as exc:
            if exc.response.status_code == 403:
                logger.warning("403 for user %s — skipping", user.email)
                cache.mark_interaction_denied(user.id, reason="interaction_403")
                cache.commit()
                counts.users_skipped += 1
                return counts
            raise

        interactions = sorted(
            [AiInteraction.model_validate(item) for item in raw],
            key=lambda x: x.created_datetime,
        )
        turns, dangling_prompts = group_interactions(interactions)
        is_tail = interval.lte == run_until
        settle_lag = config.settle_lag_seconds
        if config.audit_enrichment:
            settle_lag = max(settle_lag, config.audit_settle_lag_seconds)
        settle_edge = interval.lte - timedelta(seconds=settle_lag)
        in_flight_starts, failed_starts = await _send_turns(
            turns,
            user=user,
            nebuly=nebuly,
            config=config,
            cache=cache,
            counts=counts,
            is_tail=is_tail,
            settle_edge=settle_edge,
        )

        interval_hold_back = [p.created_datetime for p in dangling_prompts]
        if is_tail:
            interval_hold_back += in_flight_starts
        interval_hold_back += failed_starts
        run_hold_back.extend(interval_hold_back)

    if run_hold_back:
        cache.save_user_coverage(
            user.id,
            requested_from,
            run_until,
            hold_back=min(run_hold_back),
        )
    else:
        cache.save_user_coverage(user.id, requested_from, run_until)
    cache.commit()

    return counts


async def run_sync(config: Config) -> SyncSummary:
    _configure_logging(verbose=config.verbose)

    cache = SyncCache(
        config.cache_dir / "sync_state.db",
        config.azure_tenant_id,
        dry_run=config.dry_run,
    )
    run_until = config.run_until()
    summary = SyncSummary()
    graph: GraphClient | None = None

    try:
        if config.from_date is None and not cache.has_any_coverage():
            raise FirstRunRequiresFromDateError(
                "First run requires --from-date when no sync coverage "
                "exists in the cache",
            )

        graph = GraphClient(
            tenant_id=config.azure_tenant_id,
            client_id=config.azure_client_id,
            client_secret=config.azure_client_secret,
            copilot_sku=config.copilot_sku,
            max_requests_per_minute=config.graph_max_requests_per_minute,
        )

        async with httpx.AsyncClient(timeout=60.0) as nebuly_http:
            nebuly = NebulyClient(
                nebuly_http,
                config.nebuly_api_key,
                config.nebuly_endpoint,
            )

            licensed = await graph.list_copilot_users()
            users = _eligible_sync_users(licensed, cache)
            denied_count = len(licensed) - len(users)
            if denied_count:
                logger.info(
                    "Found %d Copilot user(s) ready to sync "
                    "(%d excluded after prior 403)",
                    len(users),
                    denied_count,
                )
            else:
                logger.info("Found %d Copilot-licensed users", len(users))

            await _try_audit_enrichment(graph, cache, config, users, run_until)

            for user in sorted(users, key=lambda u: u.id):
                try:
                    user_counts = await _sync_user(
                        user,
                        config=config,
                        graph=graph,
                        nebuly=nebuly,
                        cache=cache,
                        run_until=run_until,
                    )
                except Exception:
                    logger.exception("Sync failed for user %s", user.email)
                    summary.totals.users_failed += 1
                    continue
                summary.users_processed += 1
                summary.totals.fetched += user_counts.fetched
                summary.totals.sent += user_counts.sent
                summary.totals.skipped += user_counts.skipped
                summary.totals.empty += user_counts.empty
                summary.totals.failed += user_counts.failed
                summary.totals.users_skipped += user_counts.users_skipped
                summary.totals.enriched += user_counts.enriched
    finally:
        if graph is not None:
            await graph.close()
        cache.close()

    logger.info(
        "Sync complete: users=%d skipped=%d | interactions fetched=%d sent=%d "
        "skipped=%d empty=%d failed=%d enriched=%d users_failed=%d",
        summary.users_processed,
        summary.totals.users_skipped,
        summary.totals.fetched,
        summary.totals.sent,
        summary.totals.skipped,
        summary.totals.empty,
        summary.totals.failed,
        summary.totals.enriched,
        summary.totals.users_failed,
    )
    return summary
