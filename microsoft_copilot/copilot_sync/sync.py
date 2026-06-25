from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

import httpx
from httpx import HTTPStatusError

from .cache import SyncCache, UserCoverage
from .converter import SkipReason, group_interactions, turn_to_payload
from .graph_client import GraphClient
from .models import AiInteraction, CopilotUser
from .nebuly_client import NebulyClient

if TYPE_CHECKING:
    from datetime import datetime

    from .config import Config
    from .converter import InteractionTurn

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
    users_skipped: int = 0


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


async def _send_turns(
    turns: list[InteractionTurn],
    *,
    user: CopilotUser,
    nebuly: NebulyClient,
    config: Config,
    counts: Counts,
    is_tail: bool,
    settle_edge: datetime,
) -> list[datetime]:
    """Send settled turns and return the start times of deferred in-flight turns."""
    in_flight_starts: list[datetime] = []
    for turn in turns:
        if is_tail and turn.time_end > settle_edge:
            in_flight_starts.append(turn.time_start)
            continue
        counts.fetched += 1
        result = turn_to_payload(turn, user=user, anonymize=config.anonymize)
        if isinstance(result, SkipReason):
            if result is SkipReason.EMPTY_OUTPUT:
                counts.empty += 1
            else:
                counts.skipped += 1
            continue
        if config.dry_run:
            counts.sent += 1
            continue
        await nebuly.send_interaction(result)
        counts.sent += 1
    return in_flight_starts


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
    coverage = cache.get_user_coverage(user.id)
    requested_from = _resolve_requested_from(config, cache, coverage)
    intervals = cache.plan_intervals(coverage, requested_from, run_until)

    if not intervals:
        logger.debug("No intervals for user %s", user.email)
        return counts

    logger.info("Processing user %s (%d interval(s))", user.email, len(intervals))

    for interval in intervals:
        try:
            raw = await graph.fetch_interactions(
                user_id=user.id, gte=interval.gte, lte=interval.lte
            )
        except HTTPStatusError as exc:
            if exc.response.status_code == 403:
                logger.warning("403 for user %s — skipping", user.email)
                counts.users_skipped += 1
                return counts
            raise

        interactions = sorted(
            [AiInteraction.model_validate(item) for item in raw],
            key=lambda x: x.created_datetime,
        )
        turns, dangling_prompts = group_interactions(interactions)
        is_tail = interval.lte == run_until
        settle_edge = interval.lte - timedelta(seconds=config.settle_lag_seconds)
        in_flight_starts = await _send_turns(
            turns,
            user=user,
            nebuly=nebuly,
            config=config,
            counts=counts,
            is_tail=is_tail,
            settle_edge=settle_edge,
        )

        hold_back = [p.created_datetime for p in dangling_prompts]
        if is_tail:
            hold_back += in_flight_starts
        if hold_back:
            earliest = min(hold_back)
            coverage_until = max(earliest - timedelta(microseconds=1), interval.gte)
        else:
            coverage_until = interval.lte

        cache.save_user_coverage(user.id, requested_from, coverage_until)
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
                dry_run=config.dry_run,
            )

            users = await graph.list_copilot_users()
            logger.info("Found %d Copilot-licensed users", len(users))

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
                    summary.totals.failed += 1
                    continue
                summary.users_processed += 1
                summary.totals.fetched += user_counts.fetched
                summary.totals.sent += user_counts.sent
                summary.totals.skipped += user_counts.skipped
                summary.totals.empty += user_counts.empty
                summary.totals.users_skipped += user_counts.users_skipped
    finally:
        if graph is not None:
            await graph.close()
        cache.close()

    logger.info(
        "Sync complete: users=%d skipped=%d | interactions fetched=%d sent=%d "
        "skipped=%d empty=%d failed=%d",
        summary.users_processed,
        summary.totals.users_skipped,
        summary.totals.fetched,
        summary.totals.sent,
        summary.totals.skipped,
        summary.totals.empty,
        summary.totals.failed,
    )
    return summary
