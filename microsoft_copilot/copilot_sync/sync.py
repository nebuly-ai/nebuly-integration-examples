from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import httpx
from httpx import HTTPStatusError

from .cache import SyncCache, UserCoverage
from .converter import pair_interactions, pair_to_payload
from .graph_client import GraphClient
from .models import AiInteraction, CopilotUser
from .nebuly_client import NebulyClient

if TYPE_CHECKING:
    from datetime import datetime

    from .config import Config

logger = logging.getLogger(__name__)


class FirstRunRequiresFromDateError(RuntimeError):
    """Raised when the first sync run is attempted without --from-date."""


@dataclass
class Counts:
    fetched: int = 0
    sent: int = 0
    skipped: int = 0
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

    try:
        for interval in intervals:
            try:
                raw = await graph.fetch_interactions(
                    user.id,
                    interval.gte,
                    interval.lte,
                )
            except HTTPStatusError as exc:
                if exc.response.status_code == 403:
                    logger.warning("403 for user %s — skipping", user.email)
                    counts.users_skipped += 1
                    return counts
                raise

            interactions = [AiInteraction.model_validate(item) for item in raw]
            pairs = pair_interactions(interactions)
            counts.fetched += len(pairs)

            for pair in pairs:
                payload = pair_to_payload(pair, user=user, anonymize=config.anonymize)
                if payload is None:
                    counts.skipped += 1
                    continue
                if config.dry_run:
                    counts.sent += 1
                    continue
                await nebuly.send_interaction(payload)
                counts.sent += 1

        cache.save_user_coverage(user.id, requested_from, run_until)
        cache.commit()
    except Exception:
        counts.failed += 1
        raise

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
            config.azure_tenant_id,
            config.azure_client_id,
            config.azure_client_secret,
            config.copilot_sku,
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
                user_counts = await _sync_user(
                    user,
                    config=config,
                    graph=graph,
                    nebuly=nebuly,
                    cache=cache,
                    run_until=run_until,
                )
                summary.users_processed += 1
                summary.totals.fetched += user_counts.fetched
                summary.totals.sent += user_counts.sent
                summary.totals.skipped += user_counts.skipped
                summary.totals.failed += user_counts.failed
                summary.totals.users_skipped += user_counts.users_skipped
    finally:
        if graph is not None:
            await graph.close()
        cache.close()

    logger.info(
        "Sync complete: users=%d skipped=%d | interactions fetched=%d sent=%d "
        "skipped=%d failed=%d",
        summary.users_processed,
        summary.totals.users_skipped,
        summary.totals.fetched,
        summary.totals.sent,
        summary.totals.skipped,
        summary.totals.failed,
    )
    return summary
