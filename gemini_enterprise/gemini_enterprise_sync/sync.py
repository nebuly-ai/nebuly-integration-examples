from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from .cache import SyncCache
from .config import Config, timestamp_str_to_datetime
from .converter import SkipReason, Turn, session_to_turns, turn_to_payload
from .discovery_client import DiscoveryClient
from .nebuly_client import NebulyClient

if TYPE_CHECKING:
    from .models import Session

logger = logging.getLogger(__name__)


@dataclass
class Counts:
    sessions_fetched: int = 0
    sessions_skipped: int = 0
    turns_fetched: int = 0
    turns_sent: int = 0
    turns_skipped: int = 0
    turns_held_back: int = 0
    turns_failed: int = 0


@dataclass
class SyncSummary:
    totals: Counts = field(default_factory=Counts)


def _session_in_date_range(
    session: Session, *, from_date: datetime | None, to_date: datetime | None
) -> bool:
    if not session.end_time:
        return True
    end_time = timestamp_str_to_datetime(session.end_time)
    if from_date is not None and end_time < from_date:
        return False
    return not (to_date is not None and end_time > to_date)


def _turn_after_watermark(
    turn: Turn,
    *,
    session: Session,
    last_sent_turn_time: datetime | None,
    last_sent_query_id: str | None,
) -> bool:
    if last_sent_turn_time is None:
        return True
    turn_end = turn.time_end(session)
    if turn_end > last_sent_turn_time:
        return True
    if turn_end == last_sent_turn_time:
        return turn.query_id > (last_sent_query_id or "")
    return False


def _sync_session(
    session: Session,
    *,
    discovery: DiscoveryClient,
    nebuly: NebulyClient,
    cache: SyncCache,
    config: Config,
    counts: Counts,
    now: datetime,
) -> None:
    if cache.should_skip_fetch(session):
        counts.sessions_skipped += 1
        logger.debug("Skipping unchanged session %s", session.session_id)
        return

    counts.sessions_fetched += 1
    full_session = discovery.get_session(session)
    turns = session_to_turns(full_session)
    last_sent_turn_time = cache.last_sent_turn_datetime(session.session_id)
    last_sent_query_id = cache.last_sent_query_id(session.session_id)
    settle_edge = now - timedelta(seconds=config.settle_lag_seconds)
    held_back = False

    for turn in turns:
        if not _turn_after_watermark(
            turn,
            session=full_session,
            last_sent_turn_time=last_sent_turn_time,
            last_sent_query_id=last_sent_query_id,
        ):
            continue

        if turn.time_end(full_session) > settle_edge:
            counts.turns_held_back += 1
            held_back = True
            break

        counts.turns_fetched += 1
        result = turn_to_payload(turn, session=full_session, anonymize=config.anonymize)
        if isinstance(result, SkipReason):
            counts.turns_skipped += 1
            cache.advance_watermark(
                full_session,
                turn_time=turn.time_end(full_session),
                query_id=turn.query_id,
            )
            last_sent_turn_time = turn.time_end(full_session)
            last_sent_query_id = turn.query_id
            continue

        try:
            nebuly.send_interaction(result)
            counts.turns_sent += 1
            cache.checkpoint(
                full_session,
                turn_time=turn.time_end(full_session),
                query_id=turn.query_id,
            )
            last_sent_turn_time = turn.time_end(full_session)
            last_sent_query_id = turn.query_id
        except Exception:
            logger.exception(
                "Failed to send turn for session %s query %s",
                full_session.session_id,
                turn.query_id,
            )
            counts.turns_failed += 1
            cache.mark_partial(full_session, error="send_failed")
            return

    if held_back:
        cache.mark_partial(full_session)
    else:
        cache.mark_complete(full_session)


def run_sync(config: Config) -> SyncSummary:
    logging.basicConfig(
        level=logging.DEBUG if config.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if config.verbose:
        logging.getLogger("httpx").setLevel(logging.DEBUG)

    summary = SyncSummary()
    config.cache_dir.mkdir(parents=True, exist_ok=True)
    cache = SyncCache(
        config.cache_dir / "sync_state.db",
        config.engine_key,
        dry_run=config.dry_run,
    )
    now = datetime.now(UTC)

    try:
        with httpx.Client(timeout=60.0) as http_client:
            discovery = DiscoveryClient(
                config.gcp_project_id,
                config.gcp_location,
                config.gcp_collection,
                config.gcp_engine_id,
                max_requests_per_minute=config.gcp_max_requests_per_minute,
            )
            nebuly = NebulyClient(
                http_client,
                config.nebuly_api_key,
                config.nebuly_endpoint,
                dry_run=config.dry_run,
            )

            for session in discovery.list_sessions(since=config.from_date):
                if not _session_in_date_range(
                    session,
                    from_date=config.from_date,
                    to_date=config.run_until(),
                ):
                    continue
                _sync_session(
                    session,
                    discovery=discovery,
                    nebuly=nebuly,
                    cache=cache,
                    config=config,
                    counts=summary.totals,
                    now=now,
                )
    finally:
        cache.close()

    totals = summary.totals
    logger.info(
        "Sync complete: sessions_fetched=%s sessions_skipped=%s turns_sent=%s "
        "turns_skipped=%s turns_held_back=%s turns_failed=%s",
        totals.sessions_fetched,
        totals.sessions_skipped,
        totals.turns_sent,
        totals.turns_skipped,
        totals.turns_held_back,
        totals.turns_failed,
    )
    return summary
