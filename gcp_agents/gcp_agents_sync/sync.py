from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from .cache import SyncCache
from .config import Config, timestamp_str_to_datetime
from .converter import SkipReason, Turn, group_turns, turn_to_payload
from .gcp_client import GcpClient
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
    update_time = timestamp_str_to_datetime(session.update_time)
    if from_date is not None and update_time < from_date:
        return False
    return not (to_date is not None and update_time > to_date)


def _turn_after_watermark(
    turn: Turn,
    *,
    last_sent_event_time: datetime | None,
    last_sent_invocation_id: str | None,
) -> bool:
    if last_sent_event_time is None:
        return True
    if turn.time_end > last_sent_event_time:
        return True
    if turn.time_end == last_sent_event_time:
        return turn.invocation_id > (last_sent_invocation_id or "")
    return False


def _sync_session(
    session: Session,
    *,
    gcp: GcpClient,
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
    events = gcp.list_events(session)
    turns = group_turns(events)
    last_sent_event_time = cache.last_sent_event_datetime(session.session_id)
    last_sent_invocation_id = cache.last_sent_invocation_id(session.session_id)
    settle_edge = now - timedelta(seconds=config.settle_lag_seconds)
    held_back = False

    for turn in turns:
        if not _turn_after_watermark(
            turn,
            last_sent_event_time=last_sent_event_time,
            last_sent_invocation_id=last_sent_invocation_id,
        ):
            continue

        if turn.time_end > settle_edge:
            counts.turns_held_back += 1
            held_back = True
            break

        counts.turns_fetched += 1
        result = turn_to_payload(turn, session=session, anonymize=config.anonymize)
        if isinstance(result, SkipReason):
            counts.turns_skipped += 1
            cache.advance_watermark(
                session,
                event_time=turn.time_end,
                invocation_id=turn.invocation_id,
            )
            last_sent_event_time = turn.time_end
            last_sent_invocation_id = turn.invocation_id
            continue

        try:
            nebuly.send_interaction(result)
            counts.turns_sent += 1
            cache.checkpoint(
                session,
                event_time=turn.time_end,
                invocation_id=turn.invocation_id,
            )
            last_sent_event_time = turn.time_end
            last_sent_invocation_id = turn.invocation_id
        except Exception:
            logger.exception(
                "Failed to send turn for session %s invocation %s",
                session.session_id,
                turn.invocation_id,
            )
            counts.turns_failed += 1
            cache.mark_partial(session, error="send_failed")
            return

    if held_back:
        cache.mark_partial(session)
    else:
        cache.mark_complete(session)


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
        config.reasoning_engine_key,
        dry_run=config.dry_run,
    )
    now = datetime.now(UTC)

    try:
        with httpx.Client(timeout=60.0) as http_client:
            gcp = GcpClient(
                config.gcp_project_id,
                config.gcp_location,
                config.gcp_reasoning_engine_id,
                max_requests_per_minute=config.gcp_max_requests_per_minute,
            )
            nebuly = NebulyClient(
                http_client,
                config.nebuly_api_key,
                config.nebuly_endpoint,
                dry_run=config.dry_run,
            )

            for session in gcp.list_sessions():
                if not _session_in_date_range(
                    session,
                    from_date=config.from_date,
                    to_date=config.run_until(),
                ):
                    continue
                _sync_session(
                    session,
                    gcp=gcp,
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
