from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
from httpx import HTTPStatusError

from .cache import ChatState, ChatWorkPlan, SyncCache
from .compliance_client import ComplianceClient
from .config import Config, datetime_to_timestamp_str
from .converter import MessagePair, build_message_pairs, pair_to_payload
from .models import ChatSummary
from .nebuly_client import NebulyClient

logger = logging.getLogger(__name__)


@dataclass
class Counts:
    fetched: int = 0
    sent: int = 0
    skipped: int = 0
    failed: int = 0
    chats_processed: int = 0
    chats_skipped: int = 0


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


def _merge_coverage(
    state_from: datetime | None,
    state_until: datetime | None,
    requested_from: datetime | None,
    run_until: datetime,
) -> tuple[datetime | None, datetime]:
    from_candidates = [x for x in [state_from, requested_from] if x is not None]
    new_from = min(from_candidates) if from_candidates else None
    until_candidates = [x for x in [state_until, run_until] if x is not None]
    new_until = max(until_candidates) if until_candidates else run_until
    return new_from, new_until


def run_sync(config: Config) -> SyncSummary:
    _configure_logging(verbose=config.verbose)

    cache = SyncCache(
        config.cache_dir / "sync_state.db",
        config.organization_uuid,
        dry_run=config.dry_run,
    )
    run_until = config.run_until()

    summary = SyncSummary()

    try:
        with httpx.Client(
            base_url=config.compliance_base_url, timeout=60.0
        ) as compliance_http:
            compliance = ComplianceClient(
                compliance_http,
                config.compliance_api_key,
                max_requests_per_minute=config.compliance_max_requests_per_minute,
            )

            with httpx.Client(timeout=60.0) as nebuly_http:
                nebuly = NebulyClient(
                    nebuly_http,
                    config.nebuly_api_key,
                    config.nebuly_endpoint,
                    dry_run=config.dry_run,
                )

                users = compliance.list_all_users(config.organization_uuid)
                logger.info("Fetched %d users from Compliance API", len(users))

                for user in sorted(users, key=lambda u: u.id):
                    user_counts = _sync_user(
                        user_id=user.id,
                        config=config,
                        compliance=compliance,
                        nebuly=nebuly,
                        cache=cache,
                        run_until=run_until,
                    )
                    summary.users_processed += 1
                    summary.totals.fetched += user_counts.fetched
                    summary.totals.sent += user_counts.sent
                    summary.totals.skipped += user_counts.skipped
                    summary.totals.failed += user_counts.failed
                    summary.totals.chats_processed += user_counts.chats_processed
                    summary.totals.chats_skipped += user_counts.chats_skipped
    finally:
        cache.close()

    logger.info(
        "Sync complete: users=%d | chats processed=%d skipped=%d | "
        "interactions fetched=%d sent=%d skipped=%d failed=%d",
        summary.users_processed,
        summary.totals.chats_processed,
        summary.totals.chats_skipped,
        summary.totals.fetched,
        summary.totals.sent,
        summary.totals.skipped,
        summary.totals.failed,
    )
    return summary


@dataclass
class ExportOutcome:
    fetched: int = 0
    sent: int = 0
    skipped: int = 0
    failed: int = 0
    user_failed: bool = False
    chat_failed: bool = False
    exported_until: datetime | None = None
    exported_until_msg_id: str | None = None


def _at_or_before_watermark(
    ts: datetime,
    msg_id: str,
    watermark_ts: datetime | None,
    watermark_msg_id: str | None,
) -> bool:
    if watermark_ts is None:
        return False
    if ts < watermark_ts:
        return True
    if ts > watermark_ts:
        return False
    if watermark_msg_id is None:
        return True
    return msg_id <= watermark_msg_id


def _at_or_after_watermark(
    ts: datetime,
    msg_id: str,
    watermark_ts: datetime | None,
    watermark_msg_id: str | None,
) -> bool:
    if watermark_ts is None:
        return False
    if ts > watermark_ts:
        return True
    if ts < watermark_ts:
        return False
    if watermark_msg_id is None:
        return True
    return msg_id >= watermark_msg_id


def _paginate_chats_for_user(
    compliance: ComplianceClient,
    user_id: str,
    updated_at_gte: str | None,
    updated_at_lte: str,
) -> dict[str, ChatSummary]:
    chats_by_id: dict[str, ChatSummary] = {}
    after_id: str | None = None
    while True:
        chats_page = compliance.list_chats(
            [user_id],
            updated_at_gte=updated_at_gte,
            updated_at_lte=updated_at_lte,
            after_id=after_id,
        )
        for chat in chats_page.data:
            chats_by_id[chat.id] = chat
        if not chats_page.has_more:
            break
        after_id = chats_page.last_id
        if after_id is None:
            break
    return chats_by_id


def _recover_missing_chats(
    *,
    compliance: ComplianceClient,
    cache: SyncCache,
    user_id: str,
    chats_by_id: dict[str, ChatSummary],
    dry_run: bool,
) -> None:
    unfinished = cache.iter_unfinished_chats(user_id)
    missing = {cid for cid in unfinished if cid not in chats_by_id}
    if not missing:
        return
    recovery_after_id: str | None = None
    while missing:
        extra_page = compliance.list_chats([user_id], after_id=recovery_after_id)
        for chat in extra_page.data:
            if chat.id in missing:
                chats_by_id[chat.id] = chat
                missing.discard(chat.id)
        if not extra_page.has_more or extra_page.last_id is None:
            break
        recovery_after_id = extra_page.last_id
    for cid in missing:
        cache.mark_chat_deleted(cid, "Not found in chat listing")
    if missing and not dry_run:
        cache.commit()


def _collect_chats_for_user(
    *,
    user_id: str,
    compliance: ComplianceClient,
    cache: SyncCache,
    requested_from: datetime | None,
    run_until: datetime,
    dry_run: bool,
) -> list[ChatSummary]:
    updated_at_gte = (
        datetime_to_timestamp_str(requested_from) if requested_from else None
    )
    updated_at_lte = datetime_to_timestamp_str(run_until)
    # requested_from drives both the chat updated_at.gte listing window and
    # message backfill start; coverage_from <= updated_at for cached chats, so
    # backfill-eligible chats are always included in the listing.

    chats_by_id = _paginate_chats_for_user(
        compliance, user_id, updated_at_gte, updated_at_lte
    )

    _recover_missing_chats(
        compliance=compliance,
        cache=cache,
        user_id=user_id,
        chats_by_id=chats_by_id,
        dry_run=dry_run,
    )

    return sorted(chats_by_id.values(), key=lambda c: (c.updated_at, c.id))


def _highest_from_skipped_chat(
    cache: SyncCache,
    chat_id: str,
    highest_completed: datetime | None,
) -> datetime | None:
    state = cache.get_chat_state(chat_id)
    if (
        state
        and state.last_exported_chat_updated_at
        and (
            highest_completed is None
            or state.last_exported_chat_updated_at > highest_completed
        )
    ):
        return state.last_exported_chat_updated_at
    return highest_completed


def _pair_already_exported(
    *,
    is_backfill: bool,
    assistant_ts: datetime,
    assistant_id: str,
    prior_coverage_from: datetime | None,
    prior_coverage_from_msg_id: str | None,
    prior_coverage_until: datetime | None,
    prior_coverage_until_msg_id: str | None,
) -> bool:
    if is_backfill:
        return _at_or_after_watermark(
            assistant_ts,
            assistant_id,
            prior_coverage_from,
            prior_coverage_from_msg_id,
        )
    return _at_or_before_watermark(
        assistant_ts,
        assistant_id,
        prior_coverage_until,
        prior_coverage_until_msg_id,
    )


def _handle_send_interaction_failure(
    *,
    chat: ChatSummary,
    cache: SyncCache,
    config: Config,
    user_id: str,
    outcome: ExportOutcome,
    is_backfill: bool,
) -> None:
    outcome.failed += 1
    outcome.user_failed = True
    outcome.chat_failed = True
    if is_backfill:
        cache.mark_chat_failed(chat.id, "HTTP error sending interaction")
    else:
        cache.mark_chat_failed(
            chat.id,
            "HTTP error sending interaction",
            new_coverage_until=outcome.exported_until,
            new_coverage_until_msg_id=outcome.exported_until_msg_id,
        )
    if not config.dry_run:
        cache.commit()
    logger.error(
        "Failed to send interaction for user=%s chat=%s; stopping user to allow resume",
        user_id,
        chat.id,
    )


def _checkpoint_after_send(
    *,
    chat_id: str,
    cache: SyncCache,
    outcome: ExportOutcome,
    msg_ts: datetime,
    msg_id: str,
    is_backfill: bool,
) -> None:
    if is_backfill:
        cache.checkpoint_chat_coverage_from(chat_id, msg_ts, msg_id)
        return
    if outcome.exported_until is None or msg_ts > outcome.exported_until:
        outcome.exported_until = msg_ts
        outcome.exported_until_msg_id = msg_id
    elif msg_ts == outcome.exported_until and (
        outcome.exported_until_msg_id is None or msg_id > outcome.exported_until_msg_id
    ):
        outcome.exported_until_msg_id = msg_id
    cache.checkpoint_chat_coverage_until(
        chat_id, outcome.exported_until, outcome.exported_until_msg_id or msg_id
    )


def _send_chat_pairs(
    *,
    chat: ChatSummary,
    pairs: list[MessagePair],
    nebuly: NebulyClient,
    cache: SyncCache,
    config: Config,
    user_id: str,
    outcome: ExportOutcome,
    prior_coverage_from: datetime | None = None,
    prior_coverage_from_msg_id: str | None = None,
    prior_coverage_until: datetime | None = None,
    prior_coverage_until_msg_id: str | None = None,
    is_backfill: bool = False,
) -> bool:
    """Send message pairs for one interval. Returns True if export should stop."""
    ordered_pairs = reversed(pairs) if is_backfill else pairs
    for pair in ordered_pairs:
        assistant_ts = pair.assistant_message.created_at
        assistant_id = pair.assistant_message.id
        if _pair_already_exported(
            is_backfill=is_backfill,
            assistant_ts=assistant_ts,
            assistant_id=assistant_id,
            prior_coverage_from=prior_coverage_from,
            prior_coverage_from_msg_id=prior_coverage_from_msg_id,
            prior_coverage_until=prior_coverage_until,
            prior_coverage_until_msg_id=prior_coverage_until_msg_id,
        ):
            outcome.skipped += 1
            continue

        outcome.fetched += 1
        payload = pair_to_payload(pair, anonymize=config.anonymize)
        if payload is None:
            outcome.skipped += 1
            continue

        try:
            nebuly.send_interaction(payload)
        except (HTTPStatusError, httpx.RequestError):
            _handle_send_interaction_failure(
                chat=chat,
                cache=cache,
                config=config,
                user_id=user_id,
                outcome=outcome,
                is_backfill=is_backfill,
            )
            return True

        outcome.sent += 1
        msg_ts = pair.assistant_message.created_at
        msg_id = pair.assistant_message.id
        _checkpoint_after_send(
            chat_id=chat.id,
            cache=cache,
            outcome=outcome,
            msg_ts=msg_ts,
            msg_id=msg_id,
            is_backfill=is_backfill,
        )
        if not config.dry_run:
            cache.commit()
    return False


def _export_chat_intervals(
    *,
    chat: ChatSummary,
    plan: ChatWorkPlan,
    compliance: ComplianceClient,
    nebuly: NebulyClient,
    cache: SyncCache,
    config: Config,
    user_id: str,
    prior_state: ChatState | None,
) -> ExportOutcome:
    outcome = ExportOutcome(
        exported_until=(
            prior_state.coverage_until
            if prior_state and prior_state.status == "completed"
            else None
        ),
        exported_until_msg_id=(
            prior_state.coverage_until_msg_id
            if prior_state and prior_state.status == "completed"
            else None
        ),
    )
    cache.mark_chat_in_progress(chat)

    for interval in plan.intervals:
        try:
            chat_response = compliance.list_chat_messages(
                chat.id,
                created_at_gte=(
                    datetime_to_timestamp_str(interval.created_at_gte)
                    if interval.created_at_gte is not None
                    else None
                ),
                created_at_lte=datetime_to_timestamp_str(interval.created_at_lte),
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning("Chat %s not found at source, marking deleted", chat.id)
                cache.mark_chat_deleted(chat.id, "Chat not found (404)")
                if not config.dry_run:
                    cache.commit()
                outcome.chat_failed = True
                break
            raise
        except httpx.RequestError:
            outcome.chat_failed = True
            outcome.user_failed = True
            cache.mark_chat_failed(chat.id, "Network error fetching chat messages")
            if not config.dry_run:
                cache.commit()
            logger.error(
                "Failed to fetch messages for user=%s chat=%s; "
                "stopping user to allow resume",
                user_id,
                chat.id,
            )
            break

        pairs = build_message_pairs(chat_response.chat_messages, chat)
        is_backfill = (
            prior_state is not None
            and prior_state.coverage_from is not None
            and interval.created_at_lte == prior_state.coverage_from
        )
        if _send_chat_pairs(
            chat=chat,
            pairs=pairs,
            nebuly=nebuly,
            cache=cache,
            config=config,
            user_id=user_id,
            outcome=outcome,
            prior_coverage_from=(prior_state.coverage_from if prior_state else None),
            prior_coverage_from_msg_id=(
                prior_state.coverage_from_msg_id if prior_state else None
            ),
            prior_coverage_until=(prior_state.coverage_until if prior_state else None),
            prior_coverage_until_msg_id=(
                prior_state.coverage_until_msg_id if prior_state else None
            ),
            is_backfill=is_backfill,
        ):
            break

    return outcome


def _finalize_successful_chat(
    *,
    chat: ChatSummary,
    cache: SyncCache,
    config: Config,
    exported_until: datetime | None,
    exported_until_msg_id: str | None,
    requested_from: datetime | None,
    run_until: datetime,
    highest_completed: datetime | None,
    user_coverage_from: datetime | None,
    user_coverage_until: datetime | None,
) -> tuple[datetime | None, datetime | None, datetime | None]:
    # pair_to_payload returns None only when the user message has no text; such
    # pairs are permanently non-exportable, so completing the chat is intentional.
    prior = cache.get_chat_state(chat.id)
    if exported_until is not None:
        coverage_until = exported_until
        coverage_until_msg_id = exported_until_msg_id
    elif prior and prior.coverage_until is not None:
        coverage_until = prior.coverage_until
        coverage_until_msg_id = prior.coverage_until_msg_id
    else:
        coverage_until = run_until
        coverage_until_msg_id = None
    new_from, new_until = _merge_coverage(
        prior.coverage_from if prior else None,
        None,
        requested_from,
        coverage_until,
    )
    cache.mark_chat_completed(
        chat,
        new_coverage_from=new_from,
        new_coverage_until=new_until,
        new_coverage_until_msg_id=coverage_until_msg_id,
    )
    if not config.dry_run:
        cache.commit()

    if highest_completed is None or chat.updated_at > highest_completed:
        highest_completed = chat.updated_at
    user_coverage_from = (
        new_from
        if user_coverage_from is None
        else min(user_coverage_from, new_from)
        if new_from is not None
        else user_coverage_from
    )
    user_coverage_until = (
        new_until
        if user_coverage_until is None
        else max(user_coverage_until, new_until)
    )
    return highest_completed, user_coverage_from, user_coverage_until


def _sync_user(
    *,
    user_id: str,
    config: Config,
    compliance: ComplianceClient,
    nebuly: NebulyClient,
    cache: SyncCache,
    run_until: datetime,
) -> Counts:
    counts = Counts()
    requested_from = config.from_date
    user_failed = False
    highest_completed: datetime | None = None
    user_coverage_from: datetime | None = None
    user_coverage_until: datetime | None = None

    chats = _collect_chats_for_user(
        user_id=user_id,
        compliance=compliance,
        cache=cache,
        requested_from=requested_from,
        run_until=run_until,
        dry_run=config.dry_run,
    )

    for chat in chats:
        if user_failed:
            break

        plan = cache.plan_chat_work(chat, requested_from, run_until)

        if plan.skip:
            cache.mark_chat_skipped_extend(chat, run_until)
            if not config.dry_run:
                cache.commit()
            counts.chats_skipped += 1
            highest_completed = _highest_from_skipped_chat(
                cache, chat.id, highest_completed
            )
            continue

        counts.chats_processed += 1
        prior_state = cache.get_chat_state(chat.id)
        outcome = _export_chat_intervals(
            chat=chat,
            plan=plan,
            compliance=compliance,
            nebuly=nebuly,
            cache=cache,
            config=config,
            user_id=user_id,
            prior_state=prior_state,
        )
        counts.fetched += outcome.fetched
        counts.sent += outcome.sent
        counts.skipped += outcome.skipped
        counts.failed += outcome.failed

        if outcome.user_failed:
            user_failed = True
            break

        if outcome.chat_failed:
            continue

        highest_completed, user_coverage_from, user_coverage_until = (
            _finalize_successful_chat(
                chat=chat,
                cache=cache,
                config=config,
                exported_until=outcome.exported_until,
                exported_until_msg_id=outcome.exported_until_msg_id,
                requested_from=requested_from,
                run_until=run_until,
                highest_completed=highest_completed,
                user_coverage_from=user_coverage_from,
                user_coverage_until=user_coverage_until,
            )
        )

    if not user_failed:
        cache.upsert_user_state(
            user_id,
            highest_completed_chat_updated_at=highest_completed,
            coverage_from=user_coverage_from,
            coverage_until=user_coverage_until,
            last_successful_run_at=datetime.now(timezone.utc),
        )
        if not config.dry_run:
            cache.commit()

    if (
        counts.fetched > 0
        or counts.sent > 0
        or counts.skipped > 0
        or counts.failed > 0
        or counts.chats_processed > 0
        or counts.chats_skipped > 0
    ):
        logger.info(
            "User %s: chats processed=%d skipped=%d | "
            "interactions fetched=%d sent=%d skipped=%d failed=%d",
            user_id,
            counts.chats_processed,
            counts.chats_skipped,
            counts.fetched,
            counts.sent,
            counts.skipped,
            counts.failed,
        )
    return counts
