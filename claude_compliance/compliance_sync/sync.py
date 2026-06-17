from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx
from httpx import HTTPStatusError

from .checkpoint import Checkpoint
from .compliance_client import ComplianceClient
from .config import Config, datetime_to_timestamp_str
from .converter import (
    build_message_pairs,
    dedup_key,
    pair_cursor_ts,
    pair_to_payload,
)
from .nebuly_client import NebulyClient

logger = logging.getLogger(__name__)


@dataclass
class Counts:
    fetched: int = 0
    sent: int = 0
    skipped: int = 0
    failed: int = 0


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
    # httpx logs each HTTP request at INFO; keep those at DEBUG unless verbose.
    logging.getLogger("httpx").setLevel(logging.DEBUG if verbose else logging.WARNING)


def run_sync(config: Config) -> SyncSummary:
    _configure_logging(verbose=config.verbose)

    checkpoint_path = config.cache_dir / "checkpoint.json"
    checkpoint = Checkpoint(checkpoint_path, config.organization_uuid)

    summary = SyncSummary()

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
                    checkpoint=checkpoint,
                )
                summary.users_processed += 1
                summary.totals.fetched += user_counts.fetched
                summary.totals.sent += user_counts.sent
                summary.totals.skipped += user_counts.skipped
                summary.totals.failed += user_counts.failed

                if not config.dry_run:
                    checkpoint.save()

    logger.info(
        "Sync complete: users=%d fetched=%d sent=%d skipped=%d failed=%d",
        summary.users_processed,
        summary.totals.fetched,
        summary.totals.sent,
        summary.totals.skipped,
        summary.totals.failed,
    )
    return summary


def _sync_user(
    *,
    user_id: str,
    config: Config,
    compliance: ComplianceClient,
    nebuly: NebulyClient,
    checkpoint: Checkpoint,
) -> Counts:
    counts = Counts()
    updated_at_gte_dt = checkpoint.updated_at_gte(user_id, config.from_date)
    updated_at_gte = (
        datetime_to_timestamp_str(updated_at_gte_dt) if updated_at_gte_dt else None
    )
    updated_at_lte = (
        datetime_to_timestamp_str(config.to_date) if config.to_date else None
    )
    created_at_gte = updated_at_gte
    created_at_lte = updated_at_lte

    after_id: str | None = None
    user_failed = False

    while not user_failed:
        chats_page = compliance.list_chats(
            [user_id],
            updated_at_gte=updated_at_gte,
            updated_at_lte=updated_at_lte,
            after_id=after_id,
        )
        chats = sorted(chats_page.data, key=lambda c: (c.updated_at, c.id))

        for chat in chats:
            if user_failed:
                break
            try:
                chat_response = compliance.list_chat_messages(
                    chat.id,
                    created_at_gte=created_at_gte,
                    created_at_lte=created_at_lte,
                )
            except HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning("Chat %s not found, skipping", chat.id)
                    continue
                raise

            pairs = build_message_pairs(chat_response.chat_messages, chat)
            for pair in pairs:
                counts.fetched += 1
                key = dedup_key(pair)
                ts = pair_cursor_ts(pair)
                if checkpoint.is_sent(user_id, ts, key):
                    counts.skipped += 1
                    continue

                payload = pair_to_payload(pair, anonymize=config.anonymize)
                if payload is None:
                    counts.skipped += 1
                    continue

                try:
                    nebuly.send_interaction(payload)
                except HTTPStatusError:
                    counts.failed += 1
                    user_failed = True
                    logger.error(
                        "Failed to send interaction for user=%s chat=%s key=%s; "
                        "stopping user to allow resume",
                        user_id,
                        chat.id,
                        key,
                    )
                    break

                checkpoint.record_sent(user_id, ts, key)
                counts.sent += 1

        if not chats_page.has_more:
            break
        after_id = chats_page.last_id
        if after_id is None:
            break

    checkpoint_watermark = checkpoint.updated_at_gte(user_id, None)
    if counts.fetched == 0 and checkpoint_watermark is not None:
        logger.debug(
            "User %s: no new messages since checkpoint (%s)",
            user_id,
            datetime_to_timestamp_str(checkpoint_watermark),
        )
    elif (
        counts.fetched > 0 or counts.sent > 0 or counts.skipped > 0 or counts.failed > 0
    ):
        logger.info(
            "User %s: fetched=%d sent=%d skipped=%d failed=%d",
            user_id,
            counts.fetched,
            counts.sent,
            counts.skipped,
            counts.failed,
        )
    return counts
