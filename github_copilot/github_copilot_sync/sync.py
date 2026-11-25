from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

import httpx
from httpx import HTTPStatusError

from .conversations import ConversationBuilder
from .converter import SkipReason, turn_to_payload
from .csv_writer import CsvInteractionWriter
from .models import ClosedTurn, UsageRecord
from .nebuly_client import NebulyClient, PermanentRejectionError
from .pairing import PairingEngine
from .sources import build_sources
from .store import Store
from .users import UserResolver

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class InteractionSink(Protocol):
    def send_interaction(self, payload: dict[str, Any]) -> None: ...


class SinkError(Exception):
    """The sink could not accept a turn; the turn stays stored for the next run."""


@dataclass
class SourceCounts:
    records_ingested: int = 0
    records_deduped: int = 0
    sent: int = 0
    rejected: int = 0
    skipped: int = 0
    non_usage_events: int = 0
    unpaired_requests: int = 0
    unpaired_responses: int = 0
    unreadable_files: int = 0
    unreadable_lines: int = 0


@dataclass
class SyncSummary:
    sources: dict[str, SourceCounts] = field(default_factory=dict)
    pipeline_skipped_non_chat: int = 0
    pipeline_skipped_background: int = 0
    pipeline_unparseable_bodies: int = 0
    failed: bool = False


def _configure_logging(*, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.DEBUG if verbose else logging.WARNING)


def _github_client(config: Config) -> httpx.Client:
    return httpx.Client(
        base_url=config.github_api_url,
        timeout=120.0,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {config.github_token}",
            "X-GitHub-Api-Version": config.github_api_version,
        },
    )


def _emit_closed_turns(
    closed: list[ClosedTurn],
    *,
    store: Store,
    sink: InteractionSink,
    resolver: UserResolver,
    config: Config,
    counts: SourceCounts,
) -> None:
    for turn in closed:
        if store.is_turn_emitted(turn.turn_key):
            store.release_closed_turn(turn.turn_key)
            continue
        user = resolver.resolve(turn.user_id)
        result = turn_to_payload(
            turn,
            end_user=user.end_user,
            email=user.email,
            anonymize=config.anonymize,
            directory=config.entra_directory,
        )
        if isinstance(result, SkipReason):
            store.release_closed_turn(turn.turn_key)
            counts.skipped += 1
            continue
        try:
            sink.send_interaction(result)
        except PermanentRejectionError:
            _settle_turn(store, turn, "rejected")
            counts.rejected += 1
            continue
        except (HTTPStatusError, httpx.TransportError) as exc:
            raise SinkError(f"Could not send turn {turn.turn_key}") from exc
        _settle_turn(store, turn, "sent")
        counts.sent += 1


def _settle_turn(store: Store, turn: ClosedTurn, status: str) -> None:
    with store.transaction():
        store.record_emitted_turn(turn.turn_key, status)
        store.release_closed_turn(turn.turn_key)


def _process_record(
    record: UsageRecord,
    *,
    store: Store,
    pairing: PairingEngine,
    conversations: ConversationBuilder,
    sink: InteractionSink,
    resolver: UserResolver,
    config: Config,
    counts: SourceCounts,
) -> int:
    with store.transaction():
        if not store.insert_usage_record(record):
            counts.records_deduped += 1
            return record.timestamp_ms
        counts.records_ingested += 1
        closed = [
            turn
            for call in pairing.ingest(record)
            for turn in conversations.add_call(call)
        ]
    _emit_closed_turns(
        closed,
        store=store,
        sink=sink,
        resolver=resolver,
        config=config,
        counts=counts,
    )
    return record.timestamp_ms


def _flush_idle(
    watermark_ms: int,
    *,
    pairing: PairingEngine,
    conversations: ConversationBuilder,
    store: Store,
    sink: InteractionSink,
    resolver: UserResolver,
    config: Config,
    counts: SourceCounts,
) -> None:
    pairing.expire_unpaired(watermark_ms)
    counts.unpaired_requests = pairing.counters.unpaired_requests
    counts.unpaired_responses = pairing.counters.unpaired_responses
    closed_idle = conversations.close_idle_turns(watermark_ms)
    _emit_closed_turns(
        closed_idle,
        store=store,
        sink=sink,
        resolver=resolver,
        config=config,
        counts=counts,
    )


def _finalize_open(
    *,
    conversations: ConversationBuilder,
    store: Store,
    sink: InteractionSink,
    resolver: UserResolver,
    config: Config,
    counts: SourceCounts,
) -> None:
    closed = conversations.close_all_open()
    _emit_closed_turns(
        closed,
        store=store,
        sink=sink,
        resolver=resolver,
        config=config,
        counts=counts,
    )


def run_sync(config: Config) -> SyncSummary:
    _configure_logging(verbose=config.verbose)
    summary = SyncSummary()
    store = Store(config.store_path, dry_run=config.dry_run)
    pairing = PairingEngine(config.pair_wait_minutes, store)
    conversations = ConversationBuilder(
        store,
        conversation_gap_hours=config.conversation_gap_hours,
        interaction_idle_minutes=config.interaction_idle_minutes,
    )

    if config.entra_directory is not None:
        logger.info(
            "Entra directory loaded: %d directory users",
            config.entra_directory.user_count,
        )

    stopped = False
    try:
        with (
            _github_client(config) as github_http,
            httpx.Client(timeout=120.0) as nebuly_http,
        ):
            if config.csv_export:
                sink: InteractionSink = CsvInteractionWriter(
                    config.csv_export_path, dry_run=config.dry_run
                )
                logger.info("CSV export enabled: writing to %s", config.csv_export_path)
            else:
                sink = NebulyClient(
                    nebuly_http,
                    config.nebuly_api_key,
                    config.nebuly_endpoint,
                    dry_run=config.dry_run,
                )

            resolver = UserResolver(config, github_http, store)
            sources = build_sources(config)

            for name in config.sources:
                source = sources.get(name)
                if source is None:
                    continue
                counts = SourceCounts()
                cursor = store.get_source_cursor(name)
                new_cursor: str | None = None
                watermark = 0
                try:
                    _emit_closed_turns(
                        conversations.take_closed_unsent(),
                        store=store,
                        sink=sink,
                        resolver=resolver,
                        config=config,
                        counts=counts,
                    )
                    for record, cursor_value in source.iter_records(cursor):
                        watermark = max(
                            watermark,
                            _process_record(
                                record,
                                store=store,
                                pairing=pairing,
                                conversations=conversations,
                                sink=sink,
                                resolver=resolver,
                                config=config,
                                counts=counts,
                            ),
                        )
                        new_cursor = cursor_value
                    if watermark > 0:
                        _flush_idle(
                            watermark,
                            pairing=pairing,
                            conversations=conversations,
                            store=store,
                            sink=sink,
                            resolver=resolver,
                            config=config,
                            counts=counts,
                        )
                    if config.to_date is not None:
                        _finalize_open(
                            conversations=conversations,
                            store=store,
                            sink=sink,
                            resolver=resolver,
                            config=config,
                            counts=counts,
                        )
                    if new_cursor is not None:
                        store.set_source_cursor(name, new_cursor)
                except SinkError:
                    logger.exception(
                        "Nebuly sink failure on source %s; unsent turns are kept "
                        "and retried on the next run",
                        name,
                    )
                    stopped = True
                    break
                except HTTPStatusError as exc:
                    logger.exception("GitHub failure on source %s", name)
                    stopped = True
                    break
                except httpx.TransportError:
                    logger.exception("Transport failure on source %s", name)
                    stopped = True
                    break
                finally:
                    close = getattr(source, "close", None)
                    if callable(close):
                        close()

                counts.non_usage_events = source.stats.non_usage_events
                counts.unreadable_files = source.stats.unreadable_files
                counts.unreadable_lines = source.stats.unreadable_lines
                summary.sources[name] = counts
                logger.info(
                    "Source %s: ingested=%d deduped=%d sent=%d rejected=%d skipped=%d "
                    "non_usage=%d unpaired_requests=%d unpaired_responses=%d",
                    name,
                    counts.records_ingested,
                    counts.records_deduped,
                    counts.sent,
                    counts.rejected,
                    counts.skipped,
                    counts.non_usage_events,
                    counts.unpaired_requests,
                    counts.unpaired_responses,
                )

        summary.pipeline_skipped_non_chat = conversations.counters.skipped_non_chat
        summary.pipeline_skipped_background = conversations.counters.skipped_background
        summary.pipeline_unparseable_bodies = conversations.counters.unparseable_bodies
    finally:
        store.close()

    if stopped:
        logger.error("Sync stopped after a failure; unsent work is retried next run")
        summary.failed = True
    return summary
