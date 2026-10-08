from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

import httpx
from httpx import HTTPStatusError

from .cache import ConversationState, SyncCache
from .compliance_client import ComplianceClient, SourceUnavailableError
from .config import Config, timestamp_str_to_datetime
from .nebuly_client import NebulyClient, PermanentRejectionError
from .sources import (
    ChatSource,
    ConversationRef,
    FetchRequest,
    LocalSessionSource,
    RemoteSessionSource,
    listed_user_missing,
    remote_listing_status,
)

if TYPE_CHECKING:
    from .sources import ConversationResult

logger = logging.getLogger(__name__)

_LISTING_OVERLAP = timedelta(minutes=10)


class SourceAdapter(Protocol):
    name: str

    def list_changed(
        self,
        listing_from: datetime | None,
        to_date: datetime | None,
    ) -> list[ConversationRef]: ...

    def fetch(self, request: FetchRequest) -> ConversationResult: ...


@dataclass
class SourceCounts:
    listed: int = 0
    processed: int = 0
    skipped_no_user: int = 0
    skipped_deleted: int = 0
    sent: int = 0
    rejected: int = 0
    skipped_interactions: int = 0
    fetch_failed: int = 0


@dataclass
class SyncSummary:
    sources: dict[str, SourceCounts] = field(default_factory=dict)


def _configure_logging(*, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.DEBUG if verbose else logging.WARNING)


def _adapters_for(
    compliance: ComplianceClient,
    organization_uuid: str,
    source_names: tuple[str, ...],
) -> list[SourceAdapter]:
    registry: dict[str, SourceAdapter] = {
        "chats": ChatSource(compliance, organization_uuid),
        "local_sessions": LocalSessionSource(compliance, organization_uuid),
        "remote_sessions": RemoteSessionSource(compliance, organization_uuid),
    }
    return [registry[name] for name in source_names]


def _listing_from(
    cache: SyncCache,
    source: str,
    from_date: datetime | None,
) -> datetime | None:
    if from_date is not None:
        return from_date
    watermark = cache.get_watermark(source)
    if watermark is None:
        return None
    return watermark - _LISTING_OVERLAP


def _ref_from_state(state: ConversationState) -> ConversationRef:
    return ConversationRef(
        source=state.source,
        conversation_id=state.conversation_id,
        updated_at=state.updated_at_seen,
        metadata_json=state.metadata_json,
    )


def _interaction_time_end(payload: dict[str, Any]) -> datetime:
    raw = payload.get("interaction", {}).get("time_end")
    if isinstance(raw, str):
        return timestamp_str_to_datetime(raw)
    raise ValueError("interaction payload missing time_end")


def _is_transient_http(exc: HTTPStatusError) -> bool:
    status = exc.response.status_code
    return status in {408, 429} or status >= 500


def _collect_todo(
    cache: SyncCache,
    adapter: SourceAdapter,
    refs: list[ConversationRef],
    requested_from: datetime | None,
) -> list[ConversationRef]:
    listed_ids = {ref.conversation_id for ref in refs}
    todo = [
        ref
        for ref in refs
        if cache.should_process(
            adapter.name,
            ref.conversation_id,
            ref.updated_at,
            requested_from,
        )
    ]
    todo.extend(
        _ref_from_state(state)
        for state in cache.iter_unfinished(adapter.name)
        if state.conversation_id not in listed_ids
    )
    return todo


def _existing_coverage(
    cache: SyncCache,
    source: str,
    conversation_id: str,
) -> datetime | None:
    state = cache.get_conversation(source, conversation_id)
    if state is None:
        return None
    return state.coverage_from


def _process_conversation(  # noqa: C901, PLR0912, PLR0915
    *,
    adapter: SourceAdapter,
    ref: ConversationRef,
    cache: SyncCache,
    nebuly: NebulyClient,
    config: Config,
    run_until: datetime,
    now: datetime,
    counts: SourceCounts,
) -> bool:
    """Returns True when a transient failure should stop the source run."""
    if ref.deleted:
        cache.mark_gone(
            adapter.name,
            ref.conversation_id,
            updated_at_seen=ref.updated_at,
            reason="deleted",
        )
        counts.skipped_deleted += 1
        if not config.dry_run:
            cache.commit()
        return False

    if listed_user_missing(ref):
        cache.upsert_conversation(
            adapter.name,
            ref.conversation_id,
            updated_at_seen=ref.updated_at,
            coverage_from=_existing_coverage(cache, adapter.name, ref.conversation_id),
            status="completed",
            metadata_json=ref.metadata_json,
        )
        counts.skipped_no_user += 1
        if not config.dry_run:
            cache.commit()
        return False

    if adapter.name == "remote_sessions" and remote_listing_status(ref) == "pending":
        cache.upsert_conversation(
            adapter.name,
            ref.conversation_id,
            updated_at_seen=ref.updated_at,
            coverage_from=None,
            status="pending",
            metadata_json=ref.metadata_json,
        )
        if not config.dry_run:
            cache.commit()
        counts.processed += 1
        return False

    request = FetchRequest(
        ref=ref,
        from_date=config.from_date,
        now=now,
        idle_minutes=config.session_idle_minutes,
        anonymize=config.anonymize,
    )
    try:
        result = adapter.fetch(request)
    except HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 404:
            cache.mark_gone(
                adapter.name,
                ref.conversation_id,
                updated_at_seen=ref.updated_at,
                reason="not found (404)",
            )
            if not config.dry_run:
                cache.commit()
            counts.processed += 1
            return False
        if not _is_transient_http(exc):
            cache.upsert_conversation(
                adapter.name,
                ref.conversation_id,
                updated_at_seen=ref.updated_at,
                coverage_from=config.from_date,
                status="failed",
                last_error=f"fetch HTTP {status}",
                metadata_json=ref.metadata_json,
                claim_coverage=False,
            )
            counts.fetch_failed += 1
            if not config.dry_run:
                cache.commit()
            return False
        cache.upsert_conversation(
            adapter.name,
            ref.conversation_id,
            updated_at_seen=ref.updated_at,
            coverage_from=config.from_date,
            status="failed",
            last_error=f"fetch HTTP {status}",
            metadata_json=ref.metadata_json,
            claim_coverage=False,
        )
        if not config.dry_run:
            cache.commit()
        return True
    except httpx.TransportError as exc:
        cache.upsert_conversation(
            adapter.name,
            ref.conversation_id,
            updated_at_seen=ref.updated_at,
            coverage_from=config.from_date,
            status="failed",
            last_error=str(exc),
            metadata_json=ref.metadata_json,
            claim_coverage=False,
        )
        if not config.dry_run:
            cache.commit()
        return True

    if result.gone:
        cache.mark_gone(
            adapter.name,
            ref.conversation_id,
            updated_at_seen=result.updated_at,
            reason=result.gone_reason or "gone",
        )
        if not config.dry_run:
            cache.commit()
        counts.processed += 1
        return False

    if result.user_id is None:
        cache.upsert_conversation(
            adapter.name,
            ref.conversation_id,
            updated_at_seen=result.updated_at,
            coverage_from=config.from_date,
            status="completed",
            metadata_json=result.metadata_json or ref.metadata_json,
        )
        counts.skipped_no_user += 1
        if not config.dry_run:
            cache.commit()
        return False

    counts.processed += 1
    has_open = False
    held_by_to_date = False
    coverage_from = config.from_date

    for item in result.interactions:
        if not item.closed:
            has_open = True
            continue
        if item.payload is None:
            counts.skipped_interactions += 1
            continue
        if config.from_date is not None and item.time_start < config.from_date:
            counts.skipped_interactions += 1
            continue
        time_end = _interaction_time_end(item.payload)
        if time_end > run_until:
            held_by_to_date = True
            continue
        if cache.is_emitted(adapter.name, ref.conversation_id, item.key):
            counts.skipped_interactions += 1
            continue

        try:
            nebuly.send_interaction(item.payload)
        except PermanentRejectionError:
            cache.record_emitted(
                adapter.name, ref.conversation_id, item.key, "rejected"
            )
            counts.rejected += 1
            if not config.dry_run:
                cache.commit()
            continue
        except (HTTPStatusError, httpx.TransportError):
            cache.upsert_conversation(
                adapter.name,
                ref.conversation_id,
                updated_at_seen=result.updated_at,
                coverage_from=coverage_from,
                status="failed",
                last_error="send error",
                metadata_json=result.metadata_json or ref.metadata_json,
                claim_coverage=False,
            )
            if not config.dry_run:
                cache.commit()
            logger.exception(
                "Failed to send interaction source=%s conversation=%s key=%s",
                adapter.name,
                ref.conversation_id,
                item.key,
            )
            return True

        cache.record_emitted(adapter.name, ref.conversation_id, item.key, "sent")
        counts.sent += 1
        if coverage_from is None or item.time_start < coverage_from:
            coverage_from = item.time_start
        if not config.dry_run:
            cache.commit()

    pending = has_open or held_by_to_date or result.remote_open
    conversation_status = "pending" if pending else "completed"
    cache.upsert_conversation(
        adapter.name,
        ref.conversation_id,
        updated_at_seen=result.updated_at,
        coverage_from=(
            coverage_from if conversation_status == "completed" else config.from_date
        ),
        status=conversation_status,
        metadata_json=result.metadata_json or ref.metadata_json,
        claim_coverage=conversation_status == "completed",
    )
    if not config.dry_run:
        cache.commit()
    return False


def _run_source(
    *,
    adapter: SourceAdapter,
    cache: SyncCache,
    nebuly: NebulyClient,
    config: Config,
    run_until: datetime,
    run_started: datetime,
) -> tuple[SourceCounts, bool]:
    counts = SourceCounts()
    listing_from = _listing_from(cache, adapter.name, config.from_date)
    try:
        refs = adapter.list_changed(listing_from, run_until)
    except SourceUnavailableError as exc:
        logger.info("Skipping source %s: %s", adapter.name, exc)
        return counts, False

    counts.listed = len(refs)
    todo = _collect_todo(cache, adapter, refs, config.from_date)
    now = datetime.now(UTC)
    stopped = False
    for ref in todo:
        if _process_conversation(
            adapter=adapter,
            ref=ref,
            cache=cache,
            nebuly=nebuly,
            config=config,
            run_until=run_until,
            now=now,
            counts=counts,
        ):
            stopped = True
            break

    if not stopped and not config.dry_run:
        cache.set_watermark(adapter.name, run_started)
        cache.commit()
    return counts, stopped


def run_sync(config: Config) -> SyncSummary:
    _configure_logging(verbose=config.verbose)

    cache = SyncCache(
        config.cache_dir / "sync_state.db",
        config.organization_uuid,
        dry_run=config.dry_run,
    )
    run_until = config.run_until()
    run_started = datetime.now(UTC)
    summary = SyncSummary()

    try:
        timeout = 300.0
        with httpx.Client(
            base_url=config.compliance_base_url, timeout=timeout
        ) as compliance_http:
            compliance = ComplianceClient(
                compliance_http,
                config.compliance_api_key,
                max_requests_per_minute=config.compliance_max_requests_per_minute,
            )

            with httpx.Client(timeout=timeout) as nebuly_http:
                nebuly = NebulyClient(
                    nebuly_http,
                    config.nebuly_api_key,
                    config.nebuly_endpoint,
                    dry_run=config.dry_run,
                )

                for adapter in _adapters_for(
                    compliance,
                    config.organization_uuid,
                    config.sources,
                ):
                    counts, stopped = _run_source(
                        adapter=adapter,
                        cache=cache,
                        nebuly=nebuly,
                        config=config,
                        run_until=run_until,
                        run_started=run_started,
                    )
                    summary.sources[adapter.name] = counts
                    logger.info(
                        "Source %s: listed=%d processed=%d sent=%d rejected=%d "
                        "skipped_interactions=%d no_user=%d deleted=%d fetch_failed=%d",
                        adapter.name,
                        counts.listed,
                        counts.processed,
                        counts.sent,
                        counts.rejected,
                        counts.skipped_interactions,
                        counts.skipped_no_user,
                        counts.skipped_deleted,
                        counts.fetch_failed,
                    )
                    if stopped:
                        logger.error(
                            "Stopping after transient failure on source %s",
                            adapter.name,
                        )
                        break
    finally:
        cache.close()

    return summary
