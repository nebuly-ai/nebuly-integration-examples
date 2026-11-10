from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
from httpx import HTTPStatusError

from .chat_converter import (
    app_calls_to_traces,
    assemble_interactions,
    interaction_to_payload,
    parse_stored_message,
)
from .codex_converter import codex_record_to_payload
from .compliance_client import (
    EVENT_APP_LOG,
    EVENT_CODEX_LOG,
    EVENT_CONVERSATION_MESSAGE,
    ComplianceClient,
)
from .config import Config, datetime_to_timestamp_str, timestamp_str_to_datetime
from .models import AppLogRecord, CodexLogRecord, ConversationMessageRecord
from .nebuly_client import NebulyClient, PermanentRejectionError
from .store import ComplianceStore

if TYPE_CHECKING:
    from .entra import EntraDirectory

logger = logging.getLogger(__name__)

_DEFAULT_LOOKBACK = timedelta(days=30)
_CURSOR_LAG_WARN = timedelta(days=7)


@dataclass
class SourceCounts:
    files: int = 0
    rows: int = 0
    sent: int = 0
    rejected: int = 0
    skipped_open: int = 0
    skipped_empty: int = 0


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


def _is_transient_http(exc: HTTPStatusError) -> bool:
    status = exc.response.status_code
    return status in {408, 429} or status >= 500


def _initial_after(config: Config) -> str:
    if config.from_date is not None:
        return datetime_to_timestamp_str(config.from_date)
    return datetime_to_timestamp_str(datetime.now(UTC) - _DEFAULT_LOOKBACK)


def _cursor_after(store: ComplianceStore, event_type: str, config: Config) -> str:
    cursor = store.get_cursor(event_type)
    if cursor is not None:
        return datetime_to_timestamp_str(cursor)
    return _initial_after(config)


def _warn_cursor_lag(store: ComplianceStore, event_type: str, now: datetime) -> None:
    cursor = store.get_cursor(event_type)
    if cursor is None:
        return
    lag = now - cursor.astimezone(UTC)
    if lag > _CURSOR_LAG_WARN:
        logger.warning(
            "Ingest cursor for %s lags %s behind now (30-day retention limit)",
            event_type,
            lag,
        )


def _message_author_type(record: ConversationMessageRecord) -> str:
    if record.message.author is None or not record.message.author.type:
        return "unknown"
    return record.message.author.type.casefold()


def _ingest_jsonl_file(
    store: ComplianceStore,
    *,
    event_type: str,
    file_id: str,
    rows: list[dict[str, Any]],
) -> int:
    ingested = 0
    for row in rows:
        if event_type == EVENT_CONVERSATION_MESSAGE:
            record = ConversationMessageRecord.model_validate(row)
            if store.insert_chat_message(
                message_id=record.message.id,
                conversation_id=record.conversation.id,
                author_type=_message_author_type(record),
                created_at=record.message.created_at,
                previous_message_id=record.previous_message_id,
                record=row,
            ):
                ingested += 1
        elif event_type == EVENT_APP_LOG:
            app_record = AppLogRecord.model_validate(row)
            if (
                app_record.conversation_id
                and app_record.app_id
                and app_record.timestamp
                and app_record.event_id
                and store.insert_app_call(
                    event_id=app_record.event_id,
                    conversation_id=app_record.conversation_id,
                    app_id=app_record.app_id,
                    record_kind=app_record.record_kind,
                    timestamp=app_record.timestamp,
                    record=row,
                )
            ):
                ingested += 1
    store.mark_file_processed(file_id, event_type)
    return ingested


def _ingest_event_type(
    client: ComplianceClient,
    store: ComplianceStore,
    config: Config,
    event_type: str,
    counts: SourceCounts,
) -> bool:
    """Returns False when a transient error should stop the run."""
    after = _cursor_after(store, event_type, config)
    _warn_cursor_lag(store, event_type, datetime.now(UTC))
    try:
        for page in client.iter_log_files(event_type, after=after, limit=100):
            page_complete = True
            for log_file in page.data:
                if store.is_file_processed(log_file.id):
                    continue
                rows = list(client.download_log_file(log_file.id))
                counts.files += 1
                counts.rows += _ingest_jsonl_file(
                    store,
                    event_type=event_type,
                    file_id=log_file.id,
                    rows=rows,
                )
            if page_complete and page.last_end_time:
                store.set_cursor(
                    event_type, timestamp_str_to_datetime(page.last_end_time)
                )
            if not config.dry_run:
                store.commit()
    except HTTPStatusError as exc:
        if _is_transient_http(exc):
            logger.exception("Transient compliance API failure on %s", event_type)
        else:
            logger.exception("Compliance API failure on %s", event_type)
        return False
    except httpx.TransportError:
        logger.exception("Transport failure while ingesting %s", event_type)
        return False
    return True


def _emit_codex_row(
    store: ComplianceStore,
    nebuly: NebulyClient,
    config: Config,
    counts: SourceCounts,
    directory: EntraDirectory | None,
    row: dict[str, Any],
) -> None:
    record = CodexLogRecord.model_validate(row)
    counts.rows += 1
    if store.is_emitted("codex", record.event_id):
        return
    payload = codex_record_to_payload(
        record,
        anonymize=config.anonymize,
        directory=directory,
    )
    if payload is None:
        return
    try:
        nebuly.send_interaction(payload)
        store.record_emitted("codex", record.event_id, "sent")
        counts.sent += 1
    except PermanentRejectionError:
        store.record_emitted("codex", record.event_id, "rejected")
        counts.rejected += 1


def _emit_codex(
    client: ComplianceClient,
    store: ComplianceStore,
    nebuly: NebulyClient,
    config: Config,
    counts: SourceCounts,
    directory: EntraDirectory | None,
) -> bool:
    after = _cursor_after(store, EVENT_CODEX_LOG, config)
    try:
        for page in client.iter_log_files(EVENT_CODEX_LOG, after=after, limit=100):
            for log_file in page.data:
                if store.is_file_processed(log_file.id):
                    continue
                rows = list(client.download_log_file(log_file.id))
                counts.files += 1
                for row in rows:
                    _emit_codex_row(store, nebuly, config, counts, directory, row)
                store.mark_file_processed(log_file.id, EVENT_CODEX_LOG)
            if page.last_end_time:
                store.set_cursor(
                    EVENT_CODEX_LOG, timestamp_str_to_datetime(page.last_end_time)
                )
            if not config.dry_run:
                store.commit()
    except HTTPStatusError:
        return False
    except httpx.TransportError:
        return False
    return True


def _emit_chats(
    store: ComplianceStore,
    nebuly: NebulyClient,
    config: Config,
    counts: SourceCounts,
    directory: EntraDirectory | None,
) -> None:
    now = datetime.now(UTC)
    for user_stored in store.unemitted_user_messages():
        conversation_rows = store.conversation_messages(user_stored.conversation_id)
        records = [parse_stored_message(row) for row in conversation_rows]
        assembled = assemble_interactions(
            records,
            now=now,
            idle_minutes=config.interaction_idle_minutes,
        )
        match = next(
            (
                item
                for item in assembled
                if item.interaction_key == user_stored.message_id
            ),
            None,
        )
        if match is None or not match.closed:
            counts.skipped_open += 1
            continue
        if not match.assistants:
            counts.skipped_open += 1
            continue
        window_end = match.assistants[-1].message.created_at
        app_calls = store.app_calls_in_window(
            match.user.conversation.id,
            start=match.user.message.created_at,
            end=window_end,
        )
        payload = interaction_to_payload(
            match,
            retrieval_traces=app_calls_to_traces(app_calls),
            anonymize=config.anonymize,
            directory=directory,
        )
        if payload is None:
            counts.skipped_empty += 1
            continue
        try:
            nebuly.send_interaction(payload)
            store.record_emitted("chats", match.interaction_key, "sent")
            counts.sent += 1
        except PermanentRejectionError:
            store.record_emitted("chats", match.interaction_key, "rejected")
            counts.rejected += 1
        if not config.dry_run:
            store.commit()


def run_sync(config: Config) -> SyncSummary:
    _configure_logging(verbose=config.verbose)
    summary = SyncSummary()
    store = ComplianceStore(
        config.cache_dir / "compliance_store.db",
        config.compliance_principal_id,
        dry_run=config.dry_run,
    )
    if config.entra_directory is not None:
        logger.info(
            "Entra directory loaded: %d directory users",
            config.entra_directory.user_count,
        )

    stopped = False
    try:
        timeout = 300.0
        with httpx.Client(
            base_url=config.compliance_base_url, timeout=timeout
        ) as compliance_http:
            client = ComplianceClient(
                compliance_http,
                config.compliance_api_key,
                config.compliance_principal_id,
                max_requests_per_minute=config.compliance_max_requests_per_minute,
            )
            with httpx.Client(timeout=timeout) as nebuly_http:
                nebuly = NebulyClient(
                    nebuly_http,
                    config.nebuly_api_key,
                    config.nebuly_endpoint,
                    dry_run=config.dry_run,
                )

                if "chats" in config.sources:
                    chat_counts = SourceCounts()
                    if _ingest_event_type(
                        client,
                        store,
                        config,
                        EVENT_CONVERSATION_MESSAGE,
                        chat_counts,
                    ) and _ingest_event_type(
                        client, store, config, EVENT_APP_LOG, chat_counts
                    ):
                        _emit_chats(
                            store,
                            nebuly,
                            config,
                            chat_counts,
                            config.entra_directory,
                        )
                    else:
                        stopped = True
                    summary.sources["chats"] = chat_counts
                    logger.info(
                        "Source chats: files=%d rows=%d sent=%d rejected=%d "
                        "skipped_open=%d skipped_empty=%d",
                        chat_counts.files,
                        chat_counts.rows,
                        chat_counts.sent,
                        chat_counts.rejected,
                        chat_counts.skipped_open,
                        chat_counts.skipped_empty,
                    )

                if not stopped and "codex" in config.sources:
                    codex_counts = SourceCounts()
                    if not _emit_codex(
                        client,
                        store,
                        nebuly,
                        config,
                        codex_counts,
                        config.entra_directory,
                    ):
                        stopped = True
                    summary.sources["codex"] = codex_counts
                    logger.info(
                        "Source codex: files=%d rows=%d sent=%d rejected=%d",
                        codex_counts.files,
                        codex_counts.rows,
                        codex_counts.sent,
                        codex_counts.rejected,
                    )

                if not config.dry_run:
                    store.prune()
                    store.commit()
    finally:
        store.close()

    if stopped:
        logger.error("Stopping after transient failure")
    return summary
