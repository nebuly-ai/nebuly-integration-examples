from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
from httpx import HTTPStatusError
from pydantic import ValidationError

from .chat_converter import (
    app_calls_to_traces,
    assemble_interactions,
    interaction_to_payload,
    parse_stored_message,
)
from .codex_converter import (
    assemble_codex_turns,
    codex_row_store_event_type,
    codex_turn_join_id,
    codex_turn_to_payload,
    parse_stored_codex,
)
from .compliance_client import (
    EVENT_APP_LOG,
    EVENT_CODEX_LOG,
    EVENT_CODEX_TURN,
    EVENT_CONVERSATION_MESSAGE,
    ComplianceClient,
)
from .config import Config, datetime_to_timestamp_str, timestamp_str_to_datetime
from .models import (
    AppLogRecord,
    CodexLogRecord,
    CodexTurnRecord,
    ConversationMessageRecord,
)
from .nebuly_client import NebulyClient, PermanentRejectionError
from .store import ComplianceStore, StoredCodexEvent

if TYPE_CHECKING:
    from .codex_converter import CodexTurn
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
    skipped_waiting_cost: int = 0
    sent_without_cost: int = 0


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


def _is_codex_turn_scope_error(exc: HTTPStatusError) -> bool:
    return exc.response.status_code in {403, 404}


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


def _codex_session_id(record: CodexLogRecord) -> str | None:
    if record.event_details is None:
        return None
    return record.event_details.session_id


def _ingest_chat_row(store: ComplianceStore, row: dict[str, Any]) -> bool:
    record = ConversationMessageRecord.model_validate(row)
    message_time = record.message.created_at or record.timestamp
    event_time = record.timestamp or record.message.created_at
    if message_time is None or event_time is None:
        return False
    return store.insert_chat_message(
        message_id=record.message.id,
        conversation_id=record.conversation.id,
        author_type=_message_author_type(record),
        created_at=message_time,
        event_ts=event_time,
        previous_message_id=record.previous_message_id,
        record=row,
    )


def _ingest_app_log_row(store: ComplianceStore, row: dict[str, Any]) -> bool:
    app_record = AppLogRecord.model_validate(row)
    if not (
        app_record.conversation_id
        and app_record.app_id
        and app_record.timestamp
        and app_record.event_id
    ):
        return False
    return store.insert_app_call(
        event_id=app_record.event_id,
        conversation_id=app_record.conversation_id,
        app_id=app_record.app_id,
        record_kind=app_record.record_kind,
        timestamp=app_record.timestamp,
        record=row,
    )


def _ingest_codex_log_row(store: ComplianceStore, row: dict[str, Any]) -> bool:
    codex_record = CodexLogRecord.model_validate(row)
    stored_type = codex_row_store_event_type(codex_record)
    session_id = _codex_session_id(codex_record)
    if not (
        stored_type and session_id and codex_record.timestamp and codex_record.event_id
    ):
        return False
    return store.insert_codex_event(
        event_id=codex_record.event_id,
        session_id=session_id,
        event_type=stored_type,
        timestamp=codex_record.timestamp,
        record=row,
    )


def _ingest_codex_turn_row(store: ComplianceStore, row: dict[str, Any]) -> bool:
    turn_record = CodexTurnRecord.model_validate(row)
    turn_id = turn_record.payload.turn_id
    ts = turn_record.timestamp or datetime.now(UTC)
    return store.insert_codex_turn_record(
        turn_id=turn_id,
        timestamp=ts,
        record=row,
    )


def _ingest_jsonl_row(
    store: ComplianceStore,
    event_type: str,
    row: dict[str, Any],
) -> bool:
    if event_type == EVENT_CONVERSATION_MESSAGE:
        return _ingest_chat_row(store, row)
    if event_type == EVENT_APP_LOG:
        return _ingest_app_log_row(store, row)
    if event_type == EVENT_CODEX_LOG:
        return _ingest_codex_log_row(store, row)
    if event_type == EVENT_CODEX_TURN:
        return _ingest_codex_turn_row(store, row)
    return False


def _ingest_jsonl_file(
    store: ComplianceStore,
    *,
    event_type: str,
    file_id: str,
    rows: list[dict[str, Any]],
) -> int:
    ingested = 0
    for row in rows:
        try:
            if _ingest_jsonl_row(store, event_type, row):
                ingested += 1
        except ValidationError:
            logger.warning("Skipping invalid row in file %s", file_id)
            continue
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
    before = (
        datetime_to_timestamp_str(config.to_date)
        if config.to_date is not None
        else None
    )
    _warn_cursor_lag(store, event_type, datetime.now(UTC))
    try:
        for page in client.iter_log_files(
            event_type, after=after, before=before, limit=100
        ):
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


def _ingest_codex_turn_with_scope_handling(
    client: ComplianceClient,
    store: ComplianceStore,
    config: Config,
    counts: SourceCounts,
) -> tuple[bool, bool]:
    after = _cursor_after(store, EVENT_CODEX_TURN, config)
    before = (
        datetime_to_timestamp_str(config.to_date)
        if config.to_date is not None
        else None
    )
    _warn_cursor_lag(store, EVENT_CODEX_TURN, datetime.now(UTC))
    try:
        for page in client.iter_log_files(
            EVENT_CODEX_TURN, after=after, before=before, limit=100
        ):
            for log_file in page.data:
                if store.is_file_processed(log_file.id):
                    continue
                rows = list(client.download_log_file(log_file.id))
                counts.files += 1
                counts.rows += _ingest_jsonl_file(
                    store,
                    event_type=EVENT_CODEX_TURN,
                    file_id=log_file.id,
                    rows=rows,
                )
            if page.last_end_time:
                store.set_cursor(
                    EVENT_CODEX_TURN, timestamp_str_to_datetime(page.last_end_time)
                )
            if not config.dry_run:
                store.commit()
    except HTTPStatusError as exc:
        if _is_codex_turn_scope_error(exc):
            logger.warning(
                "CODEX_TURN listing returned %s; emitting Codex without cost wait",
                exc.response.status_code,
            )
            return True, True
        if _is_transient_http(exc):
            logger.exception("Transient compliance API failure on %s", EVENT_CODEX_TURN)
        else:
            logger.exception("Compliance API failure on %s", EVENT_CODEX_TURN)
        return False, False
    except httpx.TransportError:
        logger.exception("Transport failure while ingesting %s", EVENT_CODEX_TURN)
        return False, False
    return True, False


def _load_turn_record(
    store: ComplianceStore, turn_id: str | None
) -> CodexTurnRecord | None:
    if turn_id is None:
        return None
    raw = store.codex_turn_record(turn_id)
    if raw is None:
        return None
    return CodexTurnRecord.model_validate(json.loads(raw))


def _turn_last_event_at(turn: CodexTurn) -> datetime:
    last = turn.prompt.timestamp
    if last is None:
        return datetime.now(UTC)
    for row in turn.responses + turn.tool_calls:
        if row.timestamp is not None and row.timestamp > last:
            last = row.timestamp
    if last.tzinfo is None:
        return last.replace(tzinfo=UTC)
    return last.astimezone(UTC)


def _assembled_codex_turn(
    store: ComplianceStore,
    prompt_stored: StoredCodexEvent,
    *,
    now: datetime,
    idle_minutes: int,
) -> CodexTurn | None:
    session_id = prompt_stored.session_id
    if session_id is None:
        return None
    session_events = store.codex_session_events(session_id)
    records = [parse_stored_codex(row) for row in session_events]
    assembled = assemble_codex_turns(records, now=now, idle_minutes=idle_minutes)
    return next(
        (item for item in assembled if item.interaction_key == prompt_stored.event_id),
        None,
    )


def _emit_one_codex_interaction(
    store: ComplianceStore,
    nebuly: NebulyClient,
    config: Config,
    counts: SourceCounts,
    directory: EntraDirectory | None,
    prompt_stored: StoredCodexEvent,
    *,
    now: datetime,
    wait_delta: timedelta,
    cost_wait_hours: int,
) -> None:
    if prompt_stored.session_id is None:
        counts.skipped_empty += 1
        return
    match = _assembled_codex_turn(
        store,
        prompt_stored,
        now=now,
        idle_minutes=config.interaction_idle_minutes,
    )
    if match is None:
        counts.skipped_empty += 1
        return
    if not match.closed:
        counts.skipped_open += 1
        return
    join_id = codex_turn_join_id(match)
    turn_record = _load_turn_record(store, join_id)
    sent_without_cost = False
    if join_id is not None and turn_record is None and cost_wait_hours > 0:
        last_at = _turn_last_event_at(match)
        if now - last_at < wait_delta:
            counts.skipped_waiting_cost += 1
            return
        sent_without_cost = True

    payload = codex_turn_to_payload(
        match,
        turn_record,
        anonymize=config.anonymize,
        directory=directory,
    )
    if payload is None:
        counts.skipped_empty += 1
        return
    try:
        nebuly.send_interaction(payload)
        store.record_emitted("codex", match.interaction_key, "sent")
        counts.sent += 1
        if sent_without_cost:
            counts.sent_without_cost += 1
    except PermanentRejectionError:
        store.record_emitted("codex", match.interaction_key, "rejected")
        counts.rejected += 1
    if not config.dry_run:
        store.commit()


def _emit_codex_interactions(
    store: ComplianceStore,
    nebuly: NebulyClient,
    config: Config,
    counts: SourceCounts,
    directory: EntraDirectory | None,
    *,
    cost_wait_hours: int,
) -> None:
    now = datetime.now(UTC)
    wait_delta = timedelta(hours=cost_wait_hours)
    for prompt_stored in store.unemitted_codex_prompts():
        _emit_one_codex_interaction(
            store,
            nebuly,
            config,
            counts,
            directory,
            prompt_stored,
            now=now,
            wait_delta=wait_delta,
            cost_wait_hours=cost_wait_hours,
        )


def _sync_codex_source(
    client: ComplianceClient,
    store: ComplianceStore,
    nebuly: NebulyClient,
    config: Config,
    counts: SourceCounts,
    directory: EntraDirectory | None,
) -> bool:
    cost_wait_hours = config.codex_cost_wait_hours
    disable_wait = False
    if cost_wait_hours > 0:
        ingest_ok, disable_wait = _ingest_codex_turn_with_scope_handling(
            client, store, config, counts
        )
        if not ingest_ok:
            return False
    effective_wait = 0 if disable_wait else cost_wait_hours

    if not _ingest_event_type(client, store, config, EVENT_CODEX_LOG, counts):
        return False

    _emit_codex_interactions(
        store,
        nebuly,
        config,
        counts,
        directory,
        cost_wait_hours=effective_wait,
    )
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
        window_start = match.user.message.created_at
        window_end = match.assistants[-1].message.created_at
        if window_start is None or window_end is None:
            counts.skipped_empty += 1
            continue
        app_calls = store.app_calls_in_window(
            match.user.conversation.id,
            start=window_start,
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
        config.compliance_workspace_id,
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
                config.compliance_workspace_id,
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
                    if not _sync_codex_source(
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
                        "Source codex: files=%d rows=%d sent=%d rejected=%d "
                        "skipped_open=%d skipped_empty=%d skipped_waiting_cost=%d "
                        "sent_without_cost=%d",
                        codex_counts.files,
                        codex_counts.rows,
                        codex_counts.sent,
                        codex_counts.rejected,
                        codex_counts.skipped_open,
                        codex_counts.skipped_empty,
                        codex_counts.skipped_waiting_cost,
                        codex_counts.sent_without_cost,
                    )

                if not config.dry_run:
                    store.prune()
                    store.commit()
    finally:
        store.close()

    if stopped:
        logger.error("Stopping after transient failure")
    return summary
