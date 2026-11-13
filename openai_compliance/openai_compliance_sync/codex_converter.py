from __future__ import annotations

import contextlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from . import user_defined
from .chat_converter import truncate_text
from .config import datetime_to_timestamp_str
from .entra import EntraDirectory, apply_directory_segments, apply_directory_tags
from .models import CodexLogRecord

if TYPE_CHECKING:
    from .models import CodexEventDetails, CodexTurnRecord
    from .store import StoredCodexEvent

ASSEMBLY_EVENT_TYPES = frozenset(
    {
        "PROMPT_SENT",
        "PROMPT_RESPONSE_RECEIVED",
        "TOOL_CALL_COMPLETED",
        "TOOL_CALL_FAILED",
    }
)


@dataclass
class CodexTurn:
    session_id: str
    prompt: CodexLogRecord
    responses: list[CodexLogRecord] = field(default_factory=list)
    tool_calls: list[CodexLogRecord] = field(default_factory=list)
    closed: bool = False
    turn_id: str | None = None

    @property
    def interaction_key(self) -> str:
        return self.prompt.event_id


def prompt_text(details: CodexEventDetails | None) -> str:
    if details is None:
        return ""
    return (details.prompt_text or "").strip()


def response_text(details: CodexEventDetails | None) -> str:
    if details is None:
        return ""
    return (details.response_text or "").strip()


def _event_time(record: CodexLogRecord) -> datetime:
    if record.timestamp is None:
        raise ValueError(f"event {record.event_id} has no timestamp")
    if record.timestamp.tzinfo is None:
        return record.timestamp.replace(tzinfo=UTC)
    return record.timestamp.astimezone(UTC)


def parse_stored_codex(stored: StoredCodexEvent) -> CodexLogRecord:
    return CodexLogRecord.model_validate(json.loads(stored.record_json))


def codex_turn_join_id(turn: CodexTurn) -> str | None:
    if turn.turn_id:
        return turn.turn_id
    for response in turn.responses:
        turn_id = _detail_turn_id(response)
        if turn_id:
            return turn_id
    return None


def codex_row_store_event_type(record: CodexLogRecord) -> str | None:
    event_type = record.event_type
    if event_type in ASSEMBLY_EVENT_TYPES:
        return event_type
    if _codex_assembly_type(record) == "LEGACY_COMBINED":
        return "LEGACY_COMBINED"
    return None


def _detail_turn_id(record: CodexLogRecord) -> str | None:
    if record.event_details is None:
        return None
    return record.event_details.turn_id


def _codex_assembly_type(record: CodexLogRecord) -> str | None:
    event_type = record.event_type
    if event_type in ASSEMBLY_EVENT_TYPES:
        return event_type
    details = record.event_details
    if (
        details
        and details.detail_type == "PROMPT_RESPONSE_RECEIVED"
        and prompt_text(details)
        and response_text(details)
    ):
        return "LEGACY_COMBINED"
    return None


def _is_error_response(record: CodexLogRecord) -> bool:
    details = record.event_details
    if details is None:
        return False
    status = (details.status or "").casefold()
    return status in {"error", "failed"}


def _assembly_session_id(event: CodexLogRecord) -> str | None:
    details = event.event_details
    if details is None:
        return None
    return details.session_id


def _resolve_turn(
    turns: list[CodexTurn],
    open_turn: CodexTurn | None,
    event: CodexLogRecord,
) -> CodexTurn | None:
    event_turn_id = _detail_turn_id(event)
    if event_turn_id:
        for turn in reversed(turns):
            if turn.turn_id == event_turn_id:
                return turn
        return None
    if open_turn is None:
        return None
    try:
        if _event_time(event) >= _event_time(open_turn.prompt):
            return open_turn
    except ValueError:
        return None
    return None


def _filter_relevant_codex_events(events: list[CodexLogRecord]) -> list[CodexLogRecord]:
    relevant: list[CodexLogRecord] = []
    for event in events:
        kind = _codex_assembly_type(event)
        if kind is None:
            continue
        if kind == "LEGACY_COMBINED" and _assembly_session_id(event) is None:
            continue
        relevant.append(event)
    return relevant


def _group_events_by_session(
    events: list[CodexLogRecord],
) -> dict[str, list[CodexLogRecord]]:
    by_session: dict[str, list[CodexLogRecord]] = defaultdict(list)
    for event in events:
        session_id = _assembly_session_id(event)
        if session_id:
            by_session[session_id].append(event)
    return by_session


def _legacy_turn(session_id: str, event: CodexLogRecord) -> CodexTurn:
    return CodexTurn(
        session_id=session_id,
        prompt=event,
        responses=[event],
        closed=True,
        turn_id=_detail_turn_id(event),
    )


def _handle_prompt_sent(
    session_id: str,
    event: CodexLogRecord,
    session_turns: list[CodexTurn],
    open_turn: CodexTurn | None,
) -> CodexTurn:
    if open_turn is not None:
        open_turn.closed = True
    new_turn = CodexTurn(
        session_id=session_id,
        prompt=event,
        turn_id=_detail_turn_id(event),
    )
    session_turns.append(new_turn)
    return new_turn


def _handle_followup_event(
    event: CodexLogRecord,
    event_type: str,
    session_turns: list[CodexTurn],
    open_turn: CodexTurn | None,
) -> None:
    target = _resolve_turn(session_turns, open_turn, event)
    if target is None:
        return
    if event_type == "PROMPT_RESPONSE_RECEIVED":
        target.responses.append(event)
    elif event_type in {"TOOL_CALL_COMPLETED", "TOOL_CALL_FAILED"}:
        target.tool_calls.append(event)


def _turn_last_activity(turn: CodexTurn) -> datetime:
    last_candidates = [_event_time(turn.prompt)]
    for row in turn.responses + turn.tool_calls:
        with contextlib.suppress(ValueError):
            last_candidates.append(_event_time(row))
    return max(last_candidates)


def _finalize_session_turns(
    session_turns: list[CodexTurn],
    *,
    now_utc: datetime,
    idle_delta: timedelta,
) -> list[CodexTurn]:
    result: list[CodexTurn] = []
    for turn in session_turns:
        if not turn.closed and now_utc - _turn_last_activity(turn) >= idle_delta:
            turn.closed = True
        result.append(turn)
    return result


def _assemble_session_turns(
    session_id: str,
    session_events: list[CodexLogRecord],
    *,
    now_utc: datetime,
    idle_delta: timedelta,
) -> list[CodexTurn]:
    session_events.sort(key=_event_time)
    open_turn: CodexTurn | None = None
    session_turns: list[CodexTurn] = []

    for event in session_events:
        kind = _codex_assembly_type(event)
        if kind == "LEGACY_COMBINED":
            session_turns.append(_legacy_turn(session_id, event))
            continue
        event_type = event.event_type or ""
        if event_type == "PROMPT_SENT":
            open_turn = _handle_prompt_sent(session_id, event, session_turns, open_turn)
            continue
        _handle_followup_event(event, event_type, session_turns, open_turn)

    return _finalize_session_turns(
        session_turns, now_utc=now_utc, idle_delta=idle_delta
    )


def assemble_codex_turns(
    events: list[CodexLogRecord],
    *,
    now: datetime,
    idle_minutes: int,
) -> list[CodexTurn]:
    relevant = _filter_relevant_codex_events(events)
    by_session = _group_events_by_session(relevant)
    idle_delta = timedelta(minutes=idle_minutes)
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)

    all_turns: list[CodexTurn] = []
    for session_id, session_events in by_session.items():
        all_turns.extend(
            _assemble_session_turns(
                session_id,
                session_events,
                now_utc=now_utc,
                idle_delta=idle_delta,
            )
        )
    return all_turns


def _turn_output_text(turn: CodexTurn) -> str:
    parts: list[str] = []
    for record in turn.responses:
        if _is_error_response(record):
            continue
        text = response_text(record.event_details)
        if text:
            parts.append(text)
    return "\n".join(parts)


def _sum_response_tokens(turn: CodexTurn) -> tuple[int, int]:
    input_total = 0
    output_total = 0
    for record in turn.responses:
        if _is_error_response(record):
            continue
        usage = record.event_details.token_usage if record.event_details else None
        if usage is None:
            continue
        input_total += usage.input_tokens or 0
        output_total += usage.output_tokens or 0
    return input_total, output_total


def _turn_llm_model(turn: CodexTurn) -> str:
    for record in reversed(turn.responses):
        details = record.event_details
        if details and details.model:
            return details.model
    details = turn.prompt.event_details
    if details and details.model:
        return details.model
    return "unknown"


def _micro_cost(turn_record: CodexTurnRecord | None) -> int | None:
    if turn_record is None:
        return None
    payload = turn_record.payload
    if payload.cost_usd is None and payload.estimated_cost_usd is None:
        return None
    total = (payload.cost_usd or 0.0) + (payload.estimated_cost_usd or 0.0)
    return round(total * 1_000_000)


def _tokens_from_turn_record(
    turn_record: CodexTurnRecord | None,
    fallback_input: int,
    fallback_output: int,
) -> tuple[int, int]:
    if turn_record is None:
        return fallback_input, fallback_output
    usage = turn_record.payload.token_usage
    if usage is None:
        return fallback_input, fallback_output
    input_tokens = usage.input_tokens
    if input_tokens is None and (
        usage.uncached_input_tokens is not None or usage.cached_input_tokens is not None
    ):
        input_tokens = (usage.uncached_input_tokens or 0) + (
            usage.cached_input_tokens or 0
        )
    output_tokens = usage.output_tokens
    return input_tokens or fallback_input, output_tokens or fallback_output


def codex_turn_to_payload(
    turn: CodexTurn,
    turn_record: CodexTurnRecord | None,
    *,
    anonymize: bool,
    directory: EntraDirectory | None = None,
) -> dict[str, Any] | None:
    if not turn.closed:
        return None
    prompt = prompt_text(turn.prompt.event_details)
    output = _turn_output_text(turn)
    if not prompt or not output:
        return None

    actor = turn.prompt.actor
    end_user = actor.user_id if actor and actor.user_id else ""
    email = actor.user_email if actor else None

    tags = apply_directory_tags(
        user_defined.build_codex_tags(turn, turn_record),
        directory,
        email,
    )
    input_tokens, output_tokens = _sum_response_tokens(turn)
    input_tokens, output_tokens = _tokens_from_turn_record(
        turn_record, input_tokens, output_tokens
    )

    time_start = datetime_to_timestamp_str(_event_time(turn.prompt))
    last_times = [_event_time(turn.prompt)]
    for row in turn.responses + turn.tool_calls:
        with contextlib.suppress(ValueError):
            last_times.append(_event_time(row))
    time_end = datetime_to_timestamp_str(max(last_times))

    interaction: dict[str, Any] = {
        "conversation_id": turn.session_id,
        "input": truncate_text(prompt),
        "output": truncate_text(output),
        "time_start": time_start,
        "time_end": time_end,
        "end_user": end_user,
        "hide_content": False,
        "tags": tags,
    }
    cost = _micro_cost(turn_record)
    if cost is not None:
        interaction["cost"] = cost
    apply_directory_segments(interaction, directory, email)

    return {
        "interaction": interaction,
        "traces": user_defined.build_codex_traces(
            turn,
            llm_model=_turn_llm_model(turn),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            output_text=output,
        ),
        "user_feedback": user_defined.build_codex_user_feedback(turn),
        "anonymize": anonymize,
    }


def _is_legacy_combined(record: CodexLogRecord) -> bool:
    return _codex_assembly_type(record) == "LEGACY_COMBINED"


def codex_record_to_payload(
    record: CodexLogRecord,
    *,
    anonymize: bool,
    directory: EntraDirectory | None = None,
) -> dict[str, Any] | None:
    """Backward-compatible single-row entry point used by sync until emit task 5."""
    now = datetime.now(UTC)
    if record.timestamp is not None:
        now = max(now, _event_time(record))
    if _is_legacy_combined(record):
        session_id = _assembly_session_id(record)
        if session_id is None:
            return None
        turn = _legacy_turn(session_id, record)
        return codex_turn_to_payload(
            turn, None, anonymize=anonymize, directory=directory
        )
    turns = assemble_codex_turns([record], now=now, idle_minutes=0)
    if not turns:
        return None
    turn = turns[0]
    return codex_turn_to_payload(
        turn, turn_record=None, anonymize=anonymize, directory=directory
    )
