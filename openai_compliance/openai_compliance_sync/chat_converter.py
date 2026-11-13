from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from . import user_defined
from .config import datetime_to_timestamp_str
from .entra import EntraDirectory, apply_directory_segments, apply_directory_tags
from .models import AppLogRecord, ConversationMessageRecord

if TYPE_CHECKING:
    from .store import StoredAppCall, StoredChatMessage

MAX_CONTENT_CHARS = 8000


@dataclass(frozen=True)
class AssembledInteraction:
    interaction_key: str
    user: ConversationMessageRecord
    assistants: list[ConversationMessageRecord]
    closed: bool
    regenerated: bool = False
    edited: bool = False

    @property
    def llm_model(self) -> str:
        for record in reversed(self.assistants):
            if record.message.author and record.message.author.model:
                return record.message.author.model
        return "unknown"

    @property
    def models_tag(self) -> str:
        seen: set[str] = set()
        models: list[str] = []
        for record in self.assistants:
            model = record.message.author.model if record.message.author else None
            if model and model not in seen:
                seen.add(model)
                models.append(model)
        return ",".join(models) if models else "unknown"

    def assistant_output_text(self) -> str:
        parts = [
            record.message.content.value
            for record in self.assistants
            if record.message.content and record.message.content.value
        ]
        return "\n".join(parts)


def truncate_text(text: str) -> str:
    if len(text) <= MAX_CONTENT_CHARS:
        return text
    return text[:MAX_CONTENT_CHARS]


def user_text(record: ConversationMessageRecord) -> str:
    if record.message.content is None or record.message.content.value is None:
        return ""
    return record.message.content.value


def message_effective_time(record: ConversationMessageRecord) -> datetime:
    if record.message.created_at is not None:
        dt = record.message.created_at
    elif record.timestamp is not None:
        dt = record.timestamp
    else:
        raise ValueError(f"message {record.message.id} has no created_at or timestamp")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_stored_message(stored: StoredChatMessage) -> ConversationMessageRecord:
    return ConversationMessageRecord.model_validate(json.loads(stored.record_json))


def _author_role(record: ConversationMessageRecord) -> str:
    if record.message.author is None or record.message.author.type is None:
        return ""
    return record.message.author.type.casefold()


def _build_children(
    records: list[ConversationMessageRecord],
) -> dict[str, list[ConversationMessageRecord]]:
    children: dict[str, list[ConversationMessageRecord]] = defaultdict(list)
    for record in records:
        parent = record.previous_message_id
        if parent is not None:
            children[parent].append(record)
    return children


def _user_is_edited(
    user: ConversationMessageRecord, records: list[ConversationMessageRecord]
) -> bool:
    user_time = message_effective_time(user)
    parent = user.previous_message_id
    for other in records:
        if _author_role(other) != "user" or other.message.id == user.message.id:
            continue
        if other.previous_message_id != parent:
            continue
        try:
            other_time = message_effective_time(other)
        except ValueError:
            continue
        if other_time < user_time:
            return True
    return False


def _assistant_chain(
    user: ConversationMessageRecord,
    children: dict[str, list[ConversationMessageRecord]],
) -> tuple[list[ConversationMessageRecord], bool, bool]:
    chain: list[ConversationMessageRecord] = []
    parent_id = user.message.id
    closed_by_next_user = False
    regenerated = False
    while True:
        siblings = children.get(parent_id, [])
        if not siblings:
            break
        user_children = [row for row in siblings if _author_role(row) == "user"]
        if user_children:
            closed_by_next_user = True
            break
        assistants = [row for row in siblings if _author_role(row) != "user"]
        if not assistants:
            break
        if len(assistants) > 1:
            regenerated = True
        next_assistant = max(assistants, key=message_effective_time)
        chain.append(next_assistant)
        parent_id = next_assistant.message.id
    return chain, closed_by_next_user, regenerated


def assemble_interactions(
    records: list[ConversationMessageRecord],
    *,
    now: datetime,
    idle_minutes: int,
) -> list[AssembledInteraction]:
    if not records:
        return []
    children = _build_children(records)
    idle_delta = timedelta(minutes=idle_minutes)
    users = sorted(
        (row for row in records if _author_role(row) == "user"),
        key=message_effective_time,
    )
    assembled: list[AssembledInteraction] = []
    for user in users:
        assistants, closed_by_next_user, regenerated = _assistant_chain(user, children)
        if closed_by_next_user:
            closed = True
        elif not assistants:
            closed = False
        else:
            last_at = message_effective_time(assistants[-1])
            closed = now.astimezone(UTC) - last_at >= idle_delta
        assembled.append(
            AssembledInteraction(
                interaction_key=user.message.id,
                user=user,
                assistants=assistants,
                closed=closed,
                regenerated=regenerated,
                edited=_user_is_edited(user, records),
            )
        )
    return assembled


def _app_trace_input(record: AppLogRecord) -> str:
    if record.input is not None:
        return truncate_text(
            json.dumps(record.input, separators=(",", ":"), ensure_ascii=False)
        )
    return truncate_text(record.request_input() or "")


def _app_trace_outputs(record: AppLogRecord) -> list[str]:
    if record.output:
        return [
            truncate_text(json.dumps(item, separators=(",", ":"), ensure_ascii=False))
            for item in record.output
        ]
    legacy = record.response_output_text()
    if legacy:
        return [truncate_text(legacy)]
    return []


def app_calls_to_traces(calls: list[StoredAppCall]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], list[StoredAppCall]] = defaultdict(list)
    for call in calls:
        by_key[(call.conversation_id, call.app_id)].append(call)

    traces: list[dict[str, Any]] = []
    for (_conversation_id, app_id), app_calls in by_key.items():
        pending: list[StoredAppCall] = []
        for call in sorted(app_calls, key=lambda row: row.timestamp):
            if call.record_kind == "request":
                pending.append(call)
            elif call.record_kind == "response":
                if not pending:
                    continue
                request_row = pending.pop()
                request = AppLogRecord.model_validate(
                    json.loads(request_row.record_json)
                )
                response = AppLogRecord.model_validate(json.loads(call.record_json))
                traces.append(
                    {
                        "source": request.app_name or app_id,
                        "input": _app_trace_input(request),
                        "outputs": _app_trace_outputs(response),
                    }
                )
        for request_row in pending:
            request = AppLogRecord.model_validate(json.loads(request_row.record_json))
            traces.append(
                {
                    "source": request.app_name or app_id,
                    "input": _app_trace_input(request),
                    "outputs": [],
                }
            )
    return traces


def interaction_to_payload(
    interaction: AssembledInteraction,
    *,
    retrieval_traces: list[dict[str, Any]],
    anonymize: bool,
    directory: EntraDirectory | None = None,
) -> dict[str, Any] | None:
    if not interaction.closed:
        return None
    user_input = user_text(interaction.user).strip()
    if not user_input:
        return None
    if not interaction.assistants:
        return None

    actor = interaction.user.actor
    end_user = actor.user_id if actor and actor.user_id else ""
    email = actor.user_email if actor else None

    tags = apply_directory_tags(
        user_defined.build_chat_tags(interaction),
        directory,
        email,
    )
    last_assistant = interaction.assistants[-1]
    nebuly_interaction = {
        "conversation_id": interaction.user.conversation.id,
        "input": truncate_text(user_input),
        "output": truncate_text(interaction.assistant_output_text()),
        "time_start": datetime_to_timestamp_str(
            message_effective_time(interaction.user)
        ),
        "time_end": datetime_to_timestamp_str(message_effective_time(last_assistant)),
        "end_user": end_user,
        "hide_content": False,
        "tags": tags,
    }
    apply_directory_segments(nebuly_interaction, directory, email)

    return {
        "interaction": nebuly_interaction,
        "traces": user_defined.build_chat_traces(interaction, retrieval_traces),
        "user_feedback": user_defined.build_chat_user_feedback(interaction),
        "anonymize": anonymize,
    }
