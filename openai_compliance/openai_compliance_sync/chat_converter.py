from __future__ import annotations

import json
from collections import defaultdict, deque
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


def _assistant_chain(
    user: ConversationMessageRecord,
    children: dict[str, list[ConversationMessageRecord]],
) -> tuple[list[ConversationMessageRecord], bool]:
    chain: list[ConversationMessageRecord] = []
    parent_id = user.message.id
    closed_by_next_user = False
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
        next_assistant = max(assistants, key=lambda row: row.message.created_at)
        chain.append(next_assistant)
        parent_id = next_assistant.message.id
    return chain, closed_by_next_user


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
        key=lambda row: row.message.created_at,
    )
    assembled: list[AssembledInteraction] = []
    for user in users:
        assistants, closed_by_next_user = _assistant_chain(user, children)
        if closed_by_next_user:
            closed = True
        elif not assistants:
            closed = False
        else:
            last_at = assistants[-1].message.created_at
            if last_at.tzinfo is None:
                last_at = last_at.replace(tzinfo=UTC)
            closed = now - last_at.astimezone(UTC) >= idle_delta
        assembled.append(
            AssembledInteraction(
                interaction_key=user.message.id,
                user=user,
                assistants=assistants,
                closed=closed,
            )
        )
    return assembled


def app_calls_to_traces(calls: list[StoredAppCall]) -> list[dict[str, Any]]:
    by_app: dict[str, list[StoredAppCall]] = defaultdict(list)
    for call in calls:
        by_app[call.app_id].append(call)

    traces: list[dict[str, Any]] = []
    for app_id, app_calls in by_app.items():
        pending_requests: deque[StoredAppCall] = deque()
        for call in sorted(app_calls, key=lambda row: row.timestamp):
            if call.record_kind == "request":
                pending_requests.append(call)
            elif call.record_kind == "response" and pending_requests:
                request_row = pending_requests.popleft()
                request = AppLogRecord.model_validate(
                    json.loads(request_row.record_json)
                )
                response = AppLogRecord.model_validate(json.loads(call.record_json))
                input_text = request.request_input() or ""
                output_text = response.response_output_text() or ""
                traces.append(
                    {
                        "source": request.app_name or app_id,
                        "input": input_text,
                        "outputs": [output_text] if output_text else [],
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
        "time_start": datetime_to_timestamp_str(interaction.user.message.created_at),
        "time_end": datetime_to_timestamp_str(last_assistant.message.created_at),
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
