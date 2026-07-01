from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from . import user_defined
from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

if TYPE_CHECKING:
    from datetime import datetime

    from .models import Event, Part, Session

logger = logging.getLogger(__name__)


class SkipReason(Enum):
    EMPTY_INPUT = "empty_input"
    EMPTY_OUTPUT = "empty_output"


@dataclass(frozen=True)
class Turn:
    invocation_id: str
    events: tuple[Event, ...]
    user_event: Event | None
    agent_events: tuple[Event, ...]

    @property
    def final_response(self) -> Event | None:
        for event in reversed(self.agent_events):
            if event.content and event.content.role == "model":
                text = extract_text_from_event(event)
                if text:
                    return event
        return None

    @property
    def reasoning_events(self) -> tuple[Event, ...]:
        final = self.final_response
        if final is None:
            return self.agent_events
        return tuple(e for e in self.agent_events if e is not final)

    @property
    def time_start(self) -> datetime:
        if self.user_event is not None:
            return timestamp_str_to_datetime(self.user_event.timestamp)
        return timestamp_str_to_datetime(self.events[0].timestamp)

    @property
    def time_end(self) -> datetime:
        final = self.final_response
        if final is not None:
            return timestamp_str_to_datetime(final.timestamp)
        if self.user_event is not None:
            return timestamp_str_to_datetime(self.user_event.timestamp)
        return timestamp_str_to_datetime(self.events[-1].timestamp)


def _group_key(event: Event, fallback_index: int) -> str:
    if event.invocation_id:
        return event.invocation_id
    return f"__missing_invocation_{fallback_index}"


def group_turns(events: list[Event]) -> list[Turn]:
    if not events:
        return []

    sorted_events = sorted(
        events,
        key=lambda e: (timestamp_str_to_datetime(e.timestamp), e.name or ""),
    )

    groups: dict[str, list[Event]] = {}
    group_order: list[str] = []
    for index, event in enumerate(sorted_events):
        key = _group_key(event, index)
        if key not in groups:
            groups[key] = []
            group_order.append(key)
        groups[key].append(event)

    turns: list[Turn] = []
    for key in group_order:
        group_events = groups[key]
        user_events = [e for e in group_events if e.author == "user"]
        agent_events = [e for e in group_events if e.author != "user"]
        user_event = user_events[0] if user_events else None
        if len(user_events) > 1:
            logger.warning(
                "Multiple user events in invocation %s; using first only", key
            )
        turns.append(
            Turn(
                invocation_id=key,
                events=tuple(group_events),
                user_event=user_event,
                agent_events=tuple(agent_events),
            )
        )
    return turns


def render_part(part: Part) -> str:
    if part.text:
        return part.text
    if part.function_call:
        name = part.function_call.get("name", "unknown")
        args = part.function_call.get("args", {})
        return f"{name}({json.dumps(args, ensure_ascii=False)})"
    if part.function_response:
        name = part.function_response.get("name", "unknown")
        response = part.function_response.get("response", {})
        return f"{name}({json.dumps(response, ensure_ascii=False)})"
    return ""


def extract_text_from_event(event: Event) -> str:
    if not event.content:
        return ""
    parts = [render_part(part) for part in event.content.parts]
    return "".join(p for p in parts if p).strip()


def event_to_message(event: Event) -> dict[str, str]:
    role = event.content.role if event.content and event.content.role else "user"
    return {"role": role, "content": extract_text_from_event(event)}


def turn_to_payload(
    turn: Turn, *, session: Session, anonymize: bool
) -> dict[str, Any] | SkipReason:
    user_input = (
        extract_text_from_event(turn.user_event) if turn.user_event is not None else ""
    )
    if not user_input:
        return SkipReason.EMPTY_INPUT

    final = turn.final_response
    if final is None:
        return SkipReason.EMPTY_OUTPUT
    assistant_output = extract_text_from_event(final)
    if not assistant_output:
        return SkipReason.EMPTY_OUTPUT

    return {
        "interaction": {
            "conversation_id": session.session_id,
            "input": user_input,
            "output": assistant_output,
            "time_start": datetime_to_timestamp_str(turn.time_start),
            "time_end": datetime_to_timestamp_str(turn.time_end),
            "end_user": session.user_id,
            "hide_content": False,
            "tags": user_defined.build_tags(turn, session),
        },
        "traces": user_defined.build_traces(turn),
        "user_feedback": user_defined.build_user_feedback(turn),
        "anonymize": anonymize,
    }
