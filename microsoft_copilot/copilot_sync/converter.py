from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from . import user_defined
from .config import datetime_to_timestamp_str

if TYPE_CHECKING:
    from .models import AiInteraction, CopilotUser


@dataclass(frozen=True)
class InteractionPair:
    prompt: AiInteraction
    response: AiInteraction


def pair_interactions(interactions: list[AiInteraction]) -> list[InteractionPair]:
    pending_prompts: dict[str, AiInteraction] = {}
    pending_responses: dict[str, AiInteraction] = {}
    pairs: list[InteractionPair] = []

    for inter in interactions:
        rid = inter.request_id
        if not rid:
            continue
        if inter.interaction_type == "userPrompt":
            if rid in pending_responses:
                pairs.append(InteractionPair(inter, pending_responses.pop(rid)))
            else:
                pending_prompts[rid] = inter
        elif inter.interaction_type == "aiResponse":
            if rid in pending_prompts:
                pairs.append(InteractionPair(pending_prompts.pop(rid), inter))
            else:
                pending_responses[rid] = inter

    return pairs


def _interaction_time(inter: AiInteraction) -> datetime:
    if inter.created_date_time is not None:
        return inter.created_date_time
    return datetime.now(tz=UTC)


def pair_to_payload(
    pair: InteractionPair,
    *,
    user: CopilotUser,
    anonymize: bool,
) -> dict[str, Any] | None:
    user_input = (pair.prompt.body.content if pair.prompt.body else None) or ""
    if not user_input:
        return None

    assistant_output = (
        pair.response.body.content if pair.response.body else None
    ) or ""
    time_start = _interaction_time(pair.prompt)
    time_end = (
        pair.response.completed_date_time
        or pair.response.created_date_time
        or time_start
    )

    interaction = {
        "conversation_id": pair.prompt.session_id or pair.prompt.request_id or user.id,
        "input": user_input,
        "output": assistant_output,
        "time_start": datetime_to_timestamp_str(time_start),
        "time_end": datetime_to_timestamp_str(time_end),
        "end_user": user.email,
        "hide_content": False,
        "tags": user_defined.build_tags(pair),
    }

    return {
        "interaction": interaction,
        "traces": user_defined.build_traces(pair),
        "user_feedback": user_defined.build_user_feedback(pair),
        "anonymize": anonymize,
    }
