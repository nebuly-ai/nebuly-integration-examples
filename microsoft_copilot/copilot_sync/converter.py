from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from . import parser, user_defined
from .config import datetime_to_timestamp_str

if TYPE_CHECKING:
    from datetime import datetime

    from .models import AiInteraction, CopilotUser


class SkipReason(Enum):
    EMPTY_INPUT = "empty_input"
    EMPTY_OUTPUT = "empty_output"


@dataclass(frozen=True)
class InteractionTurn:
    request_id: str
    prompt: AiInteraction
    responses: tuple[AiInteraction, ...]

    @property
    def final_response(self) -> AiInteraction | None:
        return self.responses[-1] if self.responses else None

    @property
    def time_start(self) -> datetime:
        return self.prompt.created_datetime

    @property
    def time_end(self) -> datetime:
        final = self.final_response
        return final.created_datetime if final else self.prompt.created_datetime


def group_interactions(
    interactions: list[AiInteraction],
) -> tuple[list[InteractionTurn], list[AiInteraction]]:
    """Group interactions by request_id into turns.

    Returns (turns, dangling_prompts) where dangling_prompts are prompts whose
    request_id has no response yet within this window (needed for the coverage
    hold-back in sync.py).
    """
    prompts: dict[str, list[AiInteraction]] = defaultdict(list)
    responses: dict[str, list[AiInteraction]] = defaultdict(list)

    for inter in interactions:
        rid = inter.request_id
        if not rid:
            continue
        if inter.interaction_type == "userPrompt":
            prompts[rid].append(inter)
        elif inter.interaction_type == "aiResponse":
            responses[rid].append(inter)

    turns: list[InteractionTurn] = []
    dangling: list[AiInteraction] = []

    for rid, rid_prompts in prompts.items():
        rid_prompts.sort(key=lambda x: x.created_datetime)
        chosen = next(
            (p for p in rid_prompts if parser.parse_interaction_text(p)),
            rid_prompts[0],
        )
        rid_responses = sorted(responses.get(rid, []), key=lambda x: x.created_datetime)
        if not rid_responses:
            dangling.append(rid_prompts[0])
            continue
        turns.append(InteractionTurn(rid, chosen, tuple(rid_responses)))

    turns.sort(key=lambda t: t.time_start)
    return turns, dangling


def turn_to_payload(
    turn: InteractionTurn, *, user: CopilotUser, anonymize: bool
) -> dict[str, Any] | SkipReason:
    user_input = parser.parse_interaction_text(turn.prompt)
    if not user_input:
        return SkipReason.EMPTY_INPUT

    final = turn.final_response
    if final is None:
        return SkipReason.EMPTY_OUTPUT
    assistant_output = parser.parse_interaction_text(final)
    if not assistant_output:
        return SkipReason.EMPTY_OUTPUT

    interaction = {
        "conversation_id": turn.prompt.session_id,
        "input": user_input,
        "output": assistant_output,
        "time_start": datetime_to_timestamp_str(turn.time_start),
        "time_end": datetime_to_timestamp_str(turn.time_end),
        "end_user": user.id,
        "hide_content": False,
        "tags": user_defined.build_tags(turn),
    }

    return {
        "interaction": interaction,
        "traces": user_defined.build_traces(turn),
        "user_feedback": user_defined.build_user_feedback(turn),
        "anonymize": anonymize,
    }
