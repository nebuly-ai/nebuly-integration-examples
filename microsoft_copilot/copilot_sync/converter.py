from __future__ import annotations

import html
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

from . import parser, user_defined
from .config import datetime_to_timestamp_str

if TYPE_CHECKING:
    from datetime import datetime

    from .models import AiInteraction, CopilotAuditRecord, CopilotUser

logger = logging.getLogger(__name__)


class SkipReason(Enum):
    EMPTY_INPUT = "empty_input"
    EMPTY_OUTPUT = "empty_output"
    WARMUP_REQUEST = "warmup_request"


@dataclass(frozen=True)
class InteractionTurn:
    request_id: str | None
    prompt: AiInteraction
    responses: tuple[AiInteraction, ...]
    followups: tuple[AiInteraction, ...] = ()

    @property
    def final_response(self) -> AiInteraction | None:
        return self.responses[-1] if self.responses else None

    @property
    def text_responses(self) -> tuple[AiInteraction, ...]:
        """Responses carrying user-facing text, in chronological order."""
        return tuple(
            response
            for response in self.responses
            if (text := parser.parse_interaction_text(response))
            and not parser.is_warmup_response_text(text)
        )

    @property
    def effective_response(self) -> AiInteraction | None:
        text_responses = self.text_responses
        return text_responses[-1] if text_responses else None

    @property
    def time_start(self) -> datetime:
        return self.prompt.created_datetime

    @property
    def time_end(self) -> datetime:
        effective = self.effective_response
        return effective.created_datetime if effective else self.prompt.created_datetime


_TYPE_RANK = {"userPrompt": 0, "aiResponse": 1}
_RELEVANT_INTERACTION_TYPES = frozenset({"userPrompt", "aiResponse"})


def _group_by_session(
    interactions: list[AiInteraction],
) -> dict[str, list[AiInteraction]]:
    by_session: dict[str, list[AiInteraction]] = defaultdict(list)
    for inter in interactions:
        if inter.interaction_type in _RELEVANT_INTERACTION_TYPES:
            by_session[inter.session_id].append(inter)
    return by_session


def _consume_prompt_run(
    session_interactions: list[AiInteraction], start: int, n: int
) -> tuple[list[AiInteraction], int]:
    first_dt = session_interactions[start].created_datetime
    end = start
    while end < n and session_interactions[end].interaction_type == "userPrompt":
        if session_interactions[end].created_datetime != first_dt:
            break
        end += 1
    return session_interactions[start:end], end


def _consume_response_run(
    session_interactions: list[AiInteraction], start: int, n: int
) -> tuple[list[AiInteraction], int]:
    end = start
    while end < n and session_interactions[end].interaction_type == "aiResponse":
        end += 1
    return session_interactions[start:end], end


def _chosen_prompt(prompt_run: list[AiInteraction]) -> AiInteraction:
    return next(
        (p for p in prompt_run if parser.parse_interaction_text(p)),
        prompt_run[0],
    )


def _process_session_interactions(
    session_interactions: list[AiInteraction],
) -> tuple[list[InteractionTurn], list[AiInteraction]]:
    session_interactions.sort(
        key=lambda x: (x.created_datetime, _TYPE_RANK[x.interaction_type])
    )

    turns: list[InteractionTurn] = []
    dangling: list[AiInteraction] = []
    i = 0
    n = len(session_interactions)

    while i < n:
        if session_interactions[i].interaction_type == "aiResponse":
            i += 1
            continue

        prompt_run, i = _consume_prompt_run(session_interactions, i, n)
        chosen = _chosen_prompt(prompt_run)
        responses, i = _consume_response_run(session_interactions, i, n)

        if responses:
            turns.append(InteractionTurn(chosen.request_id, chosen, tuple(responses)))
        elif i >= n:
            dangling.append(prompt_run[0])

    return turns, dangling


_COWORK_APP_CLASS_SUFFIX = ".CoworkChat"
# Cowork stores several visible messages in one record with no separator,
# e.g. "...was declined.Moved the meeting...".
_GLUED_MESSAGES_RE = re.compile(r"(?<=[a-zà-ÿ0-9*)\]])([.!?])(?=[A-ZÀ-Þ][a-zà-ÿ])")
_SENTENCE_END_RE = re.compile(r"[.!?:][*_)\]]*$")
# Cowork reasoning summaries open with a bold heading, e.g.
# "**Finding an efficient solution** ...".
_REASONING_RE = re.compile(r"^\*\*[^*\n]+\*\*")


def _is_cowork(interaction: AiInteraction) -> bool:
    return interaction.app_class.endswith(_COWORK_APP_CLASS_SUFFIX)


def _merge_cowork_followups(turns: list[InteractionTurn]) -> list[InteractionTurn]:
    """Fold Cowork steering prompts into the task they were sent during.

    Cowork runs a long agent task. While it works it exports aiResponse
    records with no text, and a prompt sent meanwhile steers the same task.
    The task's text answers arrive after that prompt, with nothing linking
    them to a specific prompt, so a prompt arriving before the current turn
    has produced any text is merged into it.
    """
    merged: list[InteractionTurn] = []
    for turn in turns:
        previous = merged[-1] if merged else None
        if (
            previous is not None
            and _is_cowork(previous.prompt)
            and previous.effective_response is None
        ):
            merged[-1] = InteractionTurn(
                previous.request_id,
                previous.prompt,
                previous.responses + turn.responses,
                (*previous.followups, turn.prompt, *turn.followups),
            )
        else:
            merged.append(turn)
    return merged


def _cowork_response_text(response: AiInteraction) -> str:
    text = html.unescape(parser.parse_interaction_text(response))
    return _GLUED_MESSAGES_RE.sub(r"\1\n\n", text)


def _join_cowork_responses(responses: tuple[AiInteraction, ...]) -> str:
    """Join Cowork records: chunks of one message split mid-sentence get a
    space, records after a finished sentence start a new paragraph.
    Reasoning summaries are dropped unless they are the final record."""
    texts = [_cowork_response_text(response) for response in responses]
    kept = [
        text
        for index, text in enumerate(texts)
        if index == len(texts) - 1 or not _REASONING_RE.match(text)
    ]
    output = ""
    for text in kept:
        if not output:
            output = text
        elif _SENTENCE_END_RE.search(output):
            output = f"{output}\n\n{text}"
        else:
            output = f"{output} {text}"
    return output


def _turn_input(turn: InteractionTurn) -> str:
    prompts = (turn.prompt, *turn.followups)
    texts = [text for p in prompts if (text := parser.parse_interaction_text(p))]
    return "\n\n".join(texts)


_DUPLICATE_WINDOW = timedelta(seconds=5)


def _drop_near_duplicates(turns: list[InteractionTurn]) -> list[InteractionTurn]:
    """Drop turns the Microsoft Graph API duplicated with a small timestamp jitter.

    The API occasionally emits the same interaction twice (same session, same
    prompt/response text) with createdDateTime differing by a second or two and
    a fresh id/request_id. Exact-timestamp grouping in _consume_prompt_run does
    not catch these, so they would otherwise be sent to Nebuly as two distinct
    interactions. We treat a turn as a duplicate when an earlier kept turn in
    the same conversation has identical parsed input and output and started
    within _DUPLICATE_WINDOW.
    """
    sorted_turns = sorted(turns, key=lambda t: t.time_start)
    kept: list[InteractionTurn] = []
    first_kept_time: dict[tuple[str, str, str], datetime] = {}

    for turn in sorted_turns:
        effective = turn.effective_response
        if effective is None:
            kept.append(turn)
            continue

        output_text = parser.parse_interaction_text(effective)
        if not output_text:
            kept.append(turn)
            continue

        input_text = parser.parse_interaction_text(turn.prompt)
        key = (turn.prompt.session_id, input_text, output_text)

        anchor = first_kept_time.get(key)
        if anchor is not None and turn.time_start - anchor <= _DUPLICATE_WINDOW:
            logger.debug(
                "Dropping near-duplicate turn session_id=%s input=%r",
                turn.prompt.session_id,
                input_text[:80],
            )
            continue

        kept.append(turn)
        if anchor is None:
            first_kept_time[key] = turn.time_start

    return kept


def group_interactions(
    interactions: list[AiInteraction],
) -> tuple[list[InteractionTurn], list[AiInteraction]]:
    """Group interactions by chronological adjacency within each session into turns.

    Returns (turns, dangling_prompts) where dangling_prompts are trailing prompts
    with no response yet within this window (needed for the coverage hold-back
    in sync.py).
    """
    turns: list[InteractionTurn] = []
    dangling: list[AiInteraction] = []

    for session_interactions in _group_by_session(interactions).values():
        session_turns, session_dangling = _process_session_interactions(
            session_interactions
        )
        if session_turns and _is_cowork(session_turns[0].prompt):
            session_turns = _merge_cowork_followups(session_turns)
        turns.extend(session_turns)
        dangling.extend(session_dangling)

    turns = _drop_near_duplicates(turns)
    turns.sort(key=lambda t: t.time_start)
    return turns, dangling


def _assistant_output_for_turn(turn: InteractionTurn) -> tuple[str, SkipReason | None]:
    effective = turn.effective_response
    if effective is not None:
        if _is_cowork(turn.prompt):
            return _join_cowork_responses(turn.text_responses), None
        return parser.parse_interaction_text(effective), None
    for response in turn.responses:
        text = parser.parse_interaction_text(response)
        if text and parser.is_warmup_response_text(text):
            return "", SkipReason.WARMUP_REQUEST
    return "", SkipReason.EMPTY_OUTPUT


def turn_to_payload(
    turn: InteractionTurn,
    *,
    user: CopilotUser,
    anonymize: bool,
    audit: CopilotAuditRecord | None = None,
) -> dict[str, Any] | SkipReason:
    user_input = _turn_input(turn)
    if not user_input:
        return SkipReason.EMPTY_INPUT

    if not turn.responses:
        return SkipReason.EMPTY_OUTPUT
    assistant_output, skip = _assistant_output_for_turn(turn)
    if skip is not None:
        return skip
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
        "tags": user_defined.build_tags(turn, user, audit=audit),
        **user_defined.build_segments(user),
    }

    return {
        "interaction": interaction,
        "traces": user_defined.build_traces(
            turn,
            audit=audit,
            user_input=user_input,
            assistant_output=assistant_output,
        ),
        "user_feedback": user_defined.build_user_feedback(turn),
        "anonymize": anonymize,
    }
