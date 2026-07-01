from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from . import user_defined
from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

if TYPE_CHECKING:
    from .models import Reply, Session, SessionTurn, TextGroundingMetadata


class SkipReason(Enum):
    EMPTY_INPUT = "empty_input"
    EMPTY_OUTPUT = "empty_output"


@dataclass(frozen=True)
class Turn:
    session_turn: SessionTurn
    index: int

    @property
    def query_id(self) -> str:
        if self.session_turn.query and self.session_turn.query.query_id:
            return self.session_turn.query.query_id
        return f"__missing_query_{self.index}"

    @property
    def user_input(self) -> str:
        if self.session_turn.query and self.session_turn.query.text:
            return self.session_turn.query.text.strip()
        return ""

    @property
    def replies(self) -> tuple[Reply, ...]:
        answer = self.session_turn.detailed_assist_answer
        if answer is None:
            return ()
        return tuple(answer.replies)

    @property
    def final_answer_text(self) -> str:
        parts: list[str] = []
        for reply in self.replies:
            grounded = reply.grounded_content
            if grounded is None or grounded.content is None:
                continue
            content = grounded.content
            if content.text and not content.thought and content.inline_data is None:
                parts.append(content.text)
        return "".join(parts).strip()

    @property
    def grounding(self) -> list[TextGroundingMetadata]:
        metadata: list[TextGroundingMetadata] = []
        for reply in self.replies:
            grounded = reply.grounded_content
            if grounded is None or grounded.text_grounding_metadata is None:
                continue
            metadata.append(grounded.text_grounding_metadata)
        return metadata

    @property
    def answer_state(self) -> str:
        answer = self.session_turn.detailed_assist_answer
        return answer.state if answer and answer.state else ""

    def time_start(self, session: Session) -> datetime:
        times = _reply_create_times(self.replies)
        if times:
            return min(times)
        if session.start_time:
            return timestamp_str_to_datetime(session.start_time)
        raise ValueError(f"Turn {self.query_id} has no reply create times")

    def time_end(self, session: Session) -> datetime:
        times = _reply_create_times(self.replies)
        if times:
            return max(times)
        if session.end_time:
            return timestamp_str_to_datetime(session.end_time)
        raise ValueError(f"Turn {self.query_id} has no reply create times")


def _reply_create_times(replies: tuple[Reply, ...]) -> list[datetime]:
    return [
        timestamp_str_to_datetime(reply.create_time)
        for reply in replies
        if reply.create_time
    ]


def _first_reply_time(turn: SessionTurn) -> datetime | None:
    answer = turn.detailed_assist_answer
    if answer is None:
        return None
    for reply in answer.replies:
        if reply.create_time:
            return timestamp_str_to_datetime(reply.create_time)
    return None


def session_to_turns(session: Session) -> list[Turn]:
    indexed = list(enumerate(session.turns))
    indexed.sort(
        key=lambda item: (
            _first_reply_time(item[1]) or datetime.max.replace(tzinfo=UTC),
            item[0],
        )
    )
    return [Turn(session_turn=turn, index=index) for index, turn in indexed]


def turn_to_payload(
    turn: Turn, *, session: Session, anonymize: bool
) -> dict[str, Any] | SkipReason:
    user_input = turn.user_input
    if not user_input:
        return SkipReason.EMPTY_INPUT

    assistant_output = turn.final_answer_text
    if not assistant_output:
        return SkipReason.EMPTY_OUTPUT

    return {
        "interaction": {
            "conversation_id": session.session_id,
            "input": user_input,
            "output": assistant_output,
            "time_start": datetime_to_timestamp_str(turn.time_start(session)),
            "time_end": datetime_to_timestamp_str(turn.time_end(session)),
            "end_user": session.user_id,
            "hide_content": False,
            "tags": user_defined.build_tags(turn, session),
        },
        "traces": user_defined.build_traces(turn),
        "user_feedback": user_defined.build_user_feedback(turn),
        "anonymize": anonymize,
    }
