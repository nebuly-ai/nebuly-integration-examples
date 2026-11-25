"""Group paired calls into conversations and turns."""

from __future__ import annotations

from dataclasses import dataclass, field

from . import user_defined
from .models import (
    Call,
    ClosedTurn,
    PipelineCounters,
    digest,
    is_chat_completions_endpoint,
)
from .openai_body import (
    BodyFormatError,
    first_user_message_text,
    turn_key_for_call,
)
from .store import Store


@dataclass
class _OpenTurn:
    turn_key: str
    conversation_id: str
    user_id: int
    calls: list[Call] = field(default_factory=list)

    @property
    def time_start_ms(self) -> int:
        return min(call.request_timestamp_ms for call in self.calls)

    @property
    def time_end_ms(self) -> int:
        return max(call.response_timestamp_ms for call in self.calls)


class ConversationBuilder:
    def __init__(
        self,
        store: Store,
        conversation_gap_hours: int,
        interaction_idle_minutes: int,
    ) -> None:
        self._store = store
        self._gap_ms = conversation_gap_hours * 3_600_000
        self._idle_ms = interaction_idle_minutes * 60_000
        self._open_by_turn: dict[str, _OpenTurn] = {}
        self._open_by_conversation: dict[str, set[str]] = {}
        self.counters = PipelineCounters()

    def add_call(self, call: Call) -> list[ClosedTurn]:
        if not is_chat_completions_endpoint(call.endpoint):
            self.counters.skipped_non_chat += 1
            return []
        request_json = self._request_dict(call)
        if user_defined.is_background_call(call) or (
            request_json is not None
            and user_defined.default_is_background_call(call, request_json)
        ):
            self.counters.skipped_background += 1
            return []

        turn_key = turn_key_for_call(call.request_body, call.user_id)
        if turn_key is None:
            self.counters.unparseable_bodies += 1
            return []

        conversation_id = self._conversation_id(call)
        closed: list[ClosedTurn] = []
        closed.extend(
            self._close_other_turns_in_conversation(conversation_id, turn_key)
        )

        existing = self._open_by_turn.get(turn_key)
        if existing is not None and existing.conversation_id != conversation_id:
            closed.append(self._close_turn(turn_key))
            existing = None
        if existing is None:
            existing = _OpenTurn(
                turn_key=turn_key,
                conversation_id=conversation_id,
                user_id=call.user_id,
            )
            self._open_by_turn[turn_key] = existing
            self._open_by_conversation.setdefault(conversation_id, set()).add(turn_key)
        existing.calls.append(call)
        return closed

    def close_idle_turns(self, watermark_ms: int) -> list[ClosedTurn]:
        closed: list[ClosedTurn] = []
        cutoff = watermark_ms - self._idle_ms
        for turn_key, open_turn in list(self._open_by_turn.items()):
            if open_turn.time_end_ms <= cutoff:
                closed.append(self._close_turn(turn_key))
        return closed

    def close_all_open(self) -> list[ClosedTurn]:
        return [self._close_turn(key) for key in list(self._open_by_turn)]

    def _close_other_turns_in_conversation(
        self, conversation_id: str, new_turn_key: str
    ) -> list[ClosedTurn]:
        turn_keys = self._open_by_conversation.get(conversation_id, set())
        closed: list[ClosedTurn] = []
        for turn_key in list(turn_keys):
            if turn_key != new_turn_key and turn_key in self._open_by_turn:
                closed.append(self._close_turn(turn_key))
        return closed

    def _close_turn(self, turn_key: str) -> ClosedTurn:
        open_turn = self._open_by_turn.pop(turn_key)
        conv_keys = self._open_by_conversation.get(open_turn.conversation_id, set())
        conv_keys.discard(turn_key)
        return ClosedTurn(
            turn_key=open_turn.turn_key,
            conversation_id=open_turn.conversation_id,
            user_id=open_turn.user_id,
            calls=tuple(open_turn.calls),
            time_start_ms=open_turn.time_start_ms,
            time_end_ms=open_turn.time_end_ms,
        )

    def _conversation_id(self, call: Call) -> str:
        first_user = first_user_message_text(call.request_body) or ""
        session_key = f"{call.user_id}|{digest(first_user)}"
        session = self._store.get_conversation_session(session_key)
        if session is None:
            conversation_id = digest(call.user_id, first_user)
            self._store.upsert_conversation_session(
                session_key,
                conversation_id,
                call.user_id,
                call.response_timestamp_ms,
            )
            return conversation_id

        gap = call.request_timestamp_ms - session.last_activity_ms
        if gap > self._gap_ms:
            conversation_id = digest(session_key, call.request_timestamp_ms)
            self._store.upsert_conversation_session(
                session_key,
                conversation_id,
                call.user_id,
                call.response_timestamp_ms,
            )
            return conversation_id

        self._store.upsert_conversation_session(
            session_key,
            session.conversation_id,
            call.user_id,
            max(session.last_activity_ms, call.response_timestamp_ms),
        )
        return session.conversation_id

    def _request_dict(self, call: Call) -> dict[str, object] | None:
        from .openai_body import decode_body_string

        try:
            decoded = decode_body_string(call.request_body)
        except BodyFormatError:
            return None
        return decoded if isinstance(decoded, dict) else None
