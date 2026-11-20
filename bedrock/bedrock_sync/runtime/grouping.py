"""Group model calls into interactions with the triplet the caller sends.

Bedrock logs one record per model call and has no conversation, turn or user
id of its own. The caller puts three keys in ``requestMetadata``:

- ``interaction_id``: one user turn, with every call it needs (tools,
  sub-agents);
- ``conversation_id``: the conversation of the turn;
- ``user_id``: the person who asked.

Calls are grouped by conversation, user and ``interaction_id``: no content
hashing, no tool-id chaining. The same ``interaction_id`` in two
conversations is two turns, so a caller may number turns per conversation. A
call without a full triplet is skipped (``missing_triplet``).

**A conversation per user.** Nebuly keys a conversation by its id and its
end user, so the same ``conversation_id`` with two ``user_id`` values is two
conversations there. The grouper does the same: every user of a
``conversation_id`` has their own turns, closed by their own next turn. It is
legal but unusual, so each such ``conversation_id`` is logged once per run
and counted (``conversations_with_several_users``); nothing is dropped. The
same holds for one ``interaction_id`` with several users: it becomes one
interaction per user, logged and counted (``interactions_with_several_users``).

**Closing.** No log record says that a turn is over, so an interaction closes,
as in the OpenAI and Claude compliance integrations, on the first of:

1. an ``interaction_id`` started later in the same conversation (the next
   turn), once every file delivered up to its first call and up to the last
   call of this one, plus ``delivery_margin``, has been read: S3 delivers
   files out of order, and an agent may still work on a turn after the user
   sent the next message;
2. ``idle`` without calls (default 30 minutes), once ``delivery_margin``
   more has been read.

Time is the delivery time of the files read (the watermark), never the
wall clock, so a backfill behaves like live traffic. A call that starts
``idle`` or more after the end of its interaction is too late, and a call
for an interaction already closed cannot change what was queued: both are
dropped (``calls_for_closed_interaction``). With the margin on both closing
rules, what happens to a call depends on when it ran, not on when the
connector runs, as long as S3 delivers it within the margin.

``Grouper`` does no I/O. The sync saves the calls of open interactions as
pointers (S3 key and request id) and feeds them back with ``restore``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from bedrock_sync.shared.keys import digest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from datetime import datetime, timedelta

    from .models import Invocation

logger = logging.getLogger(__name__)

INTERACTION_ID_KEY = "interaction_id"
CONVERSATION_ID_KEY = "conversation_id"
USER_ID_KEY = "user_id"
TRIPLET_KEYS = (INTERACTION_ID_KEY, CONVERSATION_ID_KEY, USER_ID_KEY)
AGENT_ROLE_KEY = "agent_role"
SUBAGENT_ROLE = "subagent"
"""``agent_role=subagent`` marks a sub-agent call: never the final answer."""
RESERVED_METADATA_KEYS = frozenset({*TRIPLET_KEYS, AGENT_ROLE_KEY})


@dataclass(frozen=True)
class Triplet:
    interaction_id: str
    conversation_id: str
    user_id: str

    @property
    def turn_key(self) -> str:
        """Digest of the turn (conversation, user and interaction id): the key
        of an interaction in the state and the outbox, stored instead of raw
        ids."""
        return digest(
            "bedrock.runtime.turn",
            2,
            self.conversation_id,
            self.user_id,
            self.interaction_id,
        )

    @property
    def conversation_key(self) -> str:
        """Digest of the conversation as Nebuly sees it: id and user."""
        return digest(
            "bedrock.runtime.conversation", 2, self.conversation_id, self.user_id
        )


def triplet_of(invocation: Invocation) -> Triplet | None:
    values = [invocation.request_metadata.get(key, "").strip() for key in TRIPLET_KEYS]
    if not all(values):
        return None
    return Triplet(*values)


def is_subagent(invocation: Invocation) -> bool:
    return invocation.request_metadata.get(AGENT_ROLE_KEY, "").strip() == SUBAGENT_ROLE


def _order(invocation: Invocation) -> tuple[datetime, int, str]:
    # Logs have whole seconds: at a tie the shorter history came first.
    return (invocation.timestamp, len(invocation.messages), invocation.request_id)


def _is_second(seen: dict[str, set[str]], key: str, user: str) -> bool:
    """Record ``user`` under ``key``; True when it is the second one."""
    users = seen.setdefault(key, set())
    if user in users:
        return False
    users.add(user)
    return len(users) == 2


class CloseReason(StrEnum):
    NEXT_INTERACTION = "next_interaction"
    IDLE = "idle"


class AddResult(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    MISSING_TRIPLET = "missing_triplet"
    CLOSED = "closed_interaction"


@dataclass
class Interaction:
    triplet: Triplet
    invocations: list[Invocation] = field(default_factory=list)
    close_reason: CloseReason | None = None

    def add(self, invocation: Invocation) -> None:
        self.invocations.append(invocation)
        self.invocations.sort(key=_order)

    @property
    def first(self) -> Invocation:
        return self.invocations[0]

    @property
    def main_calls(self) -> list[Invocation]:
        """Calls not marked as sub-agent, in order."""
        return [i for i in self.invocations if not is_subagent(i)]

    @property
    def final(self) -> Invocation | None:
        """The call holding the answer: the last one not marked as sub-agent."""
        main = self.main_calls
        return main[-1] if main else None

    @property
    def time_start(self) -> datetime:
        return self.first.timestamp

    @property
    def time_end(self) -> datetime:
        return max(i.time_end for i in self.invocations)


@dataclass
class GroupingStats:
    accepted: int = 0
    duplicates: int = 0
    missing_triplet: int = 0
    conversations_with_several_users: int = 0
    interactions_with_several_users: int = 0
    calls_for_closed_interaction: int = 0
    closed_next_interaction: int = 0
    closed_idle: int = 0


@dataclass(frozen=True)
class CallRef:
    """Where to read a call of an open interaction again."""

    request_id: str
    source_key: str
    turn_key: str


class Grouper:
    def __init__(
        self,
        *,
        idle: timedelta,
        delivery_margin: timedelta,
        is_closed: Callable[[str], bool] = lambda _: False,
    ) -> None:
        """``is_closed`` says if a turn (by its key) was closed in an earlier
        run."""
        self._idle = idle
        self._margin = delivery_margin
        self._is_closed = is_closed
        # Users seen in this run by conversation id and by turn, to warn once
        # when there are several.
        self._users: dict[str, set[str]] = {}
        self._turn_users: dict[str, set[str]] = {}
        self._open: dict[str, Interaction] = {}
        self._closed_here: set[str] = set()
        self._ready: list[Interaction] = []
        self._request_ids: set[str] = set()
        # Calls put back by ``restore``: counted in an earlier run.
        self._restored: set[str] = set()
        self.stats = GroupingStats()

    # Feeding -------------------------------------------------------------------

    def add(self, invocation: Invocation) -> AddResult:
        result = self._add(invocation)
        if result is AddResult.ACCEPTED:
            self.stats.accepted += 1
        elif result is AddResult.DUPLICATE:
            self.stats.duplicates += 1
        elif result is AddResult.MISSING_TRIPLET:
            self.stats.missing_triplet += 1
        else:
            self._drop_late(invocation)
        return result

    def _add(self, invocation: Invocation) -> AddResult:
        triplet = triplet_of(invocation)
        if triplet is None:
            return AddResult.MISSING_TRIPLET
        self._note_user(triplet, invocation)
        key = triplet.turn_key
        if key in self._closed_here or self._is_closed(key):
            return AddResult.CLOSED
        if invocation.request_id in self._request_ids:
            return AddResult.DUPLICATE
        current = self._open.get(key)
        self._request_ids.add(invocation.request_id)
        if current is None:
            current = self._open[key] = Interaction(triplet)
        current.add(invocation)
        return AddResult.ACCEPTED

    def _drop_late(self, invocation: Invocation) -> None:
        self.stats.calls_for_closed_interaction += 1
        logger.debug(
            "Dropping request %s: its interaction is closed or idle",
            invocation.request_id,
        )

    def _note_user(self, triplet: Triplet, invocation: Invocation) -> None:
        """Warn once per run when a conversation id, or an interaction id in
        it, has several users: legal, but usually a bug in the caller."""
        user = digest("bedrock.runtime.user", 1, triplet.user_id)
        conversation = digest(
            "bedrock.runtime.conversation_id", 1, triplet.conversation_id
        )
        if _is_second(self._users, conversation, user):
            self.stats.conversations_with_several_users += 1
            logger.warning(
                "A conversation_id has calls of several user_id values (%s): "
                "Nebuly shows one conversation per user",
                invocation.source_key,
            )
        turn = digest(
            "bedrock.runtime.turn_id",
            1,
            triplet.conversation_id,
            triplet.interaction_id,
        )
        if _is_second(self._turn_users, turn, user):
            self.stats.interactions_with_several_users += 1
            logger.warning(
                "An interaction_id has calls of several user_id values (%s): "
                "it is sent as one interaction per user",
                invocation.source_key,
            )

    def restore(self, invocations: Iterable[Invocation]) -> None:
        """Put back the calls of the interactions left open at the last
        checkpoint, read again from S3. They are not counted again."""
        for invocation in invocations:
            self._restored.add(invocation.request_id)
            self._add(invocation)

    # Closing -------------------------------------------------------------------

    def advance(self, watermark: datetime) -> None:
        """Close what the files read up to ``watermark`` allow to close."""
        starts: dict[str, list[datetime]] = {}
        for interaction in self._open.values():
            starts.setdefault(interaction.triplet.conversation_key, []).append(
                interaction.time_start
            )
        for key, interaction in list(self._open.items()):
            if self._awaits_earlier_calls(interaction, watermark):
                continue
            reason = self._close_reason(interaction, starts, watermark)
            if reason is None:
                continue
            interaction.close_reason = reason
            if reason is CloseReason.IDLE:
                self.stats.closed_idle += 1
            else:
                self.stats.closed_next_interaction += 1
            del self._open[key]
            self._closed_here.add(key)
            self._ready.append(interaction)

    def _awaits_earlier_calls(
        self, interaction: Interaction, watermark: datetime
    ) -> bool:
        """Resolve the first idle gap only after all its timely calls can arrive."""
        end = interaction.first.time_end
        for index, call in enumerate(interaction.invocations[1:], start=1):
            if call.timestamp - end >= self._idle:
                if watermark < end + self._idle + self._margin:
                    return True
                for late in interaction.invocations[index:]:
                    self._drop_late(late)
                    if late.request_id not in self._restored:
                        self.stats.accepted -= 1
                del interaction.invocations[index:]
                break
            end = max(end, call.time_end)
        return False

    def _close_reason(
        self,
        interaction: Interaction,
        starts: dict[str, list[datetime]],
        watermark: datetime,
    ) -> CloseReason | None:
        later = [
            start
            for start in starts[interaction.triplet.conversation_key]
            if start > interaction.time_start
        ]
        # The next turn closes this one only once this one is quiet too: an
        # agent may still work on it after the user sent another message.
        if (
            later
            and watermark >= min(later) + self._margin
            and watermark >= interaction.time_end + self._margin
        ):
            return CloseReason.NEXT_INTERACTION
        if watermark - interaction.time_end >= self._idle + self._margin:
            return CloseReason.IDLE
        return None

    def drain_closed(self) -> list[Interaction]:
        """The interactions closed since the last call, oldest first."""
        ready = sorted(
            self._ready, key=lambda i: (i.time_start, i.triplet.interaction_id)
        )
        self._ready = []
        return ready

    # Checkpoint ----------------------------------------------------------------

    def open_refs(self) -> list[CallRef]:
        """Pointers to every call of the interactions still open."""
        return [
            CallRef(invocation.request_id, invocation.source_key, turn_key)
            for turn_key, interaction in self._open.items()
            for invocation in interaction.invocations
        ]

    @property
    def open_count(self) -> int:
        return len(self._open)
