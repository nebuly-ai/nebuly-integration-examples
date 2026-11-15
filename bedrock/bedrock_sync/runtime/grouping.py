"""Group model calls into interactions with the triplet the caller sends.

Bedrock logs one record per model call and has no conversation, turn or user
id of its own. The caller puts three keys in ``requestMetadata``:

- ``interaction_id``: one user turn, with every call it needs (tools,
  sub-agents);
- ``conversation_id``: the conversation of the turn;
- ``user_id``: the person who asked.

Calls are grouped by ``interaction_id`` only: no content hashing, no tool-id
chaining. A call without a full triplet is skipped (``missing_triplet``). An
``interaction_id`` already used in another conversation is a caller
error: the conflicting call is dropped (``interaction_id_conflicts``), users
are never merged.

**One user per conversation.** This rule comes first. A conversation belongs
to the user of its oldest call; a call of that conversation with another
``user_id`` is dropped (``conversation_user_conflicts``). While none of its
interactions is closed, an older call of another user can still take the
conversation over: the open interactions of the previous user are dropped
and counted the same way (S3 can deliver the wrong call first). Once an
interaction of the conversation closes the owner is final, also for later
runs.

**Closing.** No log record says that a turn is over, so an interaction closes,
as in the OpenAI and Claude compliance integrations, on the first of:

1. an ``interaction_id`` started later in the same conversation (the next
   turn, even if a call of this one still runs after it started), once
   every file delivered up to its first call plus ``delivery_margin`` has
   been read: S3 delivers files out of order, and the last call of the
   previous turn can land after the first call of the next one;
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
from enum import Enum, StrEnum
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
    def owner(self) -> str:
        """Digest of who owns the interaction id, stored instead of raw ids."""
        return digest("bedrock.runtime.owner", 1, self.conversation_id, self.user_id)

    @property
    def conversation_key(self) -> str:
        """Digest of the conversation id, stored instead of the raw id."""
        return digest("bedrock.runtime.conversation", 1, self.conversation_id)


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


class CloseReason(StrEnum):
    NEXT_INTERACTION = "next_interaction"
    IDLE = "idle"


class _Ownership(Enum):
    """What a call does to the owner of its conversation."""

    KEEP = "keep"
    TAKE_OVER = "take_over"
    REJECT = "reject"


class AddResult(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    MISSING_TRIPLET = "missing_triplet"
    CONFLICT = "interaction_id_conflict"
    CONVERSATION_CONFLICT = "conversation_user_conflict"
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
    interaction_id_conflicts: int = 0
    conversation_user_conflicts: int = 0
    calls_for_closed_interaction: int = 0
    closed_next_interaction: int = 0
    closed_idle: int = 0


@dataclass(frozen=True)
class CallRef:
    """Where to read a call of an open interaction again."""

    request_id: str
    source_key: str


class Grouper:
    def __init__(
        self,
        *,
        idle: timedelta,
        delivery_margin: timedelta,
        closed_owner: Callable[[str], str | None] = lambda _: None,
        conversation_owner: Callable[[str], str | None] = lambda _: None,
    ) -> None:
        """``closed_owner`` returns the owner digest of an interaction closed in
        an earlier run, ``conversation_owner`` the owner digest of a
        conversation seen in an earlier run (by its key), or None."""
        self._idle = idle
        self._margin = delivery_margin
        self._closed_owner = closed_owner
        self._conversation_owner = conversation_owner
        # Final owners decided in this run (an interaction closed), saved.
        self._fixed: dict[str, str] = {}
        # Provisional owners: owner digest and oldest call, open interactions only.
        self._claims: dict[str, tuple[str, datetime]] = {}
        self._open: dict[str, Interaction] = {}
        self._closed_here: dict[str, str] = {}
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
        elif result is AddResult.CONVERSATION_CONFLICT:
            self.stats.conversation_user_conflicts += 1
            logger.warning(
                "Dropping request %s: its conversation_id belongs to another "
                "user_id (%s)",
                invocation.request_id,
                invocation.source_key,
            )
        elif result is AddResult.CONFLICT:
            self.stats.interaction_id_conflicts += 1
            logger.warning(
                "Dropping request %s: interaction_id already used by another "
                "conversation (%s)",
                invocation.request_id,
                invocation.source_key,
            )
        else:
            self.stats.calls_for_closed_interaction += 1
            logger.debug(
                "Dropping request %s: its interaction is closed or idle",
                invocation.request_id,
            )
        return result

    def _add(self, invocation: Invocation) -> AddResult:
        triplet = triplet_of(invocation)
        if triplet is None:
            return AddResult.MISSING_TRIPLET
        conversation = triplet.conversation_key
        fixed = self._fixed.get(conversation) or self._conversation_owner(conversation)
        ownership = self._ownership(conversation, fixed, triplet, invocation)
        if ownership is _Ownership.REJECT:
            return AddResult.CONVERSATION_CONFLICT
        key = triplet.interaction_id
        closed = self._closed_here.get(key) or self._closed_owner(key)
        if closed is not None:
            return AddResult.CLOSED if closed == triplet.owner else AddResult.CONFLICT
        if invocation.request_id in self._request_ids:
            return AddResult.DUPLICATE
        current = self._open.get(key)
        if (
            ownership is _Ownership.TAKE_OVER
            and current is not None
            and current.triplet.conversation_key == conversation
        ):
            current = None  # dropped below with the rest of the conversation
        if current is not None and (rejected := self._check_open(current, invocation)):
            return rejected
        # Every check passed: only now change the state.
        if ownership is _Ownership.TAKE_OVER:
            self._drop_conversation(conversation)
        self._request_ids.add(invocation.request_id)
        if fixed is None:
            self._claim(conversation, triplet.owner, invocation.timestamp)
        if current is None:
            current = self._open[key] = Interaction(triplet)
        current.add(invocation)
        return AddResult.ACCEPTED

    def _check_open(
        self, current: Interaction, invocation: Invocation
    ) -> AddResult | None:
        """Why a call cannot join the open interaction with its id, if it cannot."""
        if current.triplet != triplet_of(invocation):
            return AddResult.CONFLICT
        if invocation.timestamp - current.time_end >= self._idle:
            return AddResult.CLOSED  # too late: the interaction was idle
        return None

    def _ownership(
        self,
        conversation: str,
        fixed: str | None,
        triplet: Triplet,
        invocation: Invocation,
    ) -> _Ownership:
        if fixed is not None:
            return _Ownership.KEEP if fixed == triplet.owner else _Ownership.REJECT
        claim = self._claims.get(conversation)
        if claim is None or claim[0] == triplet.owner:
            return _Ownership.KEEP
        # At a tie (logs have whole seconds) the user read first keeps it.
        if invocation.timestamp >= claim[1]:
            return _Ownership.REJECT
        return _Ownership.TAKE_OVER

    def _claim(self, conversation: str, owner: str, timestamp: datetime) -> None:
        """Provisional owner of a conversation and its oldest call."""
        claim = self._claims.get(conversation)
        oldest = timestamp if claim is None else min(claim[1], timestamp)
        self._claims[conversation] = (owner, oldest)

    def _drop_conversation(self, conversation: str) -> None:
        """An older call of another user took the conversation: drop the open
        interactions of the previous user."""
        for key, interaction in list(self._open.items()):
            if interaction.triplet.conversation_key != conversation:
                continue
            del self._open[key]
            calls = len(interaction.invocations)
            counted_here = sum(
                i.request_id not in self._restored for i in interaction.invocations
            )
            self.stats.accepted -= counted_here
            self.stats.conversation_user_conflicts += calls
            logger.warning(
                "Dropping %s call(s) of an open interaction: an older call of "
                "another user_id owns its conversation_id (%s)",
                calls,
                interaction.first.source_key,
            )
        self._claims.pop(conversation, None)

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
            reason = self._close_reason(interaction, starts, watermark)
            if reason is None:
                continue
            interaction.close_reason = reason
            if reason is CloseReason.IDLE:
                self.stats.closed_idle += 1
            else:
                self.stats.closed_next_interaction += 1
            del self._open[key]
            self._closed_here[key] = interaction.triplet.owner
            conversation = interaction.triplet.conversation_key
            self._fixed[conversation] = interaction.triplet.owner
            self._claims.pop(conversation, None)
            self._ready.append(interaction)

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
        if later and watermark >= min(later) + self._margin:
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
            CallRef(invocation.request_id, invocation.source_key)
            for interaction in self._open.values()
            for invocation in interaction.invocations
        ]

    def conversation_owners(self) -> dict[str, str]:
        """Final owners decided in this run, by conversation key, to be saved
        with the closed interactions. Open ones are rebuilt by ``restore``."""
        return dict(self._fixed)

    @property
    def open_count(self) -> int:
        return len(self._open)
