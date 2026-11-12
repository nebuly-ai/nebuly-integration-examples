"""Rebuild conversations and user turns from single invocations.

Bedrock logs one record per model call, with no conversation id. Two things
are rebuilt here:

1. **Conversation id**, in order of priority:
   - a session id the caller put in ``requestMetadata`` (Converse);
   - the ``session_id`` Claude Code sends in ``metadata.user_id``;
   - a hash chain: the next call of a conversation re-sends the previous
     messages plus the previous answer, so each call registers the digest of
     ``messages + [answer]`` and a later call whose history starts with that
     digest joins the same conversation. Including the answer is what keeps
     apart two conversations that opened with the same question.
   - otherwise a new conversation named after the request id.

2. **Turns**: a user turn is the first call plus the tool-use round trips that
   follow it (``stop_reason == tool_use``, then a call whose last message
   carries the matching ``tool_result`` ids). The tool ids link the round
   trips, so interleaved calls of the same session (sub-agents, title
   generation) do not mix.

3. **Arrival order**: S3 files are not ordered by call time. On real logs a
   tool continuation landed in a file delivered 7 s before the file holding
   the call that asked for the tool. So ``add`` only buffers; ``advance``
   releases, in timestamp order, the calls older than the delivery watermark
   minus ``reorder_window``. A tool continuation delivered later than that
   starts a turn of its own (``partial_start``); the converter recovers the
   question and the tool calls from its history. Every tool id that history
   answers is remembered, so the calls that asked for them, if they show up
   afterwards, are dropped (``late_tool_calls``) instead of waiting for
   results that already came. If the turn of that question is still open,
   the call joins it instead (``gap_continuations``): only the calls in
   between are missing. Calls with the same timestamp (logs have whole
   seconds) are released shortest history first, so a quick tool round trip
   keeps its order.

Tool ids are matched within the same agent loop: the caller (identity ARN)
and the history up to the question that started the loop. Some models number
their tool calls (``functions.search:0``) instead of giving them a unique id,
so the id alone would mix two conversations of the same application role.

A turn left waiting for a tool result longer than ``open_turn_timeout`` (the
client crashed, the user never approved the tool) is released as is, with
``timed_out`` set, like the idle cut of the Claude Compliance integration.
The default is long because a person can leave a tool approval pending for a
while: a turn resumed after the cut becomes a second interaction with the
same question.

A request id seen again within ``dedup_window`` is dropped (``duplicates``), like
the ``insert_id`` dedup of the Gemini integration. Two live conversations of
the same caller with an identical history and answer cannot be told apart by
content; a follow-up joins the later one and ``ambiguous_links`` counts it.

``Grouper`` keeps its state between calls, so the sync can feed it across S3
objects. ``snapshot`` and ``restore`` carry that state across runs as plain
data: hashes, ids, times and, for the calls not sent yet, only where to read
them again (``CallRef``), never their content.
"""

from __future__ import annotations

import hashlib
import heapq
import itertools
import logging
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING

from .formats.base import canonical_json
from .models import Block, BlockKind, Invocation, Message, StopReason, tool_loop

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

logger = logging.getLogger(__name__)

_TIPS_PER_CONVERSATION = 8

type _ToolKey = tuple[str, str]
"""``(loop_key, tool id)``."""

SESSION_METADATA_KEYS = ("session_id", "sessionId", "conversation_id", "conversationId")
_HASHED_KINDS = frozenset(
    {
        BlockKind.TEXT,
        BlockKind.TOOL_USE,
        BlockKind.TOOL_RESULT,
        BlockKind.IMAGE,
        BlockKind.DOCUMENT,
    }
)


class ConversationSource(StrEnum):
    REQUEST_METADATA = "request_metadata"
    CLAUDE_CODE = "claude_code"
    HASH_CHAIN = "hash_chain"
    NEW = "new"


@dataclass
class Turn:
    conversation_id: str
    conversation_source: ConversationSource
    invocations: list[Invocation]
    partial_start: bool = False
    """The first call answers a tool call that is not in the turn: it fell
    before the synced window, or was delivered too late. The question and the
    earlier tool calls are only in the first call's history."""
    timed_out: bool = False
    """Released while still waiting for a tool result."""

    @property
    def first(self) -> Invocation:
        return self.invocations[0]

    @property
    def last(self) -> Invocation:
        return self.invocations[-1]

    @property
    def time_start(self) -> datetime:
        return self.first.timestamp

    @property
    def time_end(self) -> datetime:
        """End of the latest call. A call without a logged latency (plain
        InvokeModel) counts as ending when it started."""
        return max(i.time_end for i in self.invocations)

    @property
    def pending_tool_ids(self) -> frozenset[str]:
        if self.last.stop_reason is not StopReason.TOOL_USE:
            return frozenset()
        return frozenset(b.tool_use_id for b in self.last.tool_uses if b.tool_use_id)

    @property
    def complete(self) -> bool:
        return not self.pending_tool_ids


@dataclass
class GroupingStats:
    invocations: int = 0
    duplicates: int = 0
    ambiguous_links: int = 0
    tool_continuations: int = 0
    partial_starts: int = 0
    timed_out_turns: int = 0
    late_tool_calls: int = 0
    gap_continuations: int = 0
    turns_by_source: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True)
class _Tip:
    conversation_id: str
    seen_at: datetime
    ambiguous: bool = False


@dataclass(frozen=True)
class CallRef:
    """Where to read a call again: the log file and the request id."""

    request_id: str
    source_key: str
    timestamp: datetime

    @classmethod
    def of(cls, invocation: Invocation) -> CallRef:
        return cls(invocation.request_id, invocation.source_key, invocation.timestamp)


@dataclass(frozen=True)
class OpenTurnState:
    conversation_id: str
    conversation_source: ConversationSource
    partial_start: bool
    calls: tuple[CallRef, ...]


@dataclass(frozen=True)
class TipState:
    digest: str
    conversation_id: str
    seen_at: datetime
    ambiguous: bool
    """Another live conversation registered the same digest before."""


@dataclass
class GrouperSnapshot:
    """Everything a ``Grouper`` needs to go on in the next run."""

    latest: datetime | None = None
    buffered: list[CallRef] = field(default_factory=list)
    open_turns: list[OpenTurnState] = field(default_factory=list)
    tips: list[TipState] = field(default_factory=list)
    seen: dict[str, datetime] = field(default_factory=dict)
    answered: dict[tuple[str, str], datetime] = field(default_factory=dict)
    """``(loop_key, tool id)`` -> time of the call that answered it."""

    def call_refs(self) -> list[CallRef]:
        """Every call to read again, buffered first."""
        return [*self.buffered, *(c for t in self.open_turns for c in t.calls)]


def _block_key(block: Block) -> tuple[object, ...]:
    if block.kind is BlockKind.TOOL_USE:
        return (
            "tool_use",
            block.tool_use_id,
            block.tool_name,
            canonical_json(block.tool_input),
        )
    if block.kind is BlockKind.TOOL_RESULT:
        return ("tool_result", block.tool_use_id, block.text.strip(), block.is_error)
    return (block.kind.value, block.text.strip())


def _message_key(message: Message) -> str:
    """Stable form of a message: reasoning blocks and cache markers are dropped
    and whitespace trimmed, since clients often re-send the answer that way."""
    blocks = [
        _block_key(b)
        for b in message.blocks
        if b.kind in _HASHED_KINDS and (b.text.strip() or b.kind is not BlockKind.TEXT)
    ]
    return canonical_json([message.role, blocks])


def prefix_digests(seed: str, messages: tuple[Message, ...]) -> list[str]:
    """Digest of ``messages[: i + 1]`` for every i, seeded with the caller."""
    hasher = hashlib.sha256(seed.encode())
    digests: list[str] = []
    for message in messages:
        hasher.update(_message_key(message).encode())
        hasher.update(b"\x1e")
        digests.append(hasher.copy().hexdigest())
    return digests


def loop_key(seed: str, messages: tuple[Message, ...]) -> str:
    """Digest of the history up to the user message that started the current
    agent loop. Every call of the loop re-sends that history unchanged, so it
    names the loop; seeded with the caller like the hash chain."""
    head = len(messages) - len(tool_loop(messages))
    if not head:
        return hashlib.sha256(seed.encode()).hexdigest()
    return prefix_digests(seed, messages[:head])[-1]


def session_id_from_metadata(metadata: dict[str, str]) -> str | None:
    for key in SESSION_METADATA_KEYS:
        value = metadata.get(key, "").strip()
        if value:
            return value
    return None


class Grouper:
    def __init__(
        self,
        *,
        chain_ttl: timedelta = timedelta(days=30),
        reorder_window: timedelta = timedelta(minutes=15),
        open_turn_timeout: timedelta = timedelta(hours=2),
        dedup_window: timedelta = timedelta(days=1),
    ) -> None:
        self._chain_ttl = chain_ttl
        self._dedup_window = dedup_window
        self._reorder_window = reorder_window
        self._open_turn_timeout = open_turn_timeout
        self._buffer: list[tuple[datetime, int, str, int, Invocation]] = []
        self._sequence = itertools.count()
        self._tips: dict[str, _Tip] = {}
        self._seen: dict[str, datetime] = {}
        self._answered: dict[_ToolKey, datetime] = {}
        """Tool ids answered by a ``partial_start`` turn, to recognise the calls
        that asked for them if they are released later."""
        self._conversation_tips: dict[str, deque[tuple[str, ...]]] = {}
        self._open: dict[_ToolKey, list[Turn]] = {}
        """Open turns by the tool id they wait for, oldest first. Several turns
        can wait for the same key when a model numbers its tool calls and two
        users of the same application ask the same question."""
        self._ready: list[Turn] = []
        self._latest: datetime | None = None
        self.stats = GroupingStats()

    def add(self, invocation: Invocation) -> None:
        """Buffer a call, in any order. Nothing is grouped until released."""
        if invocation.request_id:
            if invocation.request_id in self._seen:
                self.stats.duplicates += 1
                logger.debug("Dropping duplicate request %s", invocation.request_id)
                return
            self._seen[invocation.request_id] = invocation.timestamp
        self._push(invocation)

    def _push(self, invocation: Invocation) -> None:
        heapq.heappush(
            self._buffer,
            (
                invocation.timestamp,
                # A continuation re-sends the call before it, so its history
                # is longer: it goes second when both have the same second.
                len(invocation.messages),
                invocation.request_id,
                next(self._sequence),
                invocation,
            ),
        )

    def advance(self, watermark: datetime) -> None:
        """Every log delivered up to ``watermark`` has been added.

        Releases the calls that started before ``watermark - reorder_window``
        and times out the turns idle since ``open_turn_timeout`` before that.
        """
        horizon = watermark - self._reorder_window
        while self._buffer and self._buffer[0][0] <= horizon:
            self._group(heapq.heappop(self._buffer)[-1])
        self._time_out_open_turns(horizon - self._open_turn_timeout)

    def flush(self) -> None:
        """Release every buffered call: no more logs will come."""
        while self._buffer:
            self._group(heapq.heappop(self._buffer)[-1])

    def buffered(self) -> list[Invocation]:
        """Calls added but not released yet, oldest first."""
        return [entry[-1] for entry in sorted(self._buffer)]

    def drain_complete(self) -> list[Turn]:
        """Turns whose final answer has arrived, in completion order."""
        ready, self._ready = self._ready, []
        self._prune_tips()
        return ready

    def open_turns(self) -> list[Turn]:
        """Turns still waiting for a tool result, oldest first."""
        unique = {id(t): t for waiting in self._open.values() for t in waiting}
        return sorted(unique.values(), key=lambda t: t.first.timestamp)

    def snapshot(self) -> GrouperSnapshot:
        """The state to save at a checkpoint. Call it after
        ``drain_complete``: a turn ready but not taken would be lost."""
        if self._ready:
            raise RuntimeError("snapshot() with turns not drained")
        return GrouperSnapshot(
            latest=self._latest,
            buffered=[CallRef.of(i) for i in self.buffered()],
            open_turns=[
                OpenTurnState(
                    conversation_id=turn.conversation_id,
                    conversation_source=turn.conversation_source,
                    partial_start=turn.partial_start,
                    calls=tuple(CallRef.of(i) for i in turn.invocations),
                )
                for turn in self.open_turns()
            ],
            tips=[
                TipState(digest, tip.conversation_id, tip.seen_at, tip.ambiguous)
                for digest, tip in sorted(
                    self._tips.items(), key=lambda kv: (kv[1].seen_at, kv[0])
                )
            ],
            seen=dict(self._seen),
            answered=dict(self._answered),
        )

    def restore(
        self, snapshot: GrouperSnapshot, calls: Mapping[str, Invocation]
    ) -> None:
        """Go on from ``snapshot`` on a new ``Grouper``. ``calls`` maps the
        request ids of ``snapshot.call_refs()`` to the calls read again; a
        missing one is logged and lost."""
        self._latest = snapshot.latest
        self._seen = dict(snapshot.seen)
        self._answered = dict(snapshot.answered)

        self._tips = {}
        self._conversation_tips = {}
        # Tips of the same call share their time: group them back into the
        # per-conversation history that bounds them, oldest first.
        groups: dict[tuple[str, datetime], list[str]] = {}
        for tip in snapshot.tips:
            self._tips[tip.digest] = _Tip(
                tip.conversation_id, tip.seen_at, ambiguous=tip.ambiguous
            )
            groups.setdefault((tip.conversation_id, tip.seen_at), []).append(tip.digest)
        for (conversation_id, _), digests in groups.items():
            self._remember_tips(conversation_id, tuple(digests))

        for ref in snapshot.buffered:
            if (invocation := self._reread(ref, calls)) is not None:
                self._push(invocation)

        for saved in snapshot.open_turns:
            invocations = [
                i for ref in saved.calls if (i := self._reread(ref, calls)) is not None
            ]
            if not invocations:
                continue
            turn = Turn(
                conversation_id=saved.conversation_id,
                conversation_source=saved.conversation_source,
                invocations=invocations,
                partial_start=saved.partial_start
                or invocations[0].request_id != saved.calls[0].request_id,
            )
            # Every call of an open turn asked for a tool, so it still waits
            # even if its last call is gone; it times out like any other.
            self._wait_for_tools(turn)

    @staticmethod
    def _reread(ref: CallRef, calls: Mapping[str, Invocation]) -> Invocation | None:
        invocation = calls.get(ref.request_id)
        if invocation is None:
            logger.warning(
                "Request %s not found again in %s: dropped from the state",
                ref.request_id,
                ref.source_key,
            )
        return invocation

    def _group(self, invocation: Invocation) -> None:
        self.stats.invocations += 1
        if self._latest is None or invocation.timestamp > self._latest:
            self._latest = invocation.timestamp

        last = invocation.last_message
        result_ids = (
            last.tool_result_ids if last and last.role == "user" else frozenset()
        )

        caller = invocation.identity_arn
        loop = loop_key(caller, invocation.messages)
        turn = self._open_turn(loop, result_ids, invocation)
        if turn is None and result_ids:
            # The calls between an open turn and this one came late: join it.
            turn = self._open_turn(loop, self._loop_result_ids(invocation), invocation)
            if turn is not None:
                self.stats.gap_continuations += 1
                self._remember_answered(invocation, loop)
        if turn is None and self._is_late_tool_call(invocation, loop):
            return
        if turn is not None:
            self._close_pending(turn)
            turn.invocations.append(invocation)
            self.stats.tool_continuations += 1
        else:
            conversation_id, source = self._assign_conversation(invocation)
            turn = Turn(
                conversation_id=conversation_id,
                conversation_source=source,
                invocations=[invocation],
                partial_start=bool(result_ids),
            )
            self.stats.turns_by_source[source.value] += 1
            if turn.partial_start:
                self.stats.partial_starts += 1
                self._remember_answered(invocation, loop)

        self._register_tips(invocation, turn.conversation_id)
        if turn.complete:
            self._ready.append(turn)
        else:
            self._wait_for_tools(turn)

    def _wait_for_tools(self, turn: Turn) -> None:
        loop = loop_key(turn.last.identity_arn, turn.last.messages)
        for tool_id in turn.pending_tool_ids:
            self._open.setdefault((loop, tool_id), []).append(turn)

    def _open_turn(
        self, loop: str, tool_ids: Iterable[str], invocation: Invocation
    ) -> Turn | None:
        """The open turn that ``invocation`` continues. When several wait for
        the same tool id, the one whose tool request ``invocation`` re-sends in
        its history; if they all asked the same thing, the oldest."""
        candidates = [t for i in tool_ids for t in self._open.get((loop, i), ())]
        if len(candidates) <= 1:
            return next(iter(candidates), None)
        asked = {
            _message_key(m)
            for m in tool_loop(invocation.messages)
            if m.role == "assistant"
        }
        return next(
            (
                t
                for t in candidates
                if _message_key(Message("assistant", t.last.output)) in asked
            ),
            candidates[0],
        )

    @staticmethod
    def _loop_result_ids(invocation: Invocation) -> set[str]:
        return {i for m in tool_loop(invocation.messages) for i in m.tool_result_ids}

    def _remember_answered(self, invocation: Invocation, loop: str) -> None:
        for tool_id in self._loop_result_ids(invocation):
            self._answered[loop, tool_id] = invocation.timestamp

    def _is_late_tool_call(self, invocation: Invocation, loop: str) -> bool:
        """A call whose tool requests were all answered by a turn released
        before it: that turn already holds the question and the answer."""
        if invocation.stop_reason is not StopReason.TOOL_USE:
            return False
        tool_ids = {
            (loop, b.tool_use_id) for b in invocation.tool_uses if b.tool_use_id
        }
        if not tool_ids or not tool_ids <= self._answered.keys():
            return False
        self.stats.late_tool_calls += 1
        logger.debug(
            "Dropping request %s: its tool results came in an earlier turn",
            invocation.request_id,
        )
        return True

    def _close_pending(self, turn: Turn) -> None:
        loop = loop_key(turn.last.identity_arn, turn.last.messages)
        for tool_id in turn.pending_tool_ids:
            key = (loop, tool_id)
            waiting = [t for t in self._open.get(key, ()) if t is not turn]
            if waiting:
                self._open[key] = waiting
            else:
                self._open.pop(key, None)

    def _time_out_open_turns(self, cutoff: datetime) -> None:
        for turn in self.open_turns():
            if turn.last.time_end <= cutoff:
                self._close_pending(turn)
                turn.timed_out = True
                self.stats.timed_out_turns += 1
                self._ready.append(turn)

    def _assign_conversation(
        self, invocation: Invocation
    ) -> tuple[str, ConversationSource]:
        session_id = session_id_from_metadata(invocation.request_metadata)
        if session_id:
            return session_id, ConversationSource.REQUEST_METADATA
        if invocation.claude_code and invocation.claude_code.session_id:
            return invocation.claude_code.session_id, ConversationSource.CLAUDE_CODE

        digests = prefix_digests(invocation.identity_arn, invocation.messages)
        # Longest known prefix first: the full input matches a regenerated
        # call, shorter prefixes match the answer of an earlier turn.
        for digest in reversed(digests):
            tip = self._tips.get(digest)
            if tip and invocation.timestamp - tip.seen_at <= self._chain_ttl:
                if tip.ambiguous:
                    self.stats.ambiguous_links += 1
                    logger.debug(
                        "Request %s matches several conversations with the same "
                        "history; joined the latest, %s",
                        invocation.request_id,
                        tip.conversation_id,
                    )
                return tip.conversation_id, ConversationSource.HASH_CHAIN
        return f"bedrock-{invocation.request_id}", ConversationSource.NEW

    def _register_tips(self, invocation: Invocation, conversation_id: str) -> None:
        answer = Message("assistant", invocation.output)
        digests = prefix_digests(
            invocation.identity_arn, (*invocation.messages, answer)
        )
        previous = self._tips.get(digests[-1])
        ambiguous = bool(
            previous
            and previous.conversation_id != conversation_id
            and invocation.timestamp - previous.seen_at <= self._chain_ttl
        )
        tip = _Tip(conversation_id, invocation.timestamp, ambiguous=ambiguous)
        keys = [digests[-1]]
        # The bare input is only a safe key once it holds an earlier answer:
        # two users asking the same first question must not be merged.
        if any(m.role == "assistant" for m in invocation.messages):
            keys.append(digests[-2])
        for key in keys:
            self._tips[key] = tip
        self._remember_tips(conversation_id, tuple(keys))

    def _remember_tips(self, conversation_id: str, keys: tuple[str, ...]) -> None:
        """Keep the tips of the last ``_TIPS_PER_CONVERSATION`` calls only.

        A follow-up re-sends the latest answer, so older tips only serve a
        branch from an older message. Bounding them per conversation keeps
        the state proportional to the conversations, not to the calls, which
        is what lets ``chain_ttl`` be weeks long.
        """
        recent = self._conversation_tips.setdefault(conversation_id, deque())
        recent.append(keys)
        while len(recent) > _TIPS_PER_CONVERSATION:
            for key in recent.popleft():
                tip = self._tips.get(key)
                if tip and tip.conversation_id == conversation_id:
                    del self._tips[key]

    def _prune_tips(self) -> None:
        if self._latest is None:
            return
        cutoff = self._latest - self._chain_ttl
        self._tips = {k: v for k, v in self._tips.items() if v.seen_at >= cutoff}
        live = {tip.conversation_id for tip in self._tips.values()}
        self._conversation_tips = {
            k: v for k, v in self._conversation_tips.items() if k in live
        }
        seen_cutoff = self._latest - self._dedup_window
        self._seen = {k: v for k, v in self._seen.items() if v >= seen_cutoff}
        self._answered = {k: v for k, v in self._answered.items() if v >= seen_cutoff}
