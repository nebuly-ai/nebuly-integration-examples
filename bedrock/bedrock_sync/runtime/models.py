"""Format-independent model of a Bedrock invocation.

Every adapter in ``bedrock_sync.runtime.formats`` turns a raw log record into these
types, so grouping and conversion never look at Converse, Anthropic or OpenAI
shapes directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Literal

type JsonDict = dict[str, Any]


class BlockKind(StrEnum):
    TEXT = "text"
    TOOL_USE = "tool_use"
    TOOL_RESULT = "tool_result"
    REASONING = "reasoning"
    IMAGE = "image"
    DOCUMENT = "document"
    OTHER = "other"


@dataclass(frozen=True)
class Block:
    """One content block.

    ``text`` holds the text for TEXT and REASONING, the rendered result for
    TOOL_RESULT and a digest of the source for IMAGE and DOCUMENT.
    """

    kind: BlockKind
    text: str = ""
    tool_use_id: str | None = None
    tool_name: str | None = None
    tool_input: Any = None


type Role = Literal["user", "assistant"]


@dataclass(frozen=True)
class Message:
    role: Role
    blocks: tuple[Block, ...]

    @property
    def tool_result_ids(self) -> frozenset[str]:
        return frozenset(
            b.tool_use_id
            for b in self.blocks
            if b.kind is BlockKind.TOOL_RESULT and b.tool_use_id
        )


class StopReason(StrEnum):
    END_TURN = "end_turn"
    TOOL_USE = "tool_use"
    MAX_TOKENS = "max_tokens"
    STOP_SEQUENCE = "stop_sequence"
    CONTENT_FILTERED = "content_filtered"
    OTHER = "other"


@dataclass(frozen=True)
class TokenUsage:
    """Token counts of one call. The Bedrock envelope counters win over the
    body ones (see ``parser._envelope_usage``); the body only fills gaps."""

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass(frozen=True)
class Invocation:
    """One successful model call, whatever API produced it.

    ``model`` is the model id without the inference profile and its
    geographic prefix (``anthropic.claude-...``).
    """

    request_id: str
    timestamp: datetime
    latency_ms: int | None
    model: str
    account_id: str
    request_metadata: dict[str, str]
    system: str
    messages: tuple[Message, ...]
    output: tuple[Block, ...]
    stop_reason: StopReason
    usage: TokenUsage
    source_key: str

    @property
    def time_end(self) -> datetime:
        if self.latency_ms is None:
            return self.timestamp
        return self.timestamp + timedelta(milliseconds=self.latency_ms)

    @property
    def output_text(self) -> str:
        return join_text(self.output)

    @property
    def tool_uses(self) -> tuple[Block, ...]:
        return tuple(b for b in self.output if b.kind is BlockKind.TOOL_USE)


class RecordSkip(StrEnum):
    """Why the parser could not use a record."""

    ERROR_RECORD = "error_record"
    MISSING_BODY = "missing_body"
    UNSUPPORTED_FORMAT = "unsupported_format"
    INVALID_BODY = "invalid_body"


@dataclass(frozen=True)
class Skipped:
    request_id: str
    reason: RecordSkip
    detail: str = ""


def _turn_index(messages: tuple[Message, ...]) -> int | None:
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if message.role == "user" and not message.tool_result_ids:
            return index
    return None


def turn_start(messages: tuple[Message, ...]) -> Message | None:
    """The message that opened the current turn: the latest user message that
    holds no tool result. A user message with tool results is a step of the
    agent loop, even if the client adds text to it."""
    index = _turn_index(messages)
    return None if index is None else messages[index]


def tool_loop(messages: tuple[Message, ...]) -> tuple[Message, ...]:
    """The tool round trips after ``turn_start``: the agent loop of the
    current turn, as the history shows it."""
    index = _turn_index(messages)
    return messages if index is None else messages[index + 1 :]


def join_text(blocks: tuple[Block, ...]) -> str:
    """The text blocks, stripped, one per line."""
    texts = (b.text.strip() for b in blocks if b.kind is BlockKind.TEXT)
    return "\n".join(text for text in texts if text)
