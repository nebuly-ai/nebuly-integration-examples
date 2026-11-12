"""Format-independent model of a Bedrock invocation.

Every adapter in ``bedrock_sync.formats`` turns a raw log record into these
types, so grouping and conversion never look at Converse, Anthropic or OpenAI
shapes directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Literal

type JsonDict = dict[str, Any]


class ApiFormat(StrEnum):
    CONVERSE = "converse"
    ANTHROPIC = "anthropic"
    OPENAI = "openai"


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
    is_error: bool = False


type Role = Literal["user", "assistant"]


@dataclass(frozen=True)
class Message:
    role: Role
    blocks: tuple[Block, ...]

    @property
    def text(self) -> str:
        return join_text(self.blocks)

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
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0

    @property
    def total_input_tokens(self) -> int:
        """Bedrock reports cached prompt tokens apart from ``input_tokens``."""
        return self.input_tokens + self.cache_read_tokens + self.cache_write_tokens


@dataclass(frozen=True)
class ClaudeCodeMetadata:
    """Identity Claude Code sends as a JSON string in ``metadata.user_id``."""

    session_id: str | None
    device_id: str | None
    account_uuid: str | None


@dataclass(frozen=True)
class Invocation:
    """One successful model call, whatever API produced it."""

    request_id: str
    timestamp: datetime
    latency_ms: int | None
    operation: str
    api_format: ApiFormat
    model: str
    model_id: str
    region: str
    inference_region: str
    account_id: str
    identity_arn: str
    request_metadata: dict[str, str]
    system: str
    messages: tuple[Message, ...]
    output: tuple[Block, ...]
    stop_reason: StopReason
    usage: TokenUsage
    max_tokens: int | None
    claude_code: ClaudeCodeMetadata | None
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

    @property
    def last_message(self) -> Message | None:
        return self.messages[-1] if self.messages else None


class SkipReason(StrEnum):
    ERROR_RECORD = "error_record"
    MISSING_BODY = "missing_body"
    UNSUPPORTED_FORMAT = "unsupported_format"
    INVALID_BODY = "invalid_body"


@dataclass(frozen=True)
class Skipped:
    request_id: str
    reason: SkipReason
    detail: str = ""


def tool_loop(messages: tuple[Message, ...]) -> tuple[Message, ...]:
    """The tool round trips that follow the latest user message that is not a
    tool result: the agent loop of the current turn, as the history shows it."""
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if message.role == "user" and not message.tool_result_ids:
            return messages[index + 1 :]
    return messages


def join_text(blocks: tuple[Block, ...]) -> str:
    return "\n".join(b.text for b in blocks if b.kind is BlockKind.TEXT and b.text)
