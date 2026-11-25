"""Format-independent types for Copilot usage records and grouped calls."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

type JsonDict = dict[str, Any]
type Role = Literal["user", "assistant"]


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
    kind: BlockKind
    text: str = ""
    tool_use_id: str | None = None
    tool_name: str | None = None
    tool_input: Any = None


@dataclass(frozen=True)
class Message:
    role: Role
    blocks: tuple[Block, ...]

    @property
    def tool_result_ids(self) -> frozenset[str]:
        return frozenset(
            block.tool_use_id
            for block in self.blocks
            if block.kind is BlockKind.TOOL_RESULT and block.tool_use_id
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


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(*parts: object) -> str:
    rendered = [
        part if isinstance(part, str) else canonical_json(part) for part in parts
    ]
    raw = "|".join(rendered)
    return hashlib.sha256(raw.encode()).hexdigest()


def turn_start_index(messages: tuple[Message, ...]) -> int | None:
    """Index of the latest user message that is not a tool result."""
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if message.role == "user" and not message.tool_result_ids:
            return index
    return None


def turn_start(messages: tuple[Message, ...]) -> Message | None:
    """Latest user message that is not a tool result (opens the current turn)."""
    index = turn_start_index(messages)
    return None if index is None else messages[index]


def join_text(blocks: tuple[Block, ...]) -> str:
    texts = (block.text.strip() for block in blocks if block.kind is BlockKind.TEXT)
    return "\n".join(text for text in texts if text)


CHAT_COMPLETIONS_MARKERS = ("/chat/completions", "chat_completion")


def is_chat_completions_endpoint(endpoint: str) -> bool:
    lowered = endpoint.lower()
    return any(marker in lowered for marker in CHAT_COMPLETIONS_MARKERS)


class UsageRecord(BaseModel):
    """One GitHub Copilot usage record (request or response)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    type: Literal["request", "response"]
    user_id: int
    enterprise_id: int
    endpoint: str
    body: str
    timestamp_ms: int = Field(validation_alias="@timestamp")
    truncated: bool = False
    event_id: str
    github_request_id: str

    @classmethod
    def from_raw(cls, raw: JsonDict) -> UsageRecord | None:
        record_type = raw.get("type")
        github_request_id = raw.get("github_request_id")
        body = raw.get("body")
        if (
            record_type not in ("request", "response")
            or not github_request_id
            or body is None
        ):
            return None
        timestamp = raw.get("@timestamp")
        event_id = raw.get("event_id")
        if timestamp is None or not event_id:
            return None
        return cls.model_validate(
            {
                **raw,
                "@timestamp": timestamp,
            }
        )


@dataclass(frozen=True)
class Call:
    github_request_id: str
    user_id: int
    endpoint: str
    request_body: str
    response_body: str
    request_timestamp_ms: int
    response_timestamp_ms: int
    request_event_id: str
    response_event_id: str
    request_truncated: bool = False
    response_truncated: bool = False

    @property
    def truncated(self) -> bool:
        return self.request_truncated or self.response_truncated


@dataclass(frozen=True)
class ClosedTurn:
    turn_key: str
    conversation_id: str
    user_id: int
    calls: tuple[Call, ...]
    time_start_ms: int
    time_end_ms: int


@dataclass
class PipelineCounters:
    non_usage_events: int = 0
    unpaired_requests: int = 0
    skipped_non_chat: int = 0
    skipped_background: int = 0
    unparseable_bodies: int = 0
