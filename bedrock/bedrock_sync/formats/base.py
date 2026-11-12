"""Contract shared by the format adapters."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from bedrock_sync.models import Block, Message, StopReason, TokenUsage


class BodyFormatError(ValueError):
    """The body does not have the shape the adapter expects."""


@dataclass(frozen=True)
class ParsedBody:
    """What an adapter extracts from the request and response bodies.

    ``usage`` and ``latency_ms`` are None when the body does not carry them;
    the parser then falls back to the record envelope.
    """

    system: str
    messages: tuple[Message, ...]
    output: tuple[Block, ...]
    stop_reason: StopReason
    usage: TokenUsage | None
    latency_ms: int | None
    max_tokens: int | None


def as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value)
    return None


def as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: object) -> str:
    raw = value if isinstance(value, str) else canonical_json(value)
    return hashlib.sha256(raw.encode()).hexdigest()


def parse_tool_arguments(raw: object) -> object:
    """OpenAI sends tool arguments as a JSON string; keep the string if invalid."""
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw
