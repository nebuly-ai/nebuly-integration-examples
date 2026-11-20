"""Normalized AgentCore telemetry and pure assembly results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class Diagnostic:
    code: str
    trace_id: str | None = None
    span_id: str | None = None


@dataclass(frozen=True)
class Span:
    source_key: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    scope: str
    name: str
    start_ns: int
    end_ns: int | None
    status: str
    attributes: dict[str, Any] = field(repr=False)
    events: tuple[dict[str, Any], ...] = field(repr=False)
    resource_attributes: dict[str, Any] = field(repr=False)
    timestamp_ms: int


@dataclass(frozen=True)
class IdentityLog:
    source_key: str
    trace_id: str
    span_id: str
    actor_id: str = field(repr=False)
    timestamp_ms: int


@dataclass(frozen=True)
class ParseResult:
    span: Span | None = None
    identity: IdentityLog | None = None
    diagnostics: tuple[Diagnostic, ...] = ()


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_write_input_tokens: int


@dataclass(frozen=True)
class ModelCall:
    span_id: str
    model_id: str
    start_ns: int
    end_ns: int
    usage: TokenUsage
    output: str | None = field(repr=False)


@dataclass(frozen=True)
class ToolCall:
    span_id: str
    call_id: str
    name: str
    start_ns: int
    end_ns: int
    input: Any = field(repr=False)
    output: Any = field(repr=False)
    status: str


@dataclass(frozen=True)
class InteractionSnapshot:
    interaction_key: str
    trace_id: str
    root_span_id: str
    session_id: str | None
    actor_id: str | None = field(repr=False)
    identity_source: str | None
    readiness: Literal["open", "blocked", "ready"]
    terminal_status: str
    start_ns: int
    end_ns: int | None
    input: str | None = field(repr=False)
    output: str | None = field(repr=False)
    model_calls: tuple[ModelCall, ...]
    tools: tuple[ToolCall, ...]
    diagnostics: tuple[Diagnostic, ...]
    earliest_timestamp_ms: int

    @property
    def conversation_id(self) -> str:
        """The session id; without one, the trace is a conversation of its own."""
        return self.session_id or self.trace_id

    @property
    def anchored(self) -> bool:
        """The root's parents reach the top of the trace: a span without a
        parent, or POST /invocations (its parent is the AgentCore service).
        If not, a parent is still missing: the root may be a sub-agent whose
        caller has not been exported yet."""
        return not any(
            item.code in {"missing_parent", "ancestry_cycle"}
            and item.span_id == self.root_span_id
            for item in self.diagnostics
        )

    @property
    def warnings(self) -> tuple[str, ...]:
        """Problems the interaction is sent with, each code once."""
        return tuple(dict.fromkeys(item.code for item in self.diagnostics))


@dataclass(frozen=True)
class AssemblyResult:
    interactions: tuple[InteractionSnapshot, ...]
    diagnostics: tuple[Diagnostic, ...] = ()
