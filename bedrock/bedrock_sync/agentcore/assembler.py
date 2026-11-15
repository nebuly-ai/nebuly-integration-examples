"""Deterministic, source-scoped assembly of the verified Strands trace shape."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Literal

from .models import (
    AssemblyResult,
    Diagnostic,
    IdentityLog,
    InteractionSnapshot,
    ModelCall,
    Span,
    TokenUsage,
    ToolCall,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .records import SourceScope

STRANDS = "strands.telemetry.tracer"


def _decode(value: object) -> object:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            json.dumps(decoded, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except ValueError:
            return value
        else:
            return decoded
    return value


def _text(value: object) -> str | None:
    decoded = _decode(value)
    if isinstance(decoded, str):
        return decoded
    if not isinstance(decoded, list) or not _is_content_blocks(decoded):
        return value if isinstance(value, str) else None
    parts = [
        block["text"]
        for block in decoded
        if isinstance(block, dict)
        and isinstance(block.get("text"), str)
        and block.get("type") in (None, "text")
    ]
    return "".join(parts) if parts else None


def _event_value(span: Span, name: str, key: str) -> object:
    matches = [event for event in span.events if event.get("name") == name]
    if not matches:
        return None
    attrs = matches[-1].get("attributes", {})
    return attrs.get(key) if isinstance(attrs, dict) else None


def _nonblank(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _is_agent(span: Span) -> bool:
    return span.scope == STRANDS and span.name.startswith("invoke_agent ")


def _ancestors(span: Span, spans: dict[str, Span]) -> tuple[list[Span], str | None]:
    ancestors: list[Span] = []
    visited = {span.span_id}
    current = span
    while current.parent_span_id:
        parent_id = current.parent_span_id
        if parent_id in visited:
            return ancestors, "ancestry_cycle"
        visited.add(parent_id)
        parent = spans.get(parent_id)
        if parent is None:
            # Remote HTTP parents need not be exported into this runtime group.
            if current.name == "POST /invocations":
                return ancestors, None
            return ancestors, "missing_parent"
        ancestors.append(parent)
        current = parent
    return ancestors, None


def assemble(
    source: SourceScope,
    records: Iterable[Span | IdentityLog],
    *,
    actor_id_attribute: str = "actor.id",
    parse_diagnostics: Iterable[Diagnostic] = (),
    read_start_ms: int | None = None,
) -> AssemblyResult:
    """Assemble observed records without inventing absent descendants or usage.

    Pass the earliest completely read event-time bound during incremental reads.
    None means the caller already supplied complete historical evidence.
    """
    if read_start_ms is not None and (
        isinstance(read_start_ms, bool) or read_start_ms < 0
    ):
        raise ValueError("read_start_ms must be a nonnegative timestamp")
    traces: dict[str, list[Span | IdentityLog]] = defaultdict(list)
    diagnostics = list(parse_diagnostics)
    for record in records:
        if record.source_key != source.key:
            diagnostics.append(
                Diagnostic("foreign_source", record.trace_id, record.span_id)
            )
            continue
        traces[record.trace_id].append(record)
    interactions: list[InteractionSnapshot] = []
    for trace_id in sorted(traces):
        trace_result = _assemble_trace(
            source,
            traces[trace_id],
            actor_id_attribute,
            [
                item
                for item in diagnostics
                if item.trace_id == trace_id
                and item.code not in {"foreign_source", "unsupported_log"}
            ],
        )
        for original_snapshot in trace_result.interactions:
            snapshot = original_snapshot
            if (
                read_start_ms is not None
                and snapshot.start_ns // 1_000_000 < read_start_ms
            ):
                issue = Diagnostic(
                    "unread_trace_history", snapshot.trace_id, snapshot.root_span_id
                )
                snapshot = replace(
                    snapshot,
                    readiness="open" if snapshot.readiness == "open" else "blocked",
                    diagnostics=(*snapshot.diagnostics, issue),
                )
                diagnostics.append(issue)
            interactions.append(snapshot)
        diagnostics.extend(trace_result.diagnostics)
    return AssemblyResult(tuple(interactions), tuple(dict.fromkeys(diagnostics)))


def _deduplicate(
    records: list[Span | IdentityLog],
) -> tuple[dict[str, Span], list[Diagnostic]]:
    spans: dict[str, Span] = {}
    diagnostics: list[Diagnostic] = []
    by_id: dict[str, list[Span]] = defaultdict(list)
    for record in records:
        if isinstance(record, Span):
            by_id[record.span_id].append(record)
    for span_id, versions in by_id.items():
        # A terminal export supersedes a provisional observation of the same
        # operation. Two different terminal exports still fail closed.
        terminal = [version for version in versions if version.end_ns is not None]
        candidates = terminal or versions
        chosen = candidates[0]
        identity_fields = (
            "source_key",
            "trace_id",
            "parent_span_id",
            "scope",
            "name",
            "start_ns",
            "resource_attributes",
        )
        if any(
            any(
                getattr(version, name) != getattr(chosen, name)
                for name in identity_fields
            )
            for version in versions
        ) or any(
            replace(version, timestamp_ms=0) != replace(chosen, timestamp_ms=0)
            for version in candidates
        ):
            diagnostics.append(Diagnostic("conflicting_span", chosen.trace_id, span_id))
        spans[span_id] = chosen
    return spans, diagnostics


def _assemble_trace(
    source: SourceScope,
    records: list[Span | IdentityLog],
    actor_attribute: str,
    initial_diagnostics: list[Diagnostic],
) -> AssemblyResult:
    spans, diagnostics = _deduplicate(records)
    diagnostics = initial_diagnostics + diagnostics
    roots = []
    paths: dict[str, list[Span]] = {}
    for span in spans.values():
        ancestors, issue = _ancestors(span, spans)
        paths[span.span_id] = ancestors
        if issue:
            diagnostics.append(Diagnostic(issue, span.trace_id, span.span_id))
        # Strands names the span after the agent: "invoke_agent <name>". The
        # root is the one with no agent above it; the others are sub-agents.
        if _is_agent(span) and not any(_is_agent(parent) for parent in ancestors):
            roots.append(span)
    if len(roots) != 1:
        trace_id = records[0].trace_id
        diagnostics.append(
            Diagnostic("missing_root" if not roots else "ambiguous_roots", trace_id)
        )
        return AssemblyResult((), tuple(diagnostics))
    root = roots[0]
    descendants = [
        span
        for span in spans.values()
        if root.span_id in {parent.span_id for parent in paths[span.span_id]}
    ]
    connected = {
        root.span_id,
        *(span.span_id for span in descendants),
        *(span.span_id for span in paths[root.span_id]),
    }
    if any(
        span.scope == STRANDS and span.span_id not in connected
        for span in spans.values()
    ):
        diagnostics.append(Diagnostic("disconnected_span", root.trace_id))
    snapshot = _snapshot(
        source, root, descendants, paths, records, actor_attribute, diagnostics
    )
    return AssemblyResult((snapshot,), tuple(diagnostics))


def _actor(
    root: Span,
    records: list[Span | IdentityLog],
    attribute: str,
    diagnostics: list[Diagnostic],
) -> tuple[str | None, str | None]:
    structured = _nonblank(root.attributes.get(attribute))
    fallback = {
        record.actor_id for record in records if isinstance(record, IdentityLog)
    }
    if structured is not None:
        if fallback - {structured}:
            diagnostics.append(
                Diagnostic("actor_conflict", root.trace_id, root.span_id)
            )
        return structured, "structured"
    if len(fallback) == 1:
        return next(iter(fallback)), "cloudwatch_log"
    diagnostics.append(
        Diagnostic("ambiguous_actor" if fallback else "missing_actor", root.trace_id)
    )
    return None, None


def _snapshot(
    source: SourceScope,
    root: Span,
    descendants: list[Span],
    paths: dict[str, list[Span]],
    records: list[Span | IdentityLog],
    actor_attribute: str,
    diagnostics: list[Diagnostic],
) -> InteractionSnapshot:
    actor, identity_source = _actor(root, records, actor_attribute, diagnostics)
    session = _nonblank(root.attributes.get("session.id"))
    if session is None:
        diagnostics.append(Diagnostic("missing_session", root.trace_id, root.span_id))
    incoming = _text(_event_value(root, "gen_ai.user.message", "content"))
    outgoing = _text(_event_value(root, "gen_ai.choice", "message"))
    _check_content(root, incoming, outgoing, diagnostics)
    if any(
        _unsupported_content(value)
        for value in (
            _event_value(root, "gen_ai.user.message", "content"),
            _event_value(root, "gen_ai.choice", "message"),
        )
    ):
        diagnostics.append(
            Diagnostic("unsupported_content", root.trace_id, root.span_id)
        )
    calls, tools = _collect_calls(root, descendants, paths, diagnostics)
    if not any(
        span.scope == STRANDS and span.name == "chat" for span in descendants
    ) and not _verified_zero_model_error(root):
        diagnostics.append(Diagnostic("missing_model_calls", root.trace_id))
    _check_tool_links(descendants, diagnostics)
    blocking = any(item.code != "actor_conflict" for item in diagnostics)
    readiness: Literal["open", "blocked", "ready"] = (
        "open" if root.end_ns is None else "blocked" if blocking else "ready"
    )
    earliest = min(
        root.start_ns // 1_000_000,
        *(record.timestamp_ms for record in records),
        *(span.start_ns // 1_000_000 for span in descendants),
    )
    return InteractionSnapshot(
        source.interaction_key(root.trace_id, root.span_id),
        root.trace_id,
        root.span_id,
        session,
        actor,
        identity_source,
        readiness,
        root.status,
        root.start_ns,
        root.end_ns,
        incoming,
        outgoing,
        tuple(calls),
        tuple(tools),
        tuple(diagnostics),
        earliest,
    )


def _verified_zero_model_error(root: Span) -> bool:
    if root.status != "ERROR":
        return False
    try:
        return (
            _counter(
                root.attributes,
                ("gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"),
            )
            == 0
            and _counter(
                root.attributes,
                ("gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens"),
            )
            == 0
        )
    except ValueError:
        return False


def _check_content(
    root: Span,
    incoming: str | None,
    outgoing: str | None,
    diagnostics: list[Diagnostic],
) -> None:
    if root.end_ns is not None and root.status not in {"OK", "ERROR"}:
        diagnostics.append(
            Diagnostic("missing_terminal_status", root.trace_id, root.span_id)
        )
    if incoming is None:
        diagnostics.append(Diagnostic("missing_input", root.trace_id, root.span_id))
    if outgoing is None and root.status != "ERROR":
        diagnostics.append(Diagnostic("missing_output", root.trace_id, root.span_id))
    if not incoming and not outgoing:
        diagnostics.append(Diagnostic("empty_interaction", root.trace_id, root.span_id))


def _counter(
    attrs: dict[str, Any], aliases: tuple[str, ...], *, default: int | None = None
) -> int:
    values = [attrs[key] for key in aliases if key in attrs]
    if not values:
        if default is not None:
            return default
        raise ValueError("missing_usage")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in values
    ):
        raise ValueError("invalid_usage")
    if len(set(values)) != 1:
        raise ValueError("conflicting_usage_aliases")
    return int(values[0])


def _usage(span: Span) -> TokenUsage:
    attrs = span.attributes
    incoming = _counter(
        attrs, ("gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens")
    )
    outgoing = _counter(
        attrs, ("gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens")
    )
    # Strands Usage declares cache counters optional. Some observed subagent
    # calls omit both aliases; mandatory input/output counters still fail closed.
    read = _counter(
        attrs,
        (
            "gen_ai.usage.cache_read_input_tokens",
            "gen_ai.usage.cache_read.input_tokens",
        ),
        default=0,
    )
    write = _counter(
        attrs,
        (
            "gen_ai.usage.cache_write_input_tokens",
            "gen_ai.usage.cache_creation.input_tokens",
        ),
        default=0,
    )
    if incoming < read + write:
        raise ValueError("negative_noncached_input")
    return TokenUsage(incoming - read - write, outgoing, read, write)


def _model_call(span: Span, diagnostics: list[Diagnostic]) -> ModelCall | None:
    model = _nonblank(span.attributes.get("gen_ai.request.model"))
    if model is None:
        diagnostics.append(Diagnostic("missing_model", span.trace_id, span.span_id))
        return None
    try:
        usage = _usage(span)
    except ValueError as exc:
        diagnostics.append(Diagnostic(str(exc), span.trace_id, span.span_id))
        return None
    if span.end_ns is None:
        return None
    return ModelCall(
        span.span_id,
        model,
        span.start_ns,
        span.end_ns,
        usage,
        _text(_event_value(span, "gen_ai.choice", "message")),
    )


def _tool_call(span: Span, diagnostics: list[Diagnostic]) -> ToolCall | None:
    call_id = _nonblank(span.attributes.get("gen_ai.tool.call.id"))
    incoming = _event_value(span, "gen_ai.tool.message", "content")
    outgoing = _event_value(span, "gen_ai.choice", "message")
    if (
        call_id is None
        or incoming is None
        or (outgoing is None and span.status != "ERROR")
    ):
        diagnostics.append(Diagnostic("incomplete_tool", span.trace_id, span.span_id))
        return None
    if span.end_ns is None:
        return None
    decoded_input = _decode(incoming)
    if not isinstance(decoded_input, dict):
        diagnostics.append(
            Diagnostic("invalid_tool_input", span.trace_id, span.span_id)
        )
        return None
    return ToolCall(
        span.span_id,
        call_id,
        span.name.removeprefix("execute_tool "),
        span.start_ns,
        span.end_ns,
        decoded_input,
        _decode(outgoing),
        span.status,
    )


def _check_tool_links(descendants: list[Span], diagnostics: list[Diagnostic]) -> None:
    tool_ids = set()
    for tool_span in descendants:
        if tool_span.scope != STRANDS or not tool_span.name.startswith("execute_tool "):
            continue
        call_id = _nonblank(tool_span.attributes.get("gen_ai.tool.call.id"))
        if call_id is None:
            diagnostics.append(
                Diagnostic("incomplete_tool", tool_span.trace_id, tool_span.span_id)
            )
        else:
            tool_ids.add(call_id)
    for span in descendants:
        if span.scope != STRANDS or span.name != "chat":
            continue
        choice = _decode(_event_value(span, "gen_ai.choice", "message"))
        if not isinstance(choice, list):
            continue
        for block in choice:
            tool = block.get("toolUse") if isinstance(block, dict) else None
            if (
                isinstance(tool, dict)
                and _nonblank(tool.get("toolUseId")) not in tool_ids
            ):
                diagnostics.append(
                    Diagnostic("missing_tool_span", span.trace_id, span.span_id)
                )


def _collect_calls(
    root: Span,
    descendants: list[Span],
    paths: dict[str, list[Span]],
    diagnostics: list[Diagnostic],
) -> tuple[list[ModelCall], list[ToolCall]]:
    calls: list[ModelCall] = []
    tools: list[ToolCall] = []
    for span in sorted(descendants, key=lambda item: (item.start_ns, item.span_id)):
        if span.scope != STRANDS:
            continue
        if span.end_ns is None:
            diagnostics.append(
                Diagnostic("unfinished_descendant", root.trace_id, span.span_id)
            )
        if span.name == "chat":
            call = _model_call(span, diagnostics)
            if call is not None:
                calls.append(call)
        # Tools of a sub-agent stay inside the tool call that launched it.
        if span.name.startswith("execute_tool ") and not any(
            _is_agent(parent) and parent.span_id != root.span_id
            for parent in paths[span.span_id]
        ):
            tool = _tool_call(span, diagnostics)
            if tool is not None:
                tools.append(tool)
    return calls, tools


def _unsupported_content(value: object) -> bool:
    decoded = _decode(value)
    if not isinstance(decoded, list) or not _is_content_blocks(decoded):
        return False
    return any(
        not isinstance(block, dict)
        or not (
            isinstance(block.get("text"), str)
            or "reasoningContent" in block
            or block.get("type") in ("thinking", "reasoning", "redacted_thinking")
        )
        for block in decoded
    )


def _is_content_blocks(value: object) -> bool:
    if not isinstance(value, list) or not value:
        return False
    markers = {
        "text",
        "reasoningContent",
        "toolUse",
        "toolResult",
        "image",
        "video",
        "document",
        "audio",
        "type",
    }
    return any(
        isinstance(block, dict) and bool(markers.intersection(block)) for block in value
    )
