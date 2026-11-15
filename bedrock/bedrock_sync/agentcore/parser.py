"""Parse the verified CloudWatch OTEL export without reading AgentCore Memory."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .identity import DEFAULT_ACTOR_NAMESPACE, actor_from_message
from .models import Diagnostic, IdentityLog, ParseResult, Span

if TYPE_CHECKING:
    from .records import RawLogRecord, SourceScope


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError("Expected JSON object")
    return value


def _identifier(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Expected nonblank identifier")
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool):
        raise TypeError("Expected nonnegative integer")
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        value = int(value)
    if not isinstance(value, int) or value < 0:
        raise ValueError("Expected nonnegative integer")
    return value


def parse_record(
    raw: RawLogRecord,
    source: SourceScope,
    *,
    actor_namespace_template: str = DEFAULT_ACTOR_NAMESPACE,
) -> ParseResult:
    """Return a normalized record or a content-free diagnostic; never log bodies."""
    trace_id: str | None = None
    span_id: str | None = None
    try:
        data = _object(json.loads(raw.message))
        resource = _object(_object(data.get("resource", {})).get("attributes", {}))
        if resource.get("cloud.resource_id") != source.source_id:
            return ParseResult(diagnostics=(Diagnostic("foreign_or_missing_source"),))
        if resource.get("cloud.region", source.region) != source.region:
            return ParseResult(diagnostics=(Diagnostic("foreign_region"),))
        trace_id = _identifier(data.get("traceId"))
        span_id = _identifier(data.get("spanId"))
        # Python accepts non-JSON NaN/Infinity and escaped lone surrogates.
        # Diagnose these with their trace before persistence or UTF-8 delivery.
        json.dumps(data, ensure_ascii=False, allow_nan=False).encode("utf-8")
        scope = _identifier(_object(data.get("scope", {})).get("name"))
        if "startTimeUnixNano" not in data:
            return _identity(
                data, source, raw, trace_id, span_id, scope, actor_namespace_template
            )
        return _span(data, source, raw, trace_id, span_id, scope, resource)
    except (ValueError, TypeError, KeyError):
        return ParseResult(
            diagnostics=(Diagnostic("malformed_record", trace_id, span_id),)
        )


def _identity(
    data: dict[str, Any],
    source: SourceScope,
    raw: RawLogRecord,
    trace_id: str,
    span_id: str,
    scope: str,
    namespace: str,
) -> ParseResult:
    if scope != "bedrock_agentcore.memory.client":
        return ParseResult(
            diagnostics=(Diagnostic("unsupported_log", trace_id, span_id),)
        )
    body = data.get("body")
    if isinstance(body, str):
        body = json.loads(body)
    message = _object(body).get("message")
    if not isinstance(message, str):
        raise TypeError("Expected log message")
    message.encode("utf-8")
    actor = actor_from_message(message, namespace)
    if actor is None:
        return ParseResult(
            diagnostics=(Diagnostic("unsupported_log", trace_id, span_id),)
        )
    return ParseResult(
        identity=IdentityLog(
            source.key,
            trace_id,
            span_id,
            actor,
            raw.timestamp_ms,
        )
    )


def _span(
    data: dict[str, Any],
    source: SourceScope,
    raw: RawLogRecord,
    trace_id: str,
    span_id: str,
    scope: str,
    resource: dict[str, Any],
) -> ParseResult:
    start = _integer(data["startTimeUnixNano"])
    end = _integer(data["endTimeUnixNano"]) if "endTimeUnixNano" in data else None
    if end is not None and end < start:
        raise ValueError("Invalid span interval")
    parent = data.get("parentSpanId") or None
    if parent is not None:
        parent = _identifier(parent)
    attrs = _object(data.get("attributes", {}))
    events = data.get("events", [])
    if not isinstance(events, list):
        raise TypeError("Expected events")
    parsed_events = tuple(_object(event) for event in events)
    status = _object(data.get("status", {})).get("code", "UNSET")
    if type(status) not in {str, int} or status not in {
        "OK",
        "ERROR",
        "UNSET",
        0,
        1,
        2,
    }:
        raise ValueError("Unsupported status")
    status = {0: "UNSET", 1: "OK", 2: "ERROR"}.get(status, status)
    return ParseResult(
        span=Span(
            source.key,
            trace_id,
            span_id,
            parent,
            scope,
            _identifier(data.get("name")),
            start,
            end,
            str(status),
            attrs,
            parsed_events,
            resource,
            raw.timestamp_ms,
        )
    )
