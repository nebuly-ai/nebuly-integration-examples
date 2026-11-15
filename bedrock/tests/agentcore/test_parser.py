"""Malformed, unrelated and nested-body telemetry remains explicit."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from bedrock_sync.agentcore.parser import parse_record
from bedrock_sync.agentcore.records import RawLogRecord, SourceScope
from tests.agentcore.cases import load, raw_record


def sample() -> tuple[SourceScope, RawLogRecord]:
    case = load()
    return SourceScope(**case["source"]), raw_record(case["records"][0])


@pytest.mark.parametrize("message", ["not-json", "[]", "null", "{}"])
def test_bad_message_is_diagnosed_without_content(message: str) -> None:
    source, raw = sample()
    result = parse_record(replace(raw, message=message), source)
    assert result.span is None
    assert result.identity is None
    assert result.diagnostics
    assert message not in repr(result) or message == "{}"


def test_identity_log_accepts_object_body_and_preserves_actor() -> None:
    source, raw = sample()
    data = json.loads(raw.message)
    data["body"] = json.loads(data["body"])
    result = parse_record(replace(raw, message=json.dumps(data)), source)
    assert result.identity is not None
    assert result.identity.actor_id == "synthetic-actor-001"


def test_identity_requires_trace_and_exact_source() -> None:
    source, raw = sample()
    data = json.loads(raw.message)
    del data["traceId"]
    assert parse_record(replace(raw, message=json.dumps(data)), source).identity is None
    data = json.loads(raw.message)
    data["resource"]["attributes"]["cloud.resource_id"] += "foreign"
    assert parse_record(replace(raw, message=json.dumps(data)), source).identity is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("startTimeUnixNano", True),
        ("endTimeUnixNano", -1),
        ("events", {}),
        ("status", {"code": []}),
        ("status", {"code": True}),
        ("status", {"code": 1.0}),
        ("attributes", []),
        ("attributes", {"invalid": float("nan")}),
        ("attributes", {"invalid": float("inf")}),
        ("attributes", {"invalid": "\ud800"}),
    ],
)
def test_malformed_span_remains_trace_linked(field: str, value: object) -> None:
    source, _ = sample()
    case = json.loads(
        (Path(__file__).with_name("fixtures") / "text_cache_read.json").read_text()
    )
    event = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name") == "chat"
    )
    data = json.loads(event["message"])
    data[field] = value
    raw = RawLogRecord(
        event["logGroupName"],
        event["logStreamName"],
        event["eventId"],
        event["timestamp"],
        event["ingestionTime"],
        json.dumps(data),
    )
    result = parse_record(raw, source)
    assert result.span is None
    assert result.diagnostics[0].code == "malformed_record"
    assert result.diagnostics[0].trace_id == data["traceId"]
    assert result.diagnostics[0].span_id == data["spanId"]


def test_nested_identity_body_with_invalid_unicode_remains_trace_linked() -> None:
    source, raw = sample()
    data = json.loads(raw.message)
    body = json.loads(data["body"])
    body["message"] = "Retrieved 1 memories from namespace: /actors/\ud800/context"
    data["body"] = json.dumps(body)
    result = parse_record(replace(raw, message=json.dumps(data)), source)
    assert result.identity is None
    assert result.diagnostics[0].code == "malformed_record"
    assert result.diagnostics[0].trace_id == data["traceId"]


@pytest.mark.parametrize(
    ("template", "namespace"),
    [
        ("/facts/{actorId}/", "/facts/Actor A/facts/"),
        (
            "/org/{tenantId}/users/{actorId}/session/{sessionId}/",
            "/org/customer/users/Actor A/session/one/",
        ),
        ("/facts.v1/{actorId}", "/facts.v1/Actor A"),
    ],
)
def test_namespace_templates_extract_exact_actor(template: str, namespace: str) -> None:
    source, raw = sample()
    data = json.loads(raw.message)
    data["body"] = {"message": "Retrieved 0 memories from namespace: " + namespace}
    parsed = parse_record(
        replace(raw, message=json.dumps(data)),
        source,
        actor_namespace_template=template,
    )
    assert parsed.identity is not None
    assert parsed.identity.actor_id == "Actor A"
