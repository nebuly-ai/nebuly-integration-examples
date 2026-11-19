"""Independent integrity checks for sanitized telemetry and expected results.

Real cases retain export schemas and counter values, but all content and opaque
identifiers are synthetic. Variants explicitly distinguish simulated behavior.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

from tests.agentcore.cases import FIXTURES, REAL_CASES, load

if TYPE_CHECKING:
    from pathlib import Path


def records(case: dict[str, Any]) -> list[dict[str, Any]]:
    return [json.loads(event["message"]) for event in case["records"]]


def text(value: str) -> str:
    try:
        decoded = json.loads(value)
    except ValueError:
        return value
    assert isinstance(decoded, list)
    return "".join(block.get("text", "") for block in decoded)


@pytest.mark.parametrize("path", REAL_CASES, ids=lambda path: path.stem)
def test_real_case_envelopes_identity_and_terminal_oracle(path: Path) -> None:
    case = json.loads(path.read_text())
    expected = case["expected"]
    assert case["schema_version"] == 1
    assert case["provenance"]["kind"] == "sanitized_real"
    assert "context truncation" in case["provenance"]["does_not_verify"]
    decoded = records(case)
    root = next(
        record
        for record in decoded
        if record.get("spanId") == expected["root_span_id"] and "name" in record
    )
    assert root["name"] == "invoke_agent Strands Agents"
    assert root["scope"]["name"] == "strands.telemetry.tracer"
    assert root["attributes"]["session.id"] == expected["session_id"]
    assert root["status"]["code"] == expected["terminal_status"]
    assert root["endTimeUnixNano"] >= root["startTimeUnixNano"]
    assert root["durationNano"] == root["endTimeUnixNano"] - root["startTimeUnixNano"]
    incoming = [
        event for event in root["events"] if event["name"] == "gen_ai.user.message"
    ]
    outgoing = [event for event in root["events"] if event["name"] == "gen_ai.choice"]
    assert text(incoming[-1]["attributes"]["content"]) == expected["input"]
    assert (
        text(outgoing[-1]["attributes"]["message"]) if outgoing else None
    ) == expected["output"]
    identity = [
        record
        for record in decoded
        if record["scope"]["name"] == "bedrock_agentcore.memory.client"
    ]
    assert len(identity) == 1
    assert identity[0]["traceId"] == expected["trace_id"]
    assert (
        f"/actors/{expected['actor_id']}/" in json.loads(identity[0]["body"])["message"]
    )
    assert len({event["eventId"] for event in case["records"]}) == len(decoded)
    for event, record in zip(case["records"], decoded, strict=True):
        assert event["logStreamName"] == "otel-rt-logs"
        assert isinstance(event["timestamp"], int)
        assert isinstance(event["ingestionTime"], int)
        assert record["traceId"] == expected["trace_id"]
        assert (
            record["resource"]["attributes"]["cloud.resource_id"]
            == case["source"]["source_id"]
        )


@pytest.mark.parametrize("path", REAL_CASES, ids=lambda path: path.stem)
def test_usage_oracle_counts_only_canonical_model_spans(path: Path) -> None:
    case = json.loads(path.read_text())
    decoded = records(case)
    canonical = [
        record
        for record in decoded
        if record.get("name") == "chat"
        and record["scope"]["name"] == "strands.telemetry.tracer"
    ]
    assert [span["spanId"] for span in canonical] == case["expected"][
        "canonical_model_span_ids"
    ]
    assert any(
        record["scope"]["name"]
        == "opentelemetry.instrumentation.botocore.bedrock-runtime"
        for record in decoded
    )
    totals: dict[str, Counter[str]] = {}
    for span in canonical:
        attrs = span["attributes"]
        assert attrs["gen_ai.usage.input_tokens"] == attrs["gen_ai.usage.prompt_tokens"]
        assert (
            attrs["gen_ai.usage.output_tokens"]
            == attrs["gen_ai.usage.completion_tokens"]
        )
        cached_read = attrs["gen_ai.usage.cache_read_input_tokens"]
        cached_write = attrs["gen_ai.usage.cache_write_input_tokens"]
        assert cached_read == attrs["gen_ai.usage.cache_read.input_tokens"]
        assert cached_write == attrs["gen_ai.usage.cache_creation.input_tokens"]
        noncached = attrs["gen_ai.usage.input_tokens"] - cached_read - cached_write
        assert noncached >= 0
        totals.setdefault(attrs["gen_ai.request.model"], Counter()).update(
            input_tokens=noncached,
            output_tokens=attrs["gen_ai.usage.output_tokens"],
            cache_read_input_tokens=cached_read,
            cache_write_input_tokens=cached_write,
        )
    assert dict(totals) == case["expected"]["usage_by_model"]


def test_corpus_covers_real_errors_caches_models_and_subagent_tools() -> None:
    cases = [json.loads(path.read_text()) for path in REAL_CASES]
    usages = [
        usage for case in cases for usage in case["expected"]["usage_by_model"].values()
    ]
    assert any(usage["cache_read_input_tokens"] > 0 for usage in usages)
    assert any(usage["cache_write_input_tokens"] > 0 for usage in usages)
    assert (
        len({model for case in cases for model in case["expected"]["usage_by_model"]})
        == 2
    )
    error = load("terminal_error")["expected"]
    assert error["terminal_status"] == "ERROR"
    assert error["input"]
    assert error["output"] is None
    subagent = load("subagent_usage")
    tools = subagent["expected"]["tools"]
    assert any(
        tool["name"] == "subagent" and not tool["internal_to_subagent"]
        for tool in tools
    )
    assert any(
        tool["name"] == "shell" and tool["internal_to_subagent"] for tool in tools
    )
    assert len(subagent["expected"]["canonical_model_span_ids"]) > 1


@pytest.mark.parametrize("path", REAL_CASES, ids=lambda path: path.stem)
def test_fixture_identifiers_and_content_are_synthetic(path: Path) -> None:
    case = json.loads(path.read_text())
    expected = case["expected"]
    assert expected["actor_id"].startswith("synthetic-actor-")
    assert expected["session_id"].startswith("synthetic-session-")
    assert re.fullmatch(r"0{28}[0-9a-f]{4}", expected["trace_id"])
    assert case["source"]["account_id"] == "123456789012"
    assert "runtime/synthetic-agent/" in case["source"]["source_id"]
    assert expected["input"].startswith("Synthetic content ")
    assert expected["output"] is None or expected["output"].startswith(
        "Synthetic content "
    )
    for record in records(case):
        assert re.fullmatch(r"0{12}[0-9a-f]{4}", record["spanId"])
        assert (
            record["resource"]["attributes"]["service.name"]
            == "synthetic-agent.DEFAULT"
        )
        if record.get("parentSpanId"):
            assert re.fullmatch(r"0{12}[0-9a-f]{4}", record["parentSpanId"])


def test_synthetic_variants_are_explicitly_separate_contract_expectations() -> None:
    variants = load("synthetic_variants")
    assert variants["provenance"]["kind"] == "synthetic_variants"
    assert len({variant["name"] for variant in variants["cases"]}) == len(
        variants["cases"]
    )
    for variant in variants["cases"]:
        assert (FIXTURES / f"{variant['base']}.json").is_file()
        assert variant["mutation"]
        assert variant["expected"]["readiness"] in {"ready", "blocked", "open"}


@pytest.mark.parametrize(
    "name", ["principal_tools", "subagent_usage", "terminal_error"]
)
def test_tool_oracle_preserves_results_and_subagent_ancestry(name: str) -> None:
    case = load(name)
    spans = {
        record["spanId"]: record
        for record in records(case)
        if "startTimeUnixNano" in record
    }
    for tool in case["expected"]["tools"]:
        span = spans[tool["span_id"]]
        assert span["attributes"]["gen_ai.tool.call.id"] == tool["call_id"]
        assert span["name"] == f"execute_tool {tool['name']}"
        incoming = [
            event["attributes"]["content"]
            for event in span["events"]
            if event["name"] == "gen_ai.tool.message"
        ]
        outgoing = [
            event["attributes"]["message"]
            for event in span["events"]
            if event["name"] == "gen_ai.choice"
        ]
        assert (json.loads(incoming[0]) if incoming else None) == tool["input"]
        assert (json.loads(outgoing[0]) if outgoing else None) == tool["output"]
        parent = spans.get(span.get("parentSpanId", ""))
        ancestry = []
        while parent is not None:
            ancestry.append(parent["name"])
            parent = spans.get(parent.get("parentSpanId", ""))
        assert ("invoke_agent subagent" in ancestry) == tool["internal_to_subagent"]


def test_reusable_cases_materialize_labeled_mutations(
    agentcore_case: dict[str, Any],
) -> None:
    case = agentcore_case
    provenance = case["provenance"]
    if provenance["kind"] == "sanitized_real":
        assert case["records"]
        return
    assert provenance["kind"] == "synthetic"
    decoded = records(case)
    roots = [
        record
        for record in decoded
        if record.get("name") == "invoke_agent Strands Agents"
    ]
    assert len(roots) == 1
    root = roots[0]
    identity_logs = [
        record
        for record in decoded
        if record["scope"]["name"] == "bedrock_agentcore.memory.client"
    ]
    mutation = provenance["mutation"]
    if mutation in {"remove_fallback", "replace_fallback_with_structured_actor"}:
        assert not identity_logs
    if mutation in {"replace_fallback_with_structured_actor", "add_structured_actor"}:
        assert root["attributes"]["actor.id"] == case["expected"]["actor_id"]
    if mutation == "add_structured_actor":
        assert identity_logs
    if mutation == "remove_root_end":
        assert "endTimeUnixNano" not in root
        assert case["expected"]["readiness"] == "open"
    if mutation == "duplicate_model_span":
        chats = [record for record in decoded if record.get("name") == "chat"]
        assert len(chats) == 2
        assert chats[0] == chats[1]
        assert len({event["logGroupName"] for event in case["records"]}) == 2
        assert len(case["expected"]["canonical_model_span_ids"]) == 1
    if mutation == "remove_input_usage_aliases":
        chat = next(record for record in decoded if record.get("name") == "chat")
        assert "gen_ai.usage.input_tokens" not in chat["attributes"]
        assert "gen_ai.usage.prompt_tokens" not in chat["attributes"]
        assert case["expected"]["readiness"] == "ready"  # sent without its usage
