"""Pure assembly regressions against sanitized exports and explicit mutations."""

from __future__ import annotations

import copy
import json
from collections import Counter
from dataclasses import asdict, replace
from typing import Any

import pytest

from bedrock_sync.agentcore.assembler import assemble
from bedrock_sync.agentcore.models import Diagnostic, Span
from bedrock_sync.agentcore.records import SourceScope
from tests.agentcore.cases import load, parse_case


def test_assembly_matches_reusable_oracles(agentcore_case: dict[str, Any]) -> None:
    case = agentcore_case
    source, records = parse_case(case)
    result = assemble(source, reversed(records))
    assert len(result.interactions) == 1
    interaction = result.interactions[0]
    expected = case["expected"]
    assert interaction.readiness == expected.get("readiness", "ready")
    assert interaction.actor_id == expected["actor_id"]
    assert interaction.session_id == expected["session_id"]
    assert interaction.input == expected["input"]
    assert interaction.output == expected["output"]
    assert interaction.terminal_status == expected["terminal_status"]
    if expected.get("diagnostic"):
        assert expected["diagnostic"] in {item.code for item in interaction.diagnostics}
    if interaction.readiness != "ready":
        return
    actual: dict[str, Counter[str]] = {}
    for call in interaction.model_calls:
        actual.setdefault(call.model_id, Counter()).update(asdict(call.usage))
    assert actual == expected["usage_by_model"]
    visible_tools = [
        tool for tool in expected["tools"] if not tool["internal_to_subagent"]
    ]
    assert [tool.span_id for tool in interaction.tools] == [
        tool["span_id"] for tool in visible_tools
    ]
    for actual_tool, expected_tool in zip(
        interaction.tools, visible_tools, strict=True
    ):
        assert actual_tool.input == expected_tool["input"]
        assert actual_tool.output == expected_tool["output"]
    assert interaction.earliest_timestamp_ms <= interaction.start_ns // 1_000_000
    assert assemble(source, records) == assemble(source, reversed(records))


@pytest.mark.parametrize(
    ("mutation", "warning"),
    [
        ("missing_parent", "missing_parent"),
        ("cycle", "ancestry_cycle"),
        ("alias_conflict", "conflicting_usage_aliases"),
        ("negative_usage", "invalid_usage"),
        ("reasoning_output", "missing_output"),
        ("missing_tool", "missing_tool_span"),
    ],
)
def test_damaged_trace_is_sent_without_the_damaged_part(
    mutation: str, warning: str
) -> None:
    source, records = parse_case(load("principal_tools"))
    root = next(
        item
        for item in records
        if isinstance(item, Span) and item.name == "invoke_agent Strands Agents"
    )
    chat = next(
        item for item in records if isinstance(item, Span) and item.name == "chat"
    )
    replacement = chat
    if mutation == "missing_parent":
        replacement = replace(chat, parent_span_id="missing")
    elif mutation == "cycle":
        replacement = replace(chat, parent_span_id=chat.span_id)
    elif mutation in {"alias_conflict", "negative_usage"}:
        attrs = dict(chat.attributes)
        attrs["gen_ai.usage.input_tokens"] = -1 if mutation == "negative_usage" else 1
        replacement = replace(chat, attributes=attrs)
    elif mutation == "reasoning_output":
        events = copy.deepcopy(root.events)
        events[-1]["attributes"]["message"] = (
            '[{"reasoningContent":{"text":"private"}}]'
        )
        records = [
            replace(item, events=events) if item is root else item for item in records
        ]
    else:
        records = [
            item
            for item in records
            if not (isinstance(item, Span) and item.name.startswith("execute_tool "))
        ]
    records = [replacement if item is chat else item for item in records]
    snapshot = assemble(source, records).interactions[0]
    assert snapshot.readiness == "ready"
    assert warning in snapshot.warnings
    assert snapshot.output != "private"
    if replacement is not chat:
        assert chat.span_id not in {call.span_id for call in snapshot.model_calls}


def test_conflicting_span_is_a_warning_but_cross_group_duplicate_is_not() -> None:
    source, records = parse_case(load())
    chat = next(
        item for item in records if isinstance(item, Span) and item.name == "chat"
    )
    assert (
        assemble(source, [*records, replace(chat, timestamp_ms=chat.timestamp_ms + 1)])
        .interactions[0]
        .readiness
        == "ready"
    )
    conflicting = replace(chat, end_ns=chat.end_ns + 1)  # type: ignore[operator]
    snapshot = assemble(source, [*records, conflicting]).interactions[0]
    assert snapshot.readiness == "ready"
    assert "conflicting_span" in snapshot.warnings


def test_missing_root_and_multiple_roots_are_diagnosed() -> None:
    source, records = parse_case(load())
    root = next(
        item
        for item in records
        if isinstance(item, Span) and item.name == "invoke_agent Strands Agents"
    )
    result = assemble(source, [item for item in records if item is not root])
    assert not result.interactions
    assert "missing_root" in {item.code for item in result.diagnostics}
    result = assemble(
        source, [*records, replace(root, span_id="another", parent_span_id=None)]
    )
    assert not result.interactions
    assert "ambiguous_roots" in {item.code for item in result.diagnostics}


def test_source_isolation_and_exact_case_sensitive_identity() -> None:
    source, records = parse_case(load())
    foreign = SourceScope(source.account_id, source.region, source.source_id + "-other")
    assert not assemble(foreign, records).interactions
    root = next(
        item
        for item in records
        if isinstance(item, Span) and item.name == "invoke_agent Strands Agents"
    )
    records = [
        replace(item, attributes={**root.attributes, "customer.actor": " ActorA "})
        if item is root
        else item
        for item in records
    ]
    snapshot = assemble(
        source, records, actor_id_attribute="customer.actor"
    ).interactions[0]
    assert snapshot.actor_id == " ActorA "
    assert snapshot.readiness == "ready"


def test_known_malformed_child_diagnostic_is_a_warning() -> None:
    source, records = parse_case(load())
    trace_id = records[0].trace_id
    snapshot = assemble(
        source,
        records,
        parse_diagnostics=[Diagnostic("malformed_record", trace_id, "child")],
    ).interactions[0]
    assert snapshot.readiness == "ready"
    assert snapshot.warnings == ("malformed_record",)


def test_root_agent_may_have_any_name() -> None:
    source, records = parse_case(load("subagent_usage"))
    renamed = [
        replace(item, name="invoke_agent support-bot")
        if isinstance(item, Span) and item.name == "invoke_agent Strands Agents"
        else item
        for item in records
    ]
    base = assemble(source, records).interactions[0]
    result = assemble(source, renamed)
    assert len(result.interactions) == 1
    interaction = result.interactions[0]
    assert interaction.readiness == "ready"
    assert interaction.output == base.output
    assert interaction.model_calls == base.model_calls
    assert interaction.tools == base.tools


def test_nested_subagent_tokens_bubble_up_without_internal_tools() -> None:
    source, records = parse_case(load("subagent_usage"))
    nested_agent = next(
        item
        for item in records
        if isinstance(item, Span) and item.name == "invoke_agent subagent"
    )
    chat = next(
        item
        for item in records
        if isinstance(item, Span)
        and item.name == "chat"
        and item.parent_span_id != nested_agent.span_id
    )
    second_agent = replace(
        nested_agent, span_id="nested-second", parent_span_id=nested_agent.span_id
    )
    second_chat = replace(
        chat, span_id="nested-chat", parent_span_id=second_agent.span_id
    )
    base = assemble(source, records).interactions[0]
    nested = assemble(source, [*records, second_agent, second_chat]).interactions[0]
    assert nested.readiness == "ready"
    assert len(nested.model_calls) == len(base.model_calls) + 1
    assert nested.tools == base.tools


@pytest.mark.parametrize(
    "blocks",
    [
        [{"type": "thinking", "text": "private"}],
        [{"text": "visible"}, {"image": {"source": "omitted"}}],
        [{"type": {}, "image": {}}],
        [{"type": [], "image": {}}],
    ],
)
def test_reasoning_or_unsupported_blocks_do_not_become_final_text(
    blocks: list[dict[str, Any]],
) -> None:
    source, records = parse_case(load())
    root = next(
        item
        for item in records
        if isinstance(item, Span) and item.name == "invoke_agent Strands Agents"
    )
    events = copy.deepcopy(root.events)
    events[-1]["attributes"]["message"] = json.dumps(blocks)
    records = [
        replace(item, events=events) if item is root else item for item in records
    ]
    snapshot = assemble(source, records).interactions[0]
    assert snapshot.readiness == "ready"
    assert snapshot.warnings
    assert snapshot.output in {None, "visible"}


@pytest.mark.parametrize(
    "output", ["", "123", "true", "{}", "[]", "[1,2]", '[{"foo":1}]', '{"answer":42}']
)
def test_explicit_empty_or_scalar_text_output_is_preserved(output: str) -> None:
    source, records = parse_case(load())
    root = next(
        item
        for item in records
        if isinstance(item, Span) and item.name == "invoke_agent Strands Agents"
    )
    events = copy.deepcopy(root.events)
    events[-1]["attributes"]["message"] = output
    records = [
        replace(item, events=events) if item is root else item for item in records
    ]
    snapshot = assemble(source, records).interactions[0]
    assert snapshot.readiness == "ready"
    assert snapshot.output == output


def test_malformed_tool_identifiers_are_left_out_without_crash() -> None:
    source, records = parse_case(load("principal_tools"))
    tool = next(
        item
        for item in records
        if isinstance(item, Span) and item.name.startswith("execute_tool ")
    )
    records = [
        replace(item, attributes={**tool.attributes, "gen_ai.tool.call.id": {}})
        if item is tool
        else item
        for item in records
    ]
    snapshot = assemble(source, records).interactions[0]
    assert snapshot.readiness == "ready"
    assert "incomplete_tool" in snapshot.warnings
    assert tool.span_id not in {item.span_id for item in snapshot.tools}


def test_incremental_root_warns_about_its_unread_earlier_history() -> None:
    source, records = parse_case(load("principal_tools"))
    root = next(
        item
        for item in records
        if isinstance(item, Span) and item.name == "invoke_agent Strands Agents"
    )
    chats = [item for item in records if isinstance(item, Span) and item.name == "chat"]
    last_chat = max(chats, key=lambda item: item.start_ns)
    prefix_cutoff = last_chat.start_ns // 1_000_000
    # The most recent complete model call alone cannot reveal earlier missing calls.
    last_only = [
        item
        for item in records
        if not (
            isinstance(item, Span) and item.name == "chat" and item is not last_chat
        )
    ]
    result = assemble(source, last_only, read_start_ms=prefix_cutoff)
    snapshot = result.interactions[0]
    assert snapshot.readiness == "ready"
    assert "unread_trace_history" in snapshot.warnings
    assert (
        assemble(source, records, read_start_ms=root.start_ns // 1_000_000)
        .interactions[0]
        .readiness
        == "ready"
    )


@pytest.mark.parametrize("span_name", ["invoke_agent Strands Agents", "chat"])
def test_terminal_export_supersedes_open_versions_in_any_order(span_name: str) -> None:
    source, records = parse_case(load())
    terminal = next(
        item for item in records if isinstance(item, Span) and item.name == span_name
    )
    provisional = replace(terminal, end_ns=None, status="UNSET", events=())
    older = replace(
        provisional, attributes={}, timestamp_ms=provisional.timestamp_ms - 1
    )
    expected = assemble(source, records).interactions[0]
    for versions in ((older, provisional, terminal), (terminal, provisional, older)):
        evidence = [item for item in records if item is not terminal] + list(versions)
        actual = assemble(source, evidence).interactions[0]
        assert actual.readiness == "ready"
        assert actual.model_calls == expected.model_calls
        assert actual.input == expected.input
        assert actual.output == expected.output


def test_terminal_export_cannot_change_open_operation_identity() -> None:
    source, records = parse_case(load())
    chat = next(
        item for item in records if isinstance(item, Span) and item.name == "chat"
    )
    wrong_operation = replace(chat, end_ns=None, start_ns=chat.start_ns - 1)
    actual = assemble(source, [*records, wrong_operation]).interactions[0]
    assert actual.readiness == "ready"
    assert "conflicting_span" in actual.warnings


def test_optional_cache_counters_may_be_absent_without_losing_model_calls() -> None:
    source, records = parse_case(load("subagent_usage"))
    chat = next(
        item for item in records if isinstance(item, Span) and item.name == "chat"
    )
    attrs = {
        key: value for key, value in chat.attributes.items() if "cache_" not in key
    }
    updated = [
        replace(item, attributes=attrs) if item is chat else item for item in records
    ]
    actual = assemble(source, updated).interactions[0]
    assert actual.readiness == "ready"
    call = next(call for call in actual.model_calls if call.span_id == chat.span_id)
    assert call.usage.input_tokens == attrs["gen_ai.usage.input_tokens"]
    assert call.usage.cache_read_input_tokens == 0
    assert call.usage.cache_write_input_tokens == 0
    assert len(actual.model_calls) == len(
        assemble(source, records).interactions[0].model_calls
    )


@pytest.mark.parametrize("value", [None, -1, True, "0"])
def test_present_but_invalid_cache_counter_is_not_defaulted(value: object) -> None:
    source, records = parse_case(load())
    chat = next(
        item for item in records if isinstance(item, Span) and item.name == "chat"
    )
    attrs = {**chat.attributes, "gen_ai.usage.cache_read_input_tokens": value}
    updated = [
        replace(item, attributes=attrs) if item is chat else item for item in records
    ]
    snapshot = assemble(source, updated).interactions[0]
    assert snapshot.readiness == "ready"
    assert snapshot.warnings
    assert chat.span_id not in {call.span_id for call in snapshot.model_calls}
