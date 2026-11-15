"""Cross-module contracts against the sanitized real CloudWatch corpus."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict
from typing import TYPE_CHECKING

import pytest

from bedrock_sync.agentcore.assembler import assemble
from bedrock_sync.agentcore.cloudwatch_client import CloudWatchLogReader
from bedrock_sync.agentcore.config import AgentCoreConfig
from bedrock_sync.agentcore.parser import parse_record
from bedrock_sync.agentcore.reading import plan_trace_recovery
from bedrock_sync.agentcore.records import SourceScope
from bedrock_sync.agentcore.state import AgentCoreState, StateError
from tests.agentcore.cases import FIXTURES, REAL_CASES, Logs, records_of

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("path", REAL_CASES, ids=lambda path: path.stem)
def test_cloudwatch_to_durable_assembly_survives_restart(
    path: Path, tmp_path: Path
) -> None:
    case = json.loads(path.read_text())
    config = AgentCoreConfig.from_env_and_args(
        [
            "--source-id",
            case["source"]["source_id"],
            "--log-group",
            case["records"][0]["logGroupName"],
            "--log-group",
            "aws/spans",
            "--dry-run",
        ],
        env={
            "BEDROCK_ACCOUNT_ID": case["source"]["account_id"],
            "BEDROCK_REGION": case["source"]["region"],
            "SEND_PLAIN_END_USER": "true",
        },
    )
    source = SourceScope(config.account_id, config.region, config.source_id)
    events = case["records"]
    groups = config.log_groups
    reader = CloudWatchLogReader(Logs(empty_first_page=True, events=events), groups)
    start = min(event["timestamp"] for event in events)
    end = max(event["timestamp"] for event in events) + 1
    db = tmp_path / "agentcore.db"
    with AgentCoreState(db, source, groups) as state:
        window = state.begin_window(start, end)
        for page in reader.iter_pages(start, end):
            state.ingest_page(window, page)
            assert state.coverage is None
        state.complete_window(window)
        assert state.coverage == (start, end)
    with AgentCoreState(db, source, groups) as state:
        # A replay after restart never duplicates usage or raw events.
        window = state.begin_window(start, end)
        for page in reader.iter_pages(start, end):
            assert state.ingest_page(window, page) == 0
        state.complete_window(window)
        assert len(state.records()) == len(events)
        parsed = [parse_record(record, source) for record in state.records()]
        result = assemble(
            source,
            records_of(parsed),
            parse_diagnostics=[
                diagnostic for item in parsed for diagnostic in item.diagnostics
            ],
        )
        assert len(result.interactions) == 1
        interaction = result.interactions[0]
        expected = case["expected"]
        assert interaction.readiness == "ready"
        assert interaction.actor_id == expected["actor_id"]
        assert interaction.session_id == expected["session_id"]
        assert interaction.input == expected["input"]
        assert interaction.output == expected["output"]
        assert interaction.terminal_status == expected["terminal_status"]
        totals: dict[str, Counter[str]] = {}
        for call in interaction.model_calls:
            totals.setdefault(call.model_id, Counter()).update(asdict(call.usage))
        assert totals == expected["usage_by_model"]
        assert {tool.span_id for tool in interaction.tools} == {
            tool["span_id"]
            for tool in expected["tools"]
            if not tool["internal_to_subagent"]
        }
        # Placeholder payload isolates persistence from conversion.
        key = state.put_payload(
            interaction.trace_id, interaction.root_span_id, {"synthetic": True}
        )
        assert key == interaction.interaction_key
        state.begin_attempt(key)
    with AgentCoreState(db, source, groups) as state:
        delivery = state.find_delivery(key)
        assert delivery is not None
        assert delivery.status == "uncertain"
        assert not state.pending_deliveries()
        with pytest.raises(StateError):
            state.begin_attempt(key)


def test_long_trace_recovers_earlier_children_without_claiming_old_coverage(
    tmp_path: Path,
) -> None:
    case = json.loads((FIXTURES / "principal_tools.json").read_text())
    source = SourceScope(**case["source"])
    events = case["records"]
    root_event = next(
        event
        for event in events
        if json.loads(event["message"]).get("spanId")
        == case["expected"]["root_span_id"]
        and json.loads(event["message"]).get("name") == "invoke_agent Strands Agents"
    )
    start = root_event["timestamp"]
    end = max(event["timestamp"] for event in events) + 1
    group = events[0]["logGroupName"]
    reader = CloudWatchLogReader(Logs(empty_first_page=True, events=events), (group,))
    db = tmp_path / "pending.db"
    with AgentCoreState(db, source, (group,)) as state:
        window = state.begin_window(start, end)
        for page in reader.iter_pages(start, end):
            state.ingest_page(window, page)
        state.complete_window(window)
        parsed = [parse_record(raw, source) for raw in state.records()]
        snapshot = assemble(
            source,
            records_of(parsed),
        ).interactions[0]
        assert snapshot.readiness == "blocked"
        assert snapshot.earliest_timestamp_ms < start
        state.save_trace(
            snapshot.trace_id,
            snapshot.readiness,
            snapshot.earliest_timestamp_ms,
            asdict(snapshot),
        )
    with AgentCoreState(db, source, (group,)) as state:
        pending = state.pending_traces()
        assert len(pending) == 1
        coverage = state.coverage
        assert coverage is not None
        recoveries = plan_trace_recovery(
            [(trace.trace_id, trace.earliest_ms) for trace in pending],
            read_start_ms=coverage[0],
            end_ms=end + 1,
        )
        assert recoveries
        for recovery in recoveries:
            for page in reader.iter_pages(
                recovery.start_ms, recovery.end_ms, trace_id=recovery.trace_id
            ):
                window = state.begin_window(page.start_ms, page.end_ms)
                with pytest.raises(StateError):
                    state.ingest_page(window, page)
                state.ingest_recovery_page(page)
        assert state.coverage == (start, end)
        for raw in state.records(unprocessed_only=True):
            result = parse_record(raw, source)
            record = result.span or result.identity
            state.mark_processed(raw, trace_id=record.trace_id if record else None)
        trace_records = state.records(trace_id=case["expected"]["trace_id"])
        assert len(trace_records) == len(events)
        parsed = [parse_record(raw, source) for raw in trace_records]
        snapshot = assemble(
            source,
            records_of(parsed),
        ).interactions[0]
        assert snapshot.readiness == "ready"
        assert len(snapshot.model_calls) == len(
            case["expected"]["canonical_model_span_ids"]
        )
        state.save_trace(
            snapshot.trace_id,
            snapshot.readiness,
            snapshot.earliest_timestamp_ms,
            asdict(snapshot),
        )
        assert not state.pending_traces()
        assert not state.records(unprocessed_only=True)
