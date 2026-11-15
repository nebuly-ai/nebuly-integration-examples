"""Offline end-to-end runs, durable resume and conservative delivery behavior."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import httpx
import pytest

from bedrock_sync.agentcore import sync, user_defined
from bedrock_sync.agentcore.config import AgentCoreConfig
from bedrock_sync.agentcore.records import SourceScope
from bedrock_sync.agentcore.state import AgentCoreState
from tests.agentcore.cases import REAL_CASES, Logs, load

if TYPE_CHECKING:
    from pathlib import Path

CASES = sorted(path.stem for path in REAL_CASES)
DEFAULT_CASE = "principal_tools"


def date(ms: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=ms)


def settings(
    tmp_path: Path,
    case: dict[str, Any],
    *,
    dry_run: bool = False,
    start: int | None = None,
) -> tuple[AgentCoreConfig, datetime]:
    events = case["records"]
    earliest = min(
        min(
            json.loads(event["message"]).get(
                "startTimeUnixNano", event["timestamp"] * 1_000_000
            )
            // 1_000_000,
            event["timestamp"],
        )
        for event in events
    )
    end = max(event["timestamp"] for event in events) + 1000
    args = [
        "--source-id",
        case["source"]["source_id"],
        "--log-group",
        events[0]["logGroupName"],
        "--log-group",
        "aws/spans",
        "--from-date",
        date(start if start is not None else earliest).isoformat(),
        "--state-db",
        str(tmp_path / "state.db"),
    ]
    if dry_run:
        args.append("--dry-run")
    config = AgentCoreConfig.from_env_and_args(
        args,
        env={
            "BEDROCK_ACCOUNT_ID": case["source"]["account_id"],
            "BEDROCK_REGION": case["source"]["region"],
            "NEBULY_API_KEY": "synthetic-key",
            "USER_HASH_SECRET": "synthetic-secret",
        },
    )
    return config, date(end)


def source(config: AgentCoreConfig) -> SourceScope:
    return SourceScope(config.account_id, config.region, config.source_id)


def recorder(
    payloads: list[dict[str, Any]], *, status: int = 200
) -> httpx.MockTransport:
    def respond(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return httpx.Response(status)

    return httpx.MockTransport(respond)


@pytest.mark.parametrize("name", CASES)
def test_complete_run_then_incremental_replay_never_duplicates(
    tmp_path: Path, name: str
) -> None:
    case = load(name)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert first.sent == 1
    assert len(sent) == 1
    assert sent[0]["interaction"]["input"] == case["expected"]["input"]
    assert sent[0]["interaction"]["output"] == (case["expected"]["output"] or "")
    second = sync.run_sync(
        replace(config, from_date=None),
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=now + timedelta(days=1),
    )
    assert second.new_records == 0
    assert second.sent == 0
    assert len(sent) == 1


def test_dry_run_does_not_create_state_or_send(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path / "not-created", case, dry_run=True)
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert report.ready == 1
    assert report.sent == 0
    assert not sent
    assert not (tmp_path / "not-created").exists()


def test_dry_run_preserves_existing_database_bytes_and_delivery_state(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    before = config.state_db.read_bytes()
    files = set(tmp_path.iterdir())
    report = sync.run_sync(
        replace(config, dry_run=True, content_retention_seconds=0),
        logs_client=Logs(case["records"]),
        now=now,
    )
    assert report.sent == 0
    assert report.pruned_records > 0
    assert config.state_db.read_bytes() == before
    assert set(tmp_path.iterdir()) == files


def test_missing_root_is_retained_until_later_delivery(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    without_root = [
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name") != "invoke_agent Strands Agents"
    ]
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config, logs_client=Logs(without_root), transport=recorder(sent), now=now
    )
    assert first.sent == 0
    assert first.blocked == 1
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert len(state.pending_traces()) == 1
    second = sync.run_sync(
        replace(config, from_date=None),
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=now + timedelta(seconds=10),
    )
    assert second.sent == 1
    assert len(sent) == 1


def test_prefix_before_initial_coverage_is_recovered_without_claiming_coverage(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    root = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name") == "invoke_agent Strands Agents"
    )
    config, now = settings(tmp_path, case, start=root["timestamp"])
    logs = Logs(case["records"])
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(config, logs_client=logs, transport=recorder(sent), now=now)
    assert report.sent == 1
    assert any("filterPattern" in request for request in logs.requests)
    expected = next(iter(case["expected"]["usage_by_model"].values()))
    assert sent[0]["traces"][0]["input_tokens"] == expected["input_tokens"]
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert state.coverage is not None
        assert state.coverage[0] == root["timestamp"]


@pytest.mark.parametrize("failure", ["after_normalization", "before_payload"])
def test_crash_between_persistence_stages_is_recoverable(
    tmp_path: Path, failure: str
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    target = (
        "bedrock_sync.agentcore.sync._assemble_candidates"
        if failure == "after_normalization"
        else "bedrock_sync.agentcore.state.AgentCoreState.put_payload"
    )
    with (
        patch(target, side_effect=RuntimeError("simulated crash")),
        pytest.raises(RuntimeError, match="simulated"),
    ):
        sync.run_sync(
            config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
        )
    assert not sent
    report = sync.run_sync(
        config, logs_client=Logs([]), transport=recorder(sent), now=now
    )
    assert report.sent == 1
    assert len(sent) == 1


def test_partial_cloudwatch_failure_does_not_advance_coverage(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    logs = Logs(case["records"])
    original = logs.filter_log_events

    def interrupted(**kwargs: object) -> dict[str, Any]:
        if "nextToken" in kwargs:
            raise PermissionError("lost read access")
        result = original(**kwargs)
        return {"events": result["events"][:2], "nextToken": "more"}

    with (
        patch.object(logs, "filter_log_events", side_effect=interrupted),
        pytest.raises(PermissionError, match="read access"),
    ):
        sync.run_sync(config, logs_client=logs, now=now)
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert state.coverage is None
        assert len(state.records()) == 2
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert report.sent == 1


def test_uncertain_send_is_not_retried_on_next_run(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config,
        logs_client=Logs(case["records"]),
        transport=recorder(sent, status=503),
        now=now,
    )
    assert first.uncertain == 1
    second = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert second.sent == 0
    assert len(sent) == 1


def test_customer_mapping_is_loaded_once_for_multiple_traces(tmp_path: Path) -> None:
    cases = [load(name) for name in CASES]
    combined = {
        **cases[0],
        "records": [event for case in cases for event in case["records"]],
    }
    config, now = settings(tmp_path, combined)
    sent: list[dict[str, Any]] = []
    with patch.object(
        user_defined, "load_context", return_value={"synthetic": True}
    ) as mappings:
        report = sync.run_sync(
            config,
            logs_client=Logs(combined["records"]),
            transport=recorder(sent),
            now=now,
        )
    mappings.assert_called_once_with()
    assert report.sent == len(cases)


def test_bad_customer_enrichment_blocks_then_recovers_without_new_events(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    with patch.object(user_defined, "build_segments", return_value={"team": " "}):
        first = sync.run_sync(
            config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
        )
    assert first.blocked == 1
    assert first.diagnostics["conversion_failed"] == 1
    second = sync.run_sync(
        config, logs_client=Logs([]), transport=recorder(sent), now=now
    )
    assert second.sent == 1


def test_future_to_date_is_capped_and_missing_first_start_fails_before_aws(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    logs = Logs([])
    sync.run_sync(
        replace(config, to_date=now + timedelta(days=2)), logs_client=logs, now=now
    )
    assert (
        max(request["endTime"] for request in logs.requests)
        == int(now.timestamp() * 1000) - 1
    )
    with (
        patch("bedrock_sync.agentcore.sync.create_logs_client") as create,
        pytest.raises(ValueError, match="first-run"),
    ):
        sync.run_sync(
            replace(config, state_db=tmp_path / "other.db", from_date=None), now=now
        )
    create.assert_not_called()


def test_new_usage_after_confirmed_delivery_reports_discrepancy_without_resend(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    extra = dict(
        next(
            event
            for event in case["records"]
            if json.loads(event["message"]).get("name") == "chat"
        )
    )
    message = json.loads(extra["message"])
    message["spanId"] = "f" * 16
    extra["eventId"] += "-new-usage"
    extra["message"] = json.dumps(message)
    report = sync.run_sync(
        config,
        logs_client=Logs([*case["records"], extra]),
        transport=recorder(sent),
        now=now,
    )
    assert report.diagnostics["late_delivery_discrepancy"] == 1
    assert report.sent == 0
    assert len(sent) == 1
    assert report.outbox_statuses == {"sent": 1}


def test_new_malformed_evidence_suspends_previously_pending_payload(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    # Crash before transport creation leaves a staged but unattempted payload.
    with (
        patch(
            "bedrock_sync.agentcore.sync.SafeNebulyClient",
            side_effect=RuntimeError("crash"),
        ),
        pytest.raises(RuntimeError, match="crash"),
    ):
        sync.run_sync(config, logs_client=Logs(case["records"]), now=now)
    bad = dict(
        next(
            event
            for event in case["records"]
            if json.loads(event["message"]).get("name") == "chat"
        )
    )
    message = json.loads(bad["message"])
    message["spanId"] = "f" * 16
    message["attributes"] = []
    bad["eventId"] += "-malformed"
    bad["message"] = json.dumps(message)
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(
        config,
        logs_client=Logs([*case["records"], bad]),
        transport=recorder(sent),
        now=now,
    )
    assert report.blocked == 1
    assert report.sent == 0
    assert not sent
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert len(state.pending_deliveries()) == 1
        assert not state.pending_deliveries(ended_before_ns=2**62)


def test_backfill_does_not_send_pending_interactions_after_selected_end(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    with (
        patch(
            "bedrock_sync.agentcore.sync.SafeNebulyClient",
            side_effect=RuntimeError("crash"),
        ),
        pytest.raises(RuntimeError, match="crash"),
    ):
        sync.run_sync(config, logs_client=Logs(case["records"]), now=now)
    # Retain the later ready/pending payload but perform an earlier bounded backfill.
    assert config.from_date is not None
    earlier_end = config.from_date + timedelta(milliseconds=1)
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(
        replace(config, to_date=earlier_end),
        logs_client=Logs([]),
        transport=recorder(sent),
        now=now,
    )
    assert report.sent == 0
    assert not sent
    assert report.outbox_statuses == {"pending": 1}
    resumed = sync.run_sync(
        config, logs_client=Logs([]), transport=recorder(sent), now=now
    )
    assert resumed.sent == 1


def test_zero_overlap_same_end_can_resume_existing_state(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    report = sync.run_sync(
        replace(config, from_date=None, overlap_seconds=0),
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=now,
    )
    assert report.sent == 0
    assert len(sent) == 1


def test_crash_before_replacing_pending_payload_restages_latest_usage(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    with (
        patch(
            "bedrock_sync.agentcore.sync.SafeNebulyClient",
            side_effect=RuntimeError("before delivery"),
        ),
        pytest.raises(RuntimeError, match="before delivery"),
    ):
        sync.run_sync(config, logs_client=Logs(case["records"]), now=now)
    extra = dict(
        next(
            event
            for event in case["records"]
            if json.loads(event["message"]).get("name") == "chat"
        )
    )
    message = json.loads(extra["message"])
    message["spanId"] = "f" * 16
    extra["eventId"] += "-late-model-call"
    extra["message"] = json.dumps(message)
    with (
        patch.object(AgentCoreState, "put_payload", side_effect=RuntimeError("crash")),
        pytest.raises(RuntimeError, match="crash"),
    ):
        sync.run_sync(config, logs_client=Logs([extra]), now=now)
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(
        config, logs_client=Logs([]), transport=recorder(sent), now=now
    )
    assert report.sent == 1
    expected = next(iter(case["expected"]["usage_by_model"].values()))
    assert sent[0]["traces"][0]["output_tokens"] == (
        expected["output_tokens"] + message["attributes"]["gen_ai.usage.output_tokens"]
    )


def test_routine_log_inside_a_trace_is_counted_once(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    event = dict(case["records"][0])
    message = json.loads(event["message"])
    message.pop("startTimeUnixNano", None)
    message["scope"] = {"name": "unrelated.runtime.logger"}
    message["body"] = {"message": "Routine diagnostic"}
    event["message"] = json.dumps(message)
    event["eventId"] += "-routine"
    report = sync.run_sync(
        config,
        logs_client=Logs([*case["records"], event]),
        transport=recorder([]),
        now=now,
    )
    assert report.sent == 1
    assert report.diagnostics["unsupported_log"] == 1


def test_unrelated_otel_log_does_not_create_permanent_pending_trace(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    event = dict(case["records"][0])
    message = json.loads(event["message"])
    message.pop("startTimeUnixNano", None)
    message["scope"] = {"name": "unrelated.runtime.logger"}
    message["traceId"] = "f" * 32
    message["body"] = {"message": "Routine diagnostic"}
    event["message"] = json.dumps(message)
    report = sync.run_sync(config, logs_client=Logs([event]), now=now)
    assert report.blocked == 0
    assert report.diagnostics["unsupported_log"] == 1
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.pending_traces()
        assert not state.records(unprocessed_only=True)


def test_open_span_can_be_completed_by_a_later_export(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    root = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name") == "invoke_agent Strands Agents"
    )
    open_root = dict(root)
    message = json.loads(root["message"])
    del message["endTimeUnixNano"]
    message["status"] = {"code": "UNSET"}
    message["events"] = message["events"][:1]
    open_root["eventId"] += "-open"
    open_root["message"] = json.dumps(message)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config,
        logs_client=Logs(
            [event for event in case["records"] if event is not root] + [open_root]
        ),
        transport=recorder(sent),
        now=now,
    )
    assert first.open == 1
    assert not sent
    second = sync.run_sync(
        config, logs_client=Logs([root]), transport=recorder(sent), now=now
    )
    assert second.sent == 1
    assert sent[0]["interaction"]["output"] == case["expected"]["output"]
    expected = next(iter(case["expected"]["usage_by_model"].values()))
    assert sent[0]["traces"][0]["output_tokens"] == expected["output_tokens"]


def test_nonfinite_model_attributes_are_blocked_without_poisoning_the_run(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    bad = dict(
        next(
            event
            for event in case["records"]
            if json.loads(event["message"]).get("name") == "chat"
        )
    )
    message = json.loads(bad["message"])
    message["spanId"] = "f" * 16
    message["attributes"]["invalid"] = float("nan")
    bad["eventId"] += "-nonfinite"
    bad["message"] = json.dumps(message)
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(
        config,
        logs_client=Logs([*case["records"], bad]),
        transport=recorder(sent),
        now=now,
    )
    assert report.blocked == 1
    assert report.diagnostics["malformed_record"] >= 1
    assert not sent
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.records(unprocessed_only=True)
        assert len(state.pending_traces()) == 1


def test_blocked_trace_does_not_repeat_historical_prefix_queries(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    root = next(
        e
        for e in case["records"]
        if json.loads(e["message"]).get("name") == "invoke_agent Strands Agents"
    )
    missing = next(
        e
        for e in case["records"]
        if json.loads(e["message"]).get("name", "").startswith("execute_tool ")
    )
    incomplete = [e for e in case["records"] if e is not missing]
    config, now = settings(tmp_path, case, start=root["timestamp"])
    first_logs = Logs(incomplete)
    first = sync.run_sync(config, logs_client=first_logs, now=now)
    assert first.blocked == 1
    assert any("filterPattern" in request for request in first_logs.requests)
    for hours in (1, 2, 48):
        logs = Logs(incomplete)
        report = sync.run_sync(
            replace(config, from_date=None, reconciliation_seconds=0),
            logs_client=logs,
            now=now + timedelta(hours=hours),
        )
        assert report.blocked == 1
        assert not any("filterPattern" in request for request in logs.requests)
    # An explicit backfill can recover a late child outside the bounded horizon.
    sent: list[dict[str, Any]] = []
    completed = sync.run_sync(
        replace(config, from_date=date(min(e["timestamp"] for e in case["records"]))),
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=now + timedelta(hours=49),
    )
    assert completed.sent == 1


def test_hourly_runs_do_not_repeat_daily_reconciliation(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    for hour in (1, 2, 23):
        logs = Logs([])
        sync.run_sync(
            replace(config, from_date=None),
            logs_client=logs,
            now=now + timedelta(hours=hour),
        )
        expected = now + timedelta(
            hours=0 if hour == 1 else 1 if hour == 2 else 2,
            seconds=-config.overlap_seconds,
        )
        assert config.from_date is not None
        expected = max(config.from_date, expected)
        assert min(r["startTime"] for r in logs.requests) == int(
            expected.timestamp() * 1000
        )
    logs = Logs([])
    sync.run_sync(
        replace(config, from_date=None), logs_client=logs, now=now + timedelta(hours=24)
    )
    assert min(r["startTime"] for r in logs.requests) < int(now.timestamp() * 1000)


def test_failed_prefix_query_is_retried_without_claiming_recovery(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    root = next(
        e
        for e in case["records"]
        if json.loads(e["message"]).get("name") == "invoke_agent Strands Agents"
    )
    config, now = settings(tmp_path, case, start=root["timestamp"])
    logs = Logs(case["records"])
    original = logs.filter_log_events

    def fail(**kwargs: object) -> dict[str, Any]:
        if "filterPattern" in kwargs:
            raise PermissionError("prefix unavailable")
        return original(**kwargs)

    with (
        patch.object(logs, "filter_log_events", side_effect=fail),
        pytest.raises(PermissionError),
    ):
        sync.run_sync(config, logs_client=logs, now=now)
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert state.recovered_start(json.loads(root["message"])["traceId"]) is None
    restored = Logs(case["records"])
    sent: list[dict[str, Any]] = []
    assert (
        sync.run_sync(
            config, logs_client=restored, transport=recorder(sent), now=now
        ).sent
        == 1
    )
    assert any("filterPattern" in request for request in restored.requests)


def test_compacted_sent_trace_never_resends_on_replay_or_late_span(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    config = replace(config, content_retention_seconds=0)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert first.sent == 1
    assert first.pruned_records > 0
    replay = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert replay.new_records == 0
    assert replay.sent == 0
    extra = dict(
        next(
            e for e in case["records"] if json.loads(e["message"]).get("name") == "chat"
        )
    )
    body = json.loads(extra["message"])
    body["spanId"] = "f" * 16
    extra["eventId"] += "-late-after-compaction"
    extra["message"] = json.dumps(body)
    late = sync.run_sync(
        config, logs_client=Logs([extra]), transport=recorder(sent), now=now
    )
    assert late.sent == 0
    assert late.diagnostics["late_record_for_compacted_trace"] == 1
    assert len(sent) == 1
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.records()
        assert not state.pending_traces()


def test_changed_namespace_recovers_previously_unrecognized_identity_log(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    for e in case["records"]:
        body = json.loads(e["message"])
        if body.get("scope", {}).get("name") == "bedrock_agentcore.memory.client":
            value = json.loads(body["body"])
            value["message"] = value["message"].replace("/actors/", "/facts/")
            body["body"] = json.dumps(value)
            e["message"] = json.dumps(body)
    config, now = settings(tmp_path, case)
    # Blocked traces stay for the reconciliation plus the retention (8 days).
    now += timedelta(days=7)
    first = sync.run_sync(config, logs_client=Logs(case["records"]), now=now)
    assert first.blocked == 1
    sent: list[dict[str, Any]] = []
    second = sync.run_sync(
        replace(config, actor_namespace_template="/facts/{actorId}/"),
        logs_client=Logs([]),
        transport=recorder(sent),
        now=now,
    )
    assert second.sent == 1
    assert sent[0]["interaction"]["conversation_id"] == case["expected"]["session_id"]


def test_blocked_trace_expires_after_reconciliation_and_retention(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    case["records"] = [
        e
        for e in case["records"]
        if json.loads(e["message"]).get("scope", {}).get("name")
        != "bedrock_agentcore.memory.client"
    ]
    config, now = settings(tmp_path, case)
    config = replace(config, actor_id_attribute="missing.attribute")
    first = sync.run_sync(config, logs_client=Logs(case["records"]), now=now)
    assert first.blocked == 1
    assert first.diagnostics["missing_actor"] == 1
    late = sync.run_sync(
        config, logs_client=Logs([]), now=now + timedelta(days=8, seconds=1)
    )
    assert late.diagnostics["expired_trace"] == 1
    assert late.pruned_records > 0
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.pending_traces()
        assert not state.records()


def test_scheduled_reconciliation_finds_late_child_without_full_prefix_rescan(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    missing = next(
        e
        for e in case["records"]
        if json.loads(e["message"]).get("name", "").startswith("execute_tool ")
    )
    incomplete = [e for e in case["records"] if e is not missing]
    # Coverage ends an hour after the trace, so the overlap cannot find the
    # missing record again: only the reconciliation reads that far back.
    coverage_end = now + timedelta(hours=1)
    first = sync.run_sync(config, logs_client=Logs(incomplete), now=coverage_end)
    assert first.blocked == 1
    logs = Logs(case["records"])
    sent: list[dict[str, Any]] = []
    result = sync.run_sync(
        replace(config, from_date=None),
        logs_client=logs,
        transport=recorder(sent),
        now=coverage_end + timedelta(days=1),
    )
    assert result.sent == 1
    overlap_start_ms = int(
        (coverage_end - timedelta(seconds=config.overlap_seconds)).timestamp() * 1000
    )
    assert min(request["startTime"] for request in logs.requests) < overlap_start_ms
    assert not any("filterPattern" in request for request in logs.requests)


def test_retryable_delivery_is_retained_and_sent_on_a_later_run(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config,
        logs_client=Logs(case["records"]),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(429, headers={"Retry-After": "120"})
        ),
        now=now,
    )
    assert first.retryable == 1
    assert first.outbox_statuses == {"pending": 1}
    early = sync.run_sync(
        config,
        logs_client=Logs([]),
        transport=recorder(sent),
        now=now + timedelta(seconds=60),
    )
    assert early.sent == 0
    assert not sent
    later = sync.run_sync(
        config,
        logs_client=Logs([]),
        transport=recorder(sent),
        now=now + timedelta(seconds=120),
    )
    assert later.sent == 1
    assert len(sent) == 1


def _retraced(
    case: dict[str, Any], trace_id: str, shift_ms: int, actor: str
) -> list[dict[str, Any]]:
    """The records of ``case`` as another trace of the same session: new trace
    id, started ``shift_ms`` later, by another actor."""
    old_trace, old_actor = case["expected"]["trace_id"], case["expected"]["actor_id"]
    events = []
    for event in case["records"]:
        message = json.loads(event["message"].replace(old_actor, actor))
        message["traceId"] = trace_id
        for key in ("startTimeUnixNano", "endTimeUnixNano", "timeUnixNano"):
            if key in message:
                message[key] = int(message[key]) + shift_ms * 1_000_000
        assert message.get("traceId") != old_trace
        events.append(
            {
                **event,
                "eventId": event["eventId"] + "-retraced",
                "timestamp": event["timestamp"] + shift_ms,
                "ingestionTime": event["ingestionTime"] + shift_ms,
                "message": json.dumps(message),
            }
        )
    return events


def test_session_goes_to_the_actor_of_the_oldest_trace_staged(tmp_path: Path) -> None:
    """Two ready traces of one session by different actors in the same run: the
    newer one has the lower trace id, so only the ordering by start time gives
    the session to the actor who started it."""
    case = load(DEFAULT_CASE)
    newer = _retraced(case, "0" * 31 + "1", 60_000, "another-actor")
    both = {**case, "records": [*case["records"], *newer]}
    config, now = settings(tmp_path, both)
    sent: list[dict[str, Any]] = []

    report = sync.run_sync(
        config, logs_client=Logs(both["records"]), transport=recorder(sent), now=now
    )

    assert [p["interaction"]["conversation_id"] for p in sent] == [
        case["expected"]["session_id"]
    ]
    assert report.diagnostics["conversation_user_conflict"] == 1
    assert (report.sent, report.dropped) == (1, 1)
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        dropped = state.trace("0" * 31 + "1")
        assert dropped is not None
        assert dropped.status == "dropped"
        assert all(t.trace_id != dropped.trace_id for t in state.pending_traces())

    # A dropped trace is not assembled again on the next run.
    again = sync.run_sync(
        config,
        logs_client=Logs(both["records"]),
        transport=recorder(sent),
        now=now + timedelta(minutes=10),
    )
    assert "conversation_user_conflict" not in again.diagnostics
    assert len(sent) == 1
