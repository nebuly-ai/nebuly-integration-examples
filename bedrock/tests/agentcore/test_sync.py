"""Offline end-to-end runs, durable resume and conservative delivery behavior."""

from __future__ import annotations

import json
import logging
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
from bedrock_sync.shared.errors import ConfigError
from tests.agentcore.cases import REAL_CASES, Logs, load, snapshot

if TYPE_CHECKING:
    from pathlib import Path

    from bedrock_sync.agentcore.models import InteractionSnapshot

CASES = REAL_CASES
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


def test_server_error_is_sent_again_by_a_later_run(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config,
        logs_client=Logs(case["records"]),
        transport=recorder(sent, status=503),
        now=now,
    )
    assert first.retryable == 1
    same_minute = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert same_minute.sent == 0
    assert same_minute.delivery_waiting_for_retry
    assert same_minute.incomplete
    later = sync.run_sync(
        config,
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=now + timedelta(minutes=2),
    )
    assert later.sent == 1
    assert len(sent) == 2
    assert sent[0] == sent[1]


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
    assert first.incomplete
    second = sync.run_sync(
        config, logs_client=Logs([]), transport=recorder(sent), now=now
    )
    assert second.sent == 1


def test_any_hook_error_blocks_only_its_trace_and_is_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    with (
        patch.object(user_defined, "build_tags", side_effect=KeyError("secret")),
        caplog.at_level(logging.DEBUG, logger="bedrock_sync.agentcore.sync"),
    ):
        report = sync.run_sync(
            config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
        )
    assert report.blocked == 1
    assert report.incomplete
    assert sent == []
    assert "KeyError" in caplog.text
    assert "stack without message" in caplog.text
    assert "secret" not in caplog.text


def test_unserializable_hook_result_does_not_stop_other_traces(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    other = load("text_cache_read")
    records = [*case["records"], *other["records"]]
    config, now = settings(tmp_path, {**case, "records": records})
    bad_trace = snapshot(case)[1].trace_id

    def customize(
        interaction: InteractionSnapshot, traces: list[dict[str, Any]], _: object
    ) -> list[dict[str, Any]]:
        return [{"bad": object()}] if interaction.trace_id == bad_trace else traces

    with patch.object(user_defined, "customize_traces", side_effect=customize):
        report = sync.run_sync(
            config, logs_client=Logs(records), transport=recorder([]), now=now
        )
    assert (report.sent, report.blocked) == (1, 1)
    assert report.diagnostics["conversion_failed"] == 1


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
        pytest.raises(
            ConfigError, match="Cannot plan the CloudWatch read: --from-date"
        ),
    ):
        sync.run_sync(
            replace(config, state_db=tmp_path / "other.db", from_date=None), now=now
        )
    create.assert_not_called()


def test_new_usage_after_confirmed_delivery_is_sent_again_as_an_update(
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
    logs = [*case["records"], extra]
    report = sync.run_sync(
        config, logs_client=Logs(logs), transport=recorder(sent), now=now
    )
    # Nebuly may still be processing the first POST: wait a minute.
    assert report.diagnostics["updated_after_delivery"] == 1
    assert report.sent == 0
    assert report.outbox_statuses == {"pending": 1}
    later = sync.run_sync(
        config,
        logs_client=Logs(logs),
        transport=recorder(sent),
        now=now + timedelta(minutes=2),
    )
    assert later.sent == 1
    first, update = sent
    for name in ("input", "end_user", "time_start", "conversation_id"):
        assert update["interaction"][name] == first["interaction"][name]
    assert update["traces"][0]["output_tokens"] > first["traces"][0]["output_tokens"]
    assert later.outbox_statuses == {"sent": 1}


def test_new_malformed_evidence_is_a_warning_on_a_pending_payload(
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
    assert report.blocked == 0
    assert report.sent == 1
    assert report.diagnostics["malformed_record"] >= 1


def test_malformed_trace_id_does_not_block_sync_or_replay(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    bad = dict(case["records"][0])
    message = json.loads(bad["message"])
    message["traceId"] = "\ud800"
    bad["eventId"] += "-invalid-trace-id"
    bad["message"] = json.dumps(message)
    logs = [*case["records"], bad]
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config, logs_client=Logs(logs), transport=recorder(sent), now=now
    )
    assert first.diagnostics["malformed_record"] == 1
    assert first.sent == 1
    second = sync.run_sync(
        replace(config, from_date=None),
        logs_client=Logs(logs),
        transport=recorder(sent),
        now=now + timedelta(seconds=1),
    )
    assert second.sent == 0
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.records(unprocessed_only=True)


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


def test_to_date_before_the_resume_point_reads_nothing(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert config.from_date is not None
    report = sync.run_sync(
        replace(config, from_date=None, to_date=config.from_date),
        logs_client=Logs([]),
        transport=recorder(sent),
        now=now,
    )
    assert not report.incomplete
    assert report.sent == 0
    assert len(sent) == 1


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


def test_nonfinite_model_attributes_are_a_warning_without_poisoning_the_run(
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
    assert report.blocked == 0
    assert report.diagnostics["malformed_record"] >= 1
    assert report.degraded == {"malformed_record": 1}
    assert len(sent) == 1
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.records(unprocessed_only=True)
        assert not state.pending_traces()


def test_blocked_trace_does_not_repeat_historical_prefix_queries(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    root = next(
        e
        for e in case["records"]
        if json.loads(e["message"]).get("name") == "invoke_agent Strands Agents"
    )
    # Without the Memory log the trace has no actor: blocked, not sent.
    incomplete = [
        e
        for e in case["records"]
        if json.loads(e["message"])["scope"]["name"]
        != "bedrock_agentcore.memory.client"
    ]
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


def test_late_correction_survives_old_delivery_retention_on_conversion_failure(
    tmp_path: Path,
) -> None:
    case = load("principal_tools")
    config, now = settings(tmp_path, case)
    config = replace(config, content_retention_seconds=60)
    tool = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name", "").startswith("execute_tool ")
    )
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config,
        logs_client=Logs([event for event in case["records"] if event is not tool]),
        transport=recorder(sent),
        now=now,
    )
    assert first.sent == 1
    config = replace(config, from_date=None)
    with patch.object(sync, "snapshot_to_payload", side_effect=ValueError):
        failed = sync.run_sync(
            config,
            logs_client=Logs([tool]),
            transport=recorder(sent),
            now=now + timedelta(seconds=61),
        )
    assert failed.diagnostics["conversion_failed"] == 1
    assert failed.pruned_records == 0
    recovered = sync.run_sync(
        config,
        logs_client=Logs([]),
        transport=recorder(sent),
        now=now + timedelta(seconds=62),
    )
    assert recovered.sent == 1
    assert len(sent) == 2
    assert sent[0] != sent[1]


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
        replace(config, actor_namespace_templates=("/facts/{actorId}/",)),
        logs_client=Logs([]),
        transport=recorder(sent),
        now=now,
    )
    assert second.sent == 1
    assert sent[0]["interaction"]["conversation_id"] == case["expected"]["session_id"]


def test_history_retention_forgets_the_trace_and_a_replay_resends_it(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    config = replace(config, content_retention_seconds=0, history_retention_seconds=0)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    assert (first.sent, first.forgotten_traces) == (1, 1)
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        db = state._db
        for table in ("traces", "record_receipts", "trace_outbox", "outbox"):
            assert db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0  # noqa: S608
    replay = sync.run_sync(
        config, logs_client=Logs(case["records"]), transport=recorder(sent), now=now
    )
    # Same input, user and time_start: Nebuly updates the same interaction.
    assert replay.sent == 1
    assert sent[1] == sent[0]


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
    assert late.expired == 1
    assert late.expired_reasons == {"missing_actor": 1}
    assert late.incomplete
    assert late.pruned_records > 0
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.pending_traces()
        assert not state.records()


@pytest.mark.parametrize("actionable", [False, True])
def test_rootless_expiration_fails_only_with_agent_evidence(
    tmp_path: Path, *, actionable: bool
) -> None:
    case = load(DEFAULT_CASE)
    name = "chat" if actionable else "POST /invocations"
    event = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name") == name
    )
    config, now = settings(tmp_path, {**case, "records": [event]})
    first = sync.run_sync(config, logs_client=Logs([event]), now=now)
    assert first.blocked == 1
    later = sync.run_sync(
        replace(config, from_date=None),
        logs_client=Logs([]),
        now=now + timedelta(days=8, seconds=1),
    )
    assert later.incomplete == actionable
    assert (
        later.diagnostics["expired_trace" if actionable else "ignored_rootless_trace"]
        == 1
    )
    assert later.expired_reasons == ({"missing_root": 1} if actionable else {})
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert not state.pending_traces()
        assert not state.records()


def test_other_harness_payload_is_not_persisted(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    other = dict(case["records"][0])
    message = json.loads(other["message"])
    message["resource"]["attributes"]["cloud.resource_id"] = "foreign-harness"
    message["private_marker"] = "foreign-body"
    other["message"] = json.dumps(message)
    other["eventId"] += "-foreign"
    report = sync.run_sync(
        config,
        logs_client=Logs([*case["records"], other]),
        transport=recorder([]),
        now=now,
    )
    assert report.sent == 1
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        assert all("foreign-body" not in record.message for record in state.records())


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
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config,
        logs_client=Logs(incomplete),
        transport=recorder(sent),
        now=coverage_end,
    )
    assert first.sent == 1
    assert first.degraded == {"missing_tool_span": 1}
    logs = Logs(case["records"])
    result = sync.run_sync(
        replace(config, from_date=None),
        logs_client=logs,
        transport=recorder(sent),
        now=coverage_end + timedelta(days=1),
    )
    assert result.sent == 1
    assert result.diagnostics["updated_after_delivery"] == 1
    assert len(sent[1]["traces"]) == len(sent[0]["traces"]) + 1
    overlap_start_ms = int(
        (coverage_end - timedelta(seconds=config.overlap_seconds)).timestamp() * 1000
    )
    assert min(request["startTime"] for request in logs.requests) < overlap_start_ms
    assert not any("filterPattern" in request for request in logs.requests)


def test_hourly_checkpoint_does_not_shorten_daily_reconciliation(
    tmp_path: Path,
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    tool = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name", "").startswith("execute_tool ")
    )
    first_end = now + timedelta(hours=2)
    posts: list[dict[str, Any]] = []
    sync.run_sync(
        config,
        logs_client=Logs([event for event in case["records"] if event is not tool]),
        transport=recorder(posts),
        now=first_end,
    )
    config = replace(config, from_date=None)
    for hour in (1, 23):
        report = sync.run_sync(
            config,
            logs_client=Logs([tool]),
            transport=recorder(posts),
            now=first_end + timedelta(hours=hour),
        )
        assert report.new_records == 0
    report = sync.run_sync(
        config,
        logs_client=Logs([tool]),
        transport=recorder(posts),
        now=first_end + timedelta(hours=27),
    )
    assert report.diagnostics["updated_after_delivery"] == 1
    assert len(posts) == 2
    assert len(posts[1]["traces"]) == len(posts[0]["traces"]) + 1


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


def test_session_with_several_actors_sends_both_and_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Nebuly keys a conversation by id and end user: two actors in one
    session are two conversations there, so both are sent, with a warning."""
    case = load(DEFAULT_CASE)
    newer = _retraced(case, "0" * 31 + "1", 60_000, "another-actor")
    both = {**case, "records": [*case["records"], *newer]}
    config, now = settings(tmp_path, both)
    sent: list[dict[str, Any]] = []

    with caplog.at_level(logging.WARNING):
        report = sync.run_sync(
            config,
            logs_client=Logs(both["records"]),
            transport=recorder(sent),
            now=now,
        )

    assert [p["interaction"]["conversation_id"] for p in sent] == [
        case["expected"]["session_id"]
    ] * 2
    assert len({p["interaction"]["end_user"] for p in sent}) == 2
    assert report.sessions_with_several_actors == 1
    assert caplog.text.count("several actors") == 1
    assert not report.incomplete


def test_changed_match_values_after_delivery_keep_the_sent_interaction() -> None:
    """Nebuly matches input, end user and start time: a payload that changes one
    of them would add a copy, so it is not sent again."""
    sent = {"interaction": {"input": "a", "end_user": "u", "time_start": "t"}}
    for changed in ({"input": "b"}, {"end_user": "v"}, {"time_start": "s"}):
        payload = {"interaction": {**sent["interaction"], **changed}}
        assert not sync._same_nebuly_interaction(sent, payload)
    assert not sync._same_nebuly_interaction({}, sent)
    assert sync._same_nebuly_interaction(
        sent, {"interaction": {**sent["interaction"], "output": "new"}}
    )


@pytest.mark.parametrize("initial_status", [200, 503])
def test_late_pending_payload_keeps_the_first_post_key(
    tmp_path: Path, initial_status: int
) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    tool = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name", "").startswith("execute_tool ")
    )
    posts: list[dict[str, Any]] = []
    sync.run_sync(
        config,
        logs_client=Logs([event for event in case["records"] if event is not tool]),
        transport=recorder(posts, status=initial_status),
        now=now,
    )
    config = replace(config, from_date=None)
    correction = sync.run_sync(
        config,
        logs_client=Logs([tool]),
        transport=recorder(posts),
        now=now + timedelta(seconds=1),
    )
    assert correction.outbox_statuses == {"pending": 1}
    assert len(posts) == 1
    retried = sync.run_sync(
        replace(config, pseudonymize_end_user=True),
        logs_client=Logs([]),
        transport=recorder(posts),
        now=now + timedelta(minutes=2),
    )
    assert retried.sent == 1
    assert retried.diagnostics["late_delivery_discrepancy"] == 1
    assert sync._same_nebuly_interaction(posts[0], posts[1])
    assert len(posts[1]["traces"]) == len(posts[0]["traces"]) + 1


def test_crash_before_requeue_retries_sent_trace_correction(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    tool = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name", "").startswith("execute_tool ")
    )
    posts: list[dict[str, Any]] = []
    sync.run_sync(
        config,
        logs_client=Logs([event for event in case["records"] if event is not tool]),
        transport=recorder(posts),
        now=now,
    )
    config = replace(config, from_date=None)
    with (
        patch.object(
            AgentCoreState, "requeue_payload", side_effect=RuntimeError("crash")
        ),
        pytest.raises(RuntimeError, match="crash"),
    ):
        sync.run_sync(
            config,
            logs_client=Logs([tool]),
            transport=recorder(posts),
            now=now + timedelta(seconds=1),
        )
    resumed = sync.run_sync(
        config,
        logs_client=Logs([]),
        transport=recorder(posts),
        now=now + timedelta(minutes=2),
    )
    assert resumed.sent == 1
    assert len(posts) == 2
    assert len(posts[1]["traces"]) == len(posts[0]["traces"]) + 1


def test_retry_given_up_only_replays_the_retained_post(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    tool = next(
        event
        for event in case["records"]
        if json.loads(event["message"]).get("name", "").startswith("execute_tool ")
    )
    incomplete = [event for event in case["records"] if event is not tool]
    posts: list[dict[str, Any]] = []
    for clock in (now, now + timedelta(days=7)):
        report = sync.run_sync(
            config,
            logs_client=Logs(incomplete),
            transport=recorder(posts, status=503),
            now=clock,
        )
    assert report.outbox_statuses == {"rejected": 1}
    _, item = snapshot(case)
    late = sync.run_sync(
        config,
        logs_client=Logs([tool]),
        transport=recorder(posts),
        now=now + timedelta(days=7, minutes=1),
    )
    assert late.outbox_statuses == {"rejected": 1}
    assert len(posts) == 2
    with AgentCoreState(config.state_db, source(config), config.log_groups) as state:
        state.put_payload("unrelated-trace", "unrelated-root", posts[0])
    with patch.object(
        sync, "create_logs_client", side_effect=AssertionError("No AWS reads")
    ):
        recovered = sync.run_sync(
            replace(config, from_date=None, retry_given_up=item.interaction_key),
            transport=recorder(posts),
            now=now + timedelta(days=7, minutes=2),
        )
    assert recovered.sent == 1
    assert recovered.outbox_statuses == {"sent": 1, "pending": 1}
    assert len(posts) == 3
    assert posts[0] == posts[2]


def _sub_agent_exported_ms(case: dict[str, Any]) -> int:
    """When the first sub-agent span reached CloudWatch, before its caller."""
    return min(
        int(event["timestamp"])
        for event in case["records"]
        if json.loads(event["message"]).get("name", "").startswith("invoke_agent ")
        and json.loads(event["message"])["name"] != "invoke_agent Strands Agents"
    )


def _without_http_parent(
    case: dict[str, Any], *, orphan_root: bool = False
) -> list[dict[str, Any]]:
    """The records without POST /invocations; ``orphan_root`` also clears the
    root's parent, as for an agent run outside AgentCore Runtime."""
    events = []
    for event in case["records"]:
        message = json.loads(event["message"])
        if message.get("name") == "POST /invocations":
            continue
        if orphan_root and message.get("name") == "invoke_agent Strands Agents":
            message.pop("parentSpanId", None)
            events.append({**event, "message": json.dumps(message)})
            continue
        events.append(event)
    return events


def test_sub_agent_seen_before_its_caller_waits_and_only_the_turn_is_sent(
    tmp_path: Path,
) -> None:
    """A run between the sub-agent's end and its caller's must not send the
    sub-agent as the turn: Nebuly would get two interactions for one turn."""
    case = load("subagent_usage")
    config, end = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    early = sync.run_sync(
        config,
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=date(_sub_agent_exported_ms(case) + 1),
    )
    assert (early.sent, early.open) == (0, 1)
    assert early.diagnostics["waiting_for_parent"] == 1
    later = sync.run_sync(
        replace(config, from_date=None),
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=end,
    )
    assert later.sent == 1
    assert [p["interaction"]["input"] for p in sent] == [case["expected"]["input"]]


def test_root_without_its_http_parent_is_sent_after_the_wait(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    records = _without_http_parent(case)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        config, logs_client=Logs(records), transport=recorder(sent), now=now
    )
    assert (first.sent, first.open) == (0, 1)
    later = sync.run_sync(
        replace(config, from_date=None),
        logs_client=Logs(records),
        transport=recorder(sent),
        now=now + timedelta(seconds=config.root_wait_seconds + 60),
    )
    assert later.sent == 1
    assert later.degraded["missing_parent"] == 1
    assert sent[0]["interaction"]["input"] == case["expected"]["input"]


def test_to_date_before_the_wait_keeps_the_root_open_days_later(
    tmp_path: Path,
) -> None:
    """The parent wait is measured against --to-date, not the wall clock."""
    case = load(DEFAULT_CASE)
    records = _without_http_parent(case)
    config, end = settings(tmp_path, case)
    config = replace(config, to_date=end)
    sent: list[dict[str, Any]] = []
    for days in (2, 3):
        report = sync.run_sync(
            config,
            logs_client=Logs(records),
            transport=recorder(sent),
            now=end + timedelta(days=days),
        )
        assert (report.open, report.sent) == (1, 0)
        assert report.diagnostics["waiting_for_parent"] == 1
    assert sent == []


@pytest.mark.parametrize("later_to_date", [None, "past_the_wait"])
def test_later_horizon_past_the_wait_sends_with_missing_parent(
    tmp_path: Path, later_to_date: str | None
) -> None:
    case = load(DEFAULT_CASE)
    records = _without_http_parent(case)
    config, end = settings(tmp_path, case)
    now = end + timedelta(days=2)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        replace(config, to_date=end),
        logs_client=Logs(records),
        transport=recorder(sent),
        now=now,
    )
    assert (first.open, first.sent) == (1, 0)
    horizon = (
        end + timedelta(seconds=config.root_wait_seconds + 60)
        if later_to_date
        else None
    )
    later = sync.run_sync(
        replace(config, from_date=None, to_date=horizon),
        logs_client=Logs(records),
        transport=recorder(sent),
        now=now,
    )
    assert (later.open, later.sent) == (0, 1)
    assert later.degraded["missing_parent"] == 1
    assert [p["interaction"]["input"] for p in sent] == [case["expected"]["input"]]


def test_backfill_split_between_sub_agent_and_caller_sends_one_turn(
    tmp_path: Path,
) -> None:
    """A chunk ending after the sub-agent but before its caller must not send
    the sub-agent as the turn, even when the wall clock is days later."""
    case = load("subagent_usage")
    config, end = settings(tmp_path, case)
    split = date(_sub_agent_exported_ms(case) + 1)
    now = end + timedelta(days=2)
    sent: list[dict[str, Any]] = []
    first = sync.run_sync(
        replace(config, to_date=split),
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=now,
    )
    assert (first.sent, first.open) == (0, 1)
    assert first.diagnostics["waiting_for_parent"] == 1
    second = sync.run_sync(
        replace(config, from_date=split, to_date=end),
        logs_client=Logs(case["records"]),
        transport=recorder(sent),
        now=now,
    )
    assert second.sent == 1
    assert "sub_agent_sent_as_turn" not in second.diagnostics
    assert [p["interaction"]["input"] for p in sent] == [case["expected"]["input"]]


def test_root_without_a_parent_is_sent_at_once(tmp_path: Path) -> None:
    case = load(DEFAULT_CASE)
    config, now = settings(tmp_path, case)
    sent: list[dict[str, Any]] = []
    report = sync.run_sync(
        config,
        logs_client=Logs(_without_http_parent(case, orphan_root=True)),
        transport=recorder(sent),
        now=now,
    )
    assert report.sent == 1
    assert "waiting_for_parent" not in report.diagnostics


@pytest.mark.parametrize("first_status", [200, 503])
def test_sub_agent_sent_after_the_wait_is_followed_by_the_turn_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, first_status: int
) -> None:
    """A caller later than --root-wait-seconds: the sub-agent went out as the
    turn. The turn is sent too, with a warning; a sub-agent payload still
    waiting for a retry is dropped, not sent again."""
    case = load("subagent_usage")
    config, end = settings(tmp_path, case)
    config = replace(config, root_wait_seconds=0)
    sent: list[dict[str, Any]] = []
    sync.run_sync(
        config,
        logs_client=Logs(case["records"]),
        transport=recorder(sent, status=first_status),
        now=date(_sub_agent_exported_ms(case) + 1),
    )
    assert len(sent) == 1
    assert sent[0]["interaction"]["input"] != case["expected"]["input"]

    with caplog.at_level(logging.WARNING):
        later = sync.run_sync(
            replace(config, from_date=None),
            logs_client=Logs(case["records"]),
            transport=recorder(sent),
            now=end + timedelta(minutes=5),
        )

    assert len(sent) == 2
    assert sent[1]["interaction"]["input"] == case["expected"]["input"]
    assert later.diagnostics["sub_agent_sent_as_turn"] == 1
    assert "sub-agent was sent as the turn" in caplog.text
    assert later.outbox_statuses == {"sent": 1 if first_status == 503 else 2}
