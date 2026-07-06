from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from gemini_enterprise_sync.config import Config
from gemini_enterprise_sync.coverage import Coverage
from gemini_enterprise_sync.logging_client import LogRecord
from gemini_enterprise_sync.sync import (
    FirstRunRequiresFromDateError,
    GapAbortedError,
    run_sync,
)
from gemini_enterprise_sync.trace_client import TraceData

_ENGINE_ID = "gemini-enterprise-17828149_1782814981868"


def _config(
    tmp_path: Path,
    *,
    from_date: datetime | None = None,
    to_date: datetime | None = datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
    dry_run: bool = False,
    force: bool = False,
) -> Config:
    return Config(
        nebuly_api_key="key",
        nebuly_endpoint="https://example.com/trace",
        gcp_project_id="p",
        gcp_location="eu",
        gcp_collection="default_collection",
        gcp_engine_id=_ENGINE_ID,
        settle_lag_seconds=60,
        log_batch_size=2,
        log_page_size=1000,
        trace_concurrency=4,
        anonymize=False,
        from_date=from_date,
        to_date=to_date,
        cache_dir=tmp_path,
        dry_run=dry_run,
        verbose=False,
        force=force,
        log_source="logging",
        bigquery_table=None,
        bigquery_location=None,
    )


def _record(insert_id: str, minute: int) -> LogRecord:
    payload = json.loads(
        (Path(__file__).parent / "data" / "log_entry.json").read_text(encoding="utf-8")
    )
    return LogRecord(
        timestamp=datetime(2026, 7, 2, 16, minute, 0, tzinfo=UTC),
        insert_id=insert_id,
        trace_id=f"trace-{insert_id}",
        payload=payload,
    )


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.LoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_run_sync_drains_batches_and_persists_coverage(
    http_cls: MagicMock,
    logging_cls: MagicMock,
    trace_cls: MagicMock,
    nebuly_cls: MagicMock,
    tmp_path: Path,
) -> None:
    batch_one = [_record("a", 1), _record("b", 2)]
    batch_two = [_record("c", 3)]
    logging = MagicMock()
    logging.fetch_batch.side_effect = [batch_one, batch_two, []]
    logging_cls.return_value = logging

    trace = MagicMock()
    trace.fetch_traces.return_value = {
        "trace-a": TraceData(10, 5),
        "trace-b": TraceData(20, 6),
        "trace-c": TraceData(30, 7),
    }
    trace_cls.return_value = trace

    nebuly = MagicMock()
    nebuly_cls.return_value = nebuly

    summary = run_sync(
        _config(tmp_path, from_date=datetime(2026, 7, 2, 0, 0, tzinfo=UTC))
    )

    assert summary.totals.entries_fetched == 3
    assert summary.totals.entries_sent == 3
    assert nebuly.send_interaction.call_count == 3

    state = Coverage(tmp_path).load()
    assert state.coverage_until == datetime(2026, 7, 2, 16, 3, 0, tzinfo=UTC)


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.LoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_run_sync_send_failure_stops_without_advancing_failed_entry(
    http_cls: MagicMock,
    logging_cls: MagicMock,
    trace_cls: MagicMock,
    nebuly_cls: MagicMock,
    tmp_path: Path,
) -> None:
    records = [_record("a", 1), _record("b", 2)]
    logging = MagicMock()
    logging.fetch_batch.side_effect = [records, []]
    logging_cls.return_value = logging
    trace_cls.return_value = MagicMock(fetch_traces=MagicMock(return_value={}))
    nebuly = MagicMock()
    nebuly.send_interaction.side_effect = [None, RuntimeError("send failed")]
    nebuly_cls.return_value = nebuly

    summary = run_sync(
        _config(tmp_path, from_date=datetime(2026, 7, 2, 0, 0, tzinfo=UTC))
    )

    assert summary.totals.entries_sent == 1
    assert summary.totals.entries_failed == 1
    state = Coverage(tmp_path).load()
    assert state.coverage_until == datetime(2026, 7, 2, 16, 1, 0, tzinfo=UTC)


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.LoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_run_sync_skip_advances_coverage(
    http_cls: MagicMock,
    logging_cls: MagicMock,
    trace_cls: MagicMock,
    nebuly_cls: MagicMock,
    tmp_path: Path,
) -> None:
    skipped = _record("skip", 1)
    skipped.payload["serviceTextReply"] = ""
    records = [skipped, _record("ok", 2)]
    logging = MagicMock()
    logging.fetch_batch.side_effect = [records, []]
    logging_cls.return_value = logging
    trace_cls.return_value = MagicMock(fetch_traces=MagicMock(return_value={}))
    nebuly = MagicMock()
    nebuly_cls.return_value = nebuly

    summary = run_sync(
        _config(tmp_path, from_date=datetime(2026, 7, 2, 0, 0, tzinfo=UTC))
    )

    assert summary.totals.entries_skipped == 1
    assert summary.totals.entries_sent == 1
    state = Coverage(tmp_path).load()
    assert state.coverage_until == datetime(2026, 7, 2, 16, 2, 0, tzinfo=UTC)


def test_first_run_without_from_date_raises(tmp_path: Path) -> None:
    with pytest.raises(FirstRunRequiresFromDateError):
        run_sync(_config(tmp_path, from_date=None))


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.LoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_backfill_with_existing_coverage_does_not_crash(
    http_cls: MagicMock,
    logging_cls: MagicMock,
    trace_cls: MagicMock,
    nebuly_cls: MagicMock,
    tmp_path: Path,
) -> None:
    Coverage(tmp_path).save(
        coverage_from=datetime(2026, 7, 2, 8, 0, tzinfo=UTC),
        coverage_until=datetime(2026, 7, 2, 16, 12, 0, tzinfo=UTC),
    )

    records = [_record("earlier", 1)]
    logging = MagicMock()
    logging.fetch_batch.side_effect = [records, []]
    logging_cls.return_value = logging
    trace_cls.return_value = MagicMock(fetch_traces=MagicMock(return_value={}))
    nebuly_cls.return_value = MagicMock()

    summary = run_sync(
        _config(
            tmp_path,
            from_date=datetime(2026, 7, 2, 0, 0, tzinfo=UTC),
            to_date=datetime(2026, 7, 2, 16, 8, 0, tzinfo=UTC),
        )
    )

    assert summary.totals.entries_sent == 1


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.LoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_gap_aborts_without_force(
    http_cls: MagicMock,
    logging_cls: MagicMock,
    trace_cls: MagicMock,
    nebuly_cls: MagicMock,
    tmp_path: Path,
) -> None:
    Coverage(tmp_path).save(
        coverage_from=datetime(2026, 7, 2, 8, 0, tzinfo=UTC),
        coverage_until=datetime(2026, 7, 2, 16, 12, 0, tzinfo=UTC),
    )

    with (
        patch("gemini_enterprise_sync.sync.sys.stdin.isatty", return_value=False),
        pytest.raises(GapAbortedError),
    ):
        run_sync(
            _config(
                tmp_path,
                from_date=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
            )
        )


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.LoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_gap_with_force_invalidates_and_syncs(
    http_cls: MagicMock,
    logging_cls: MagicMock,
    trace_cls: MagicMock,
    nebuly_cls: MagicMock,
    tmp_path: Path,
) -> None:
    Coverage(tmp_path).save(
        coverage_from=datetime(2026, 7, 2, 8, 0, tzinfo=UTC),
        coverage_until=datetime(2026, 7, 2, 16, 12, 0, tzinfo=UTC),
    )

    records = [_record("future", 5)]
    logging = MagicMock()
    logging.fetch_batch.side_effect = [records, []]
    logging_cls.return_value = logging
    trace_cls.return_value = MagicMock(fetch_traces=MagicMock(return_value={}))
    nebuly_cls.return_value = MagicMock()

    summary = run_sync(
        _config(
            tmp_path,
            from_date=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
            force=True,
        )
    )

    assert summary.totals.entries_sent == 1
    state = Coverage(tmp_path).load()
    assert state.coverage_until == datetime(2026, 7, 2, 16, 5, 0, tzinfo=UTC)


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.BigQueryLoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_run_sync_uses_bigquery_client_when_configured(
    http_cls: MagicMock,
    bq_cls: MagicMock,
    trace_cls: MagicMock,
    nebuly_cls: MagicMock,
    tmp_path: Path,
) -> None:
    bq = MagicMock()
    bq.fetch_batch.return_value = []
    bq_cls.return_value = bq
    trace_cls.return_value = MagicMock(fetch_traces=MagicMock(return_value={}))
    nebuly_cls.return_value = MagicMock()

    config = _config(
        tmp_path,
        from_date=datetime(2026, 7, 2, 0, 0, tzinfo=UTC),
    )
    object.__setattr__(config, "log_source", "bigquery")
    object.__setattr__(config, "bigquery_table", "p.ds.table_*")

    run_sync(config)

    bq_cls.assert_called_once_with("p", "p.ds.table_*", location=None)
