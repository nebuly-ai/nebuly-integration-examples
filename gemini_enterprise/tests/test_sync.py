from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from gemini_enterprise_sync.config import Config
from gemini_enterprise_sync.cursor import Cursor
from gemini_enterprise_sync.logging_client import LogRecord
from gemini_enterprise_sync.sync import run_sync
from gemini_enterprise_sync.trace_client import TraceData

_ENGINE_ID = "gemini-enterprise-17828149_1782814981868"


def _config(tmp_path: Path) -> Config:
    return Config(
        nebuly_api_key="key",
        nebuly_endpoint="https://example.com/trace",
        gcp_project_id="p",
        gcp_location="eu",
        gcp_collection="default_collection",
        gcp_engine_id=_ENGINE_ID,
        settle_lag_seconds=60,
        log_batch_size=2,
        trace_max_workers=4,
        anonymize=False,
        from_date=None,
        to_date=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
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
def test_run_sync_drains_batches_and_persists_cursor(
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
    trace.fetch_tokens.return_value = {
        "trace-a": TraceData(10, 5),
        "trace-b": TraceData(20, 6),
        "trace-c": TraceData(30, 7),
    }
    trace_cls.return_value = trace

    nebuly = MagicMock()
    nebuly_cls.return_value = nebuly

    summary = run_sync(_config(tmp_path))

    assert summary.totals.entries_fetched == 3
    assert summary.totals.entries_sent == 3
    assert nebuly.send_interaction.call_count == 3

    cursor = Cursor(tmp_path / "cursor.json").load()
    assert cursor.last_insert_id == "c"
    assert cursor.last_timestamp == datetime(2026, 7, 2, 16, 3, 0, tzinfo=UTC)


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
    trace_cls.return_value = MagicMock(fetch_tokens=MagicMock(return_value={}))
    nebuly = MagicMock()
    nebuly.send_interaction.side_effect = [None, RuntimeError("send failed")]
    nebuly_cls.return_value = nebuly

    summary = run_sync(_config(tmp_path))

    assert summary.totals.entries_sent == 1
    assert summary.totals.entries_failed == 1
    cursor = Cursor(tmp_path / "cursor.json").load()
    assert cursor.last_insert_id == "a"


@patch("gemini_enterprise_sync.sync.NebulyClient")
@patch("gemini_enterprise_sync.sync.TraceClient")
@patch("gemini_enterprise_sync.sync.LoggingClient")
@patch("gemini_enterprise_sync.sync.httpx.Client")
def test_run_sync_skip_advances_cursor(
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
    trace_cls.return_value = MagicMock(fetch_tokens=MagicMock(return_value={}))
    nebuly = MagicMock()
    nebuly_cls.return_value = nebuly

    summary = run_sync(_config(tmp_path))

    assert summary.totals.entries_skipped == 1
    assert summary.totals.entries_sent == 1
    cursor = Cursor(tmp_path / "cursor.json").load()
    assert cursor.last_insert_id == "ok"
