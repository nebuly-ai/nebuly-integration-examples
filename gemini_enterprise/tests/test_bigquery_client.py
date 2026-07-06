from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

from gemini_enterprise_sync.bigquery_client import BigQueryLoggingClient

_LOG_ENTRY = json.loads(
    (Path(__file__).parent / "data" / "log_entry.json").read_text(encoding="utf-8")
)


def _row(
    *,
    insert_id: str,
    minute: int,
    trace: str | None = "projects/p/traces/trace-abc",
) -> dict[str, object]:
    return {
        "timestamp": datetime(2026, 7, 2, 16, minute, 0, tzinfo=UTC),
        "insertId": insert_id,
        "trace": trace,
        "jsonPayload": _LOG_ENTRY,
    }


def _client_with_rows(rows: list[dict[str, object]]) -> MagicMock:
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = rows
    return mock_client


def test_fetch_batch_parses_rows() -> None:
    client = BigQueryLoggingClient(
        "p",
        "p.ds.table",
        client=_client_with_rows([_row(insert_id="a", minute=1)]),
    )

    records = client.fetch_batch(
        since=datetime(2026, 7, 2, 0, 0, tzinfo=UTC),
        until=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
        limit=10,
    )

    assert len(records) == 1
    assert records[0].insert_id == "a"
    assert records[0].trace_id == "trace-abc"
    assert records[0].timestamp == datetime(2026, 7, 2, 16, 1, 0, tzinfo=UTC)
    assert records[0].payload["serviceTextReply"] == _LOG_ENTRY["serviceTextReply"]


def test_fetch_batch_honors_limit() -> None:
    rows = [_row(insert_id=f"id-{i}", minute=i) for i in range(5)]
    client = BigQueryLoggingClient(
        "p",
        "p.ds.table",
        client=_client_with_rows(rows),
    )

    records = client.fetch_batch(
        since=None,
        until=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
        limit=3,
    )

    assert len(records) == 3
    assert records[-1].insert_id == "id-2"


def test_wildcard_table_adds_suffix_params() -> None:
    mock_client = _client_with_rows([])
    client = BigQueryLoggingClient(
        "p",
        "p.ds.table_*",
        client=mock_client,
    )

    client.fetch_batch(
        since=datetime(2026, 7, 2, 0, 0, tzinfo=UTC),
        until=datetime(2026, 7, 6, 0, 0, tzinfo=UTC),
        limit=10,
    )

    sql = mock_client.query.call_args.args[0]
    job_config = mock_client.query.call_args.kwargs["job_config"]
    param_names = {p.name for p in job_config.query_parameters}

    assert "_TABLE_SUFFIX" in sql
    assert param_names == {"until", "since", "limit", "sfx_start", "sfx_end"}
    params = {p.name: p.value for p in job_config.query_parameters}
    assert params["sfx_start"] == "20260702"
    assert params["sfx_end"] == "20260706"


def test_plain_table_omits_suffix_params() -> None:
    mock_client = _client_with_rows([])
    client = BigQueryLoggingClient(
        "p",
        "p.ds.table",
        client=mock_client,
    )

    client.fetch_batch(
        since=datetime(2026, 7, 2, 0, 0, tzinfo=UTC),
        until=datetime(2026, 7, 6, 0, 0, tzinfo=UTC),
        limit=10,
    )

    sql = mock_client.query.call_args.args[0]
    job_config = mock_client.query.call_args.kwargs["job_config"]
    param_names = {p.name for p in job_config.query_parameters}

    assert "_TABLE_SUFFIX" not in sql
    assert param_names == {"until", "since", "limit"}
