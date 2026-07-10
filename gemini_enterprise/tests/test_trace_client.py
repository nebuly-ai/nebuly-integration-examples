from __future__ import annotations

import asyncio as aio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

from gemini_enterprise_sync.trace_client import TraceClient, TraceData, _parse_trace
from google.api_core.exceptions import NotFound

if TYPE_CHECKING:
    import pytest

_DEFAULT_START = datetime(2026, 7, 2, 16, 14, 27, 554361, tzinfo=UTC)
_DEFAULT_END = datetime(2026, 7, 2, 16, 14, 38, 513741, tzinfo=UTC)


def _span(
    labels: dict[str, str],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> MagicMock:
    span = MagicMock()
    span.labels = labels
    span.start_time = start if start is not None else _DEFAULT_START
    span.end_time = end if end is not None else _DEFAULT_END
    return span


def _trace(spans: list[MagicMock]) -> MagicMock:
    trace = MagicMock()
    trace.spans = spans
    return trace


def test_fetch_traces_sums_labels_and_omits_not_found() -> None:
    client = AsyncMock()
    transport = AsyncMock()
    client.transport = transport
    trace_client = TraceClient("project", concurrency=2, async_client=client)
    span_start = datetime(2026, 7, 2, 16, 14, 27, 554361, tzinfo=UTC)
    span_end = datetime(2026, 7, 2, 16, 14, 38, 513741, tzinfo=UTC)

    async def get_trace(*, project_id: str, trace_id: str) -> MagicMock:
        if trace_id == "missing":
            raise NotFound("trace not found")  # type: ignore[no-untyped-call]
        if trace_id == "t1":
            return _trace(
                [
                    _span(
                        {
                            "gen_ai.usage.input_tokens": "100",
                            "gen_ai.usage.output_tokens": "20",
                        },
                        start=span_start,
                        end=span_end,
                    ),
                    _span(
                        {
                            "gen_ai.usage.input_tokens": "25",
                            "gen_ai.usage.output_tokens": "5",
                        }
                    ),
                ]
            )
        return _trace([_span({"gen_ai.usage.input_tokens": "10"})])

    client.get_trace.side_effect = get_trace
    result = trace_client.fetch_traces({"t1", "t2", "missing"})

    assert result == {
        "t1": TraceData(
            input_tokens=125,
            output_tokens=25,
            time_start=span_start,
            time_end=span_end,
        ),
        "t2": TraceData(
            input_tokens=10,
            output_tokens=None,
            time_start=_DEFAULT_START,
            time_end=_DEFAULT_END,
        ),
    }
    assert "missing" not in result


def test_fetch_traces_uses_asyncio_run() -> None:
    client = AsyncMock()
    transport = AsyncMock()
    client.transport = transport
    client.get_trace.return_value = _trace([])
    trace_client = TraceClient("project", concurrency=3, async_client=client)

    with patch(
        "gemini_enterprise_sync.trace_client.asyncio.run",
        wraps=aio.run,
    ) as run_mock:
        trace_client.fetch_traces({f"id{i}" for i in range(5)})
        run_mock.assert_called_once()


def test_fetch_traces_refreshes_credentials_once_before_gather() -> None:
    creds = MagicMock()
    creds.valid = False
    client = AsyncMock()
    client.transport = AsyncMock()
    client.get_trace.return_value = _trace([])

    with (
        patch(
            "gemini_enterprise_sync.trace_client.google.auth.default",
            return_value=(creds, None),
        ),
        patch(
            "gemini_enterprise_sync.trace_client.TraceServiceAsyncClient",
            return_value=client,
        ) as client_cls,
        patch(
            "gemini_enterprise_sync.trace_client.google.auth.transport.requests.Request"
        ),
        patch.object(creds, "refresh") as refresh_mock,
    ):
        trace_client = TraceClient("project", concurrency=32)
        trace_client.fetch_traces({f"id{i}" for i in range(10)})

    refresh_mock.assert_called_once()
    client_cls.assert_called_once_with(credentials=creds)


def test_fetch_traces_skips_refresh_when_credentials_valid() -> None:
    creds = MagicMock()
    creds.valid = True
    client = AsyncMock()
    client.transport = AsyncMock()
    client.get_trace.return_value = _trace([])

    with (
        patch(
            "gemini_enterprise_sync.trace_client.google.auth.default",
            return_value=(creds, None),
        ),
        patch(
            "gemini_enterprise_sync.trace_client.TraceServiceAsyncClient",
            return_value=client,
        ),
        patch.object(creds, "refresh") as refresh_mock,
    ):
        trace_client = TraceClient("project", concurrency=32)
        trace_client.fetch_traces({"id1"})

    refresh_mock.assert_not_called()


def test_parse_trace_sums_valid_labels_and_skips_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    trace = _trace(
        [
            _span({"gen_ai.usage.input_tokens": "not-a-number"}),
            _span(
                {
                    "gen_ai.usage.input_tokens": "25",
                    "gen_ai.usage.output_tokens": "5",
                }
            ),
        ]
    )

    result = _parse_trace(trace, "trace-bad-label")

    assert result == TraceData(
        input_tokens=25,
        output_tokens=5,
        time_start=_DEFAULT_START,
        time_end=_DEFAULT_END,
    )
    assert any(
        "gen_ai.usage.input_tokens" in record.message
        and "not-a-number" in record.message
        and "trace-bad-label" in record.message
        for record in caplog.records
    )


def test_parse_trace_malformed_only_label_leaves_tokens_none(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    trace = _trace([_span({"gen_ai.usage.input_tokens": "bad"})])

    result = _parse_trace(trace, "trace-only-bad")

    assert result == TraceData(
        input_tokens=None,
        output_tokens=None,
        time_start=_DEFAULT_START,
        time_end=_DEFAULT_END,
    )
    assert any("trace-only-bad" in record.message for record in caplog.records)


def test_fetch_traces_tolerates_malformed_token_labels() -> None:
    client = AsyncMock()
    transport = AsyncMock()
    client.transport = transport
    trace_client = TraceClient("project", concurrency=2, async_client=client)
    client.get_trace.return_value = _trace(
        [
            _span({"gen_ai.usage.input_tokens": "oops"}),
            _span({"gen_ai.usage.input_tokens": "10"}),
        ]
    )

    result = trace_client.fetch_traces({"t1"})

    assert result == {
        "t1": TraceData(
            input_tokens=10,
            output_tokens=None,
            time_start=_DEFAULT_START,
            time_end=_DEFAULT_END,
        )
    }
