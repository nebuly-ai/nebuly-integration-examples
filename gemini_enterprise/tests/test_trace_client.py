from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from gemini_enterprise_sync.trace_client import TraceClient, TraceData
from google.api_core.exceptions import NotFound


def _span(
    labels: dict[str, str],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> MagicMock:
    span = MagicMock()
    span.labels = labels
    span.start_time = start
    span.end_time = end
    return span


def _trace(spans: list[MagicMock]) -> MagicMock:
    trace = MagicMock()
    trace.spans = spans
    return trace


def test_fetch_tokens_sums_labels_and_omits_not_found() -> None:
    client = MagicMock()
    trace_client = TraceClient("project", max_workers=2, client=client)
    span_start = datetime(2026, 7, 2, 16, 14, 27, 554361, tzinfo=UTC)
    span_end = datetime(2026, 7, 2, 16, 14, 38, 513741, tzinfo=UTC)

    def get_trace(*, project_id: str, trace_id: str) -> MagicMock:
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
    result = trace_client.fetch_tokens({"t1", "t2", "missing"})

    assert result == {
        "t1": TraceData(
            input_tokens=125,
            output_tokens=25,
            time_start=span_start,
            time_end=span_end,
        ),
        "t2": TraceData(input_tokens=10, output_tokens=0),
    }
    assert "missing" not in result


def test_fetch_tokens_uses_thread_pool_width() -> None:
    client = MagicMock()
    client.get_trace.return_value = _trace([])
    trace_client = TraceClient("project", max_workers=3, client=client)

    with patch(
        "gemini_enterprise_sync.trace_client.ThreadPoolExecutor",
        wraps=ThreadPoolExecutor,
    ) as pool_cls:
        trace_client.fetch_tokens({f"id{i}" for i in range(5)})
        pool_cls.assert_called_once_with(max_workers=3)
