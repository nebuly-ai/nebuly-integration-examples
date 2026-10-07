from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from langfuse_sync.config import Config
from langfuse_sync.http_retry import RateLimiter
from langfuse_sync.langfuse_client import LangfuseClient

_START = datetime(2026, 7, 2, 10, tzinfo=UTC)
_END = datetime(2026, 7, 2, 11, tzinfo=UTC)


def _config() -> Config:
    return Config(
        langfuse_public_key="pub",
        langfuse_secret_key="sec",
        langfuse_base_url="https://langfuse.example.com",
        nebuly_api_key="neb",
        nebuly_endpoint="https://example.com/events",
        anonymize=False,
        settle_lag_seconds=900,
        from_date=None,
        to_date=None,
        cache_dir=Path(),
        dry_run=False,
        verbose=False,
        force=False,
    )


def _page(page: int, total_pages: int) -> dict[str, object]:
    return {
        "data": [{"id": f"p{page}-{i}", "traceId": f"t{page}"} for i in range(2)],
        "meta": {"totalPages": total_pages},
    }


async def _fetch_traces(
    handler: httpx.MockTransport, max_concurrency: int = 2
) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(transport=handler) as http:
        client = LangfuseClient(http, _config(), RateLimiter(max_concurrency))
        return cast("list[dict[str, Any]]", await client.get_traces(_START, _END))


def test_pages_are_returned_in_order_within_concurrency_limit() -> None:
    in_flight = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01 * (4 - int(request.url.params["page"])))
        in_flight -= 1
        return httpx.Response(200, json=_page(int(request.url.params["page"]), 3))

    traces = asyncio.run(_fetch_traces(httpx.MockTransport(handler)))

    assert [t["id"] for t in traces] == [
        "p1-0",
        "p1-1",
        "p2-0",
        "p2-1",
        "p3-0",
        "p3-1",
    ]
    assert peak == 2


def test_observations_are_grouped_by_trace_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_page(int(request.url.params["page"]), 2))

    async def run() -> dict[str, list[dict[str, Any]]]:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as http:
            client = LangfuseClient(http, _config(), RateLimiter(2))
            grouped = await client.get_observations_by_trace_id(_START, _END)
            return cast("dict[str, list[dict[str, Any]]]", grouped)

    grouped = asyncio.run(run())

    assert sorted(grouped) == ["t1", "t2"]
    assert [o["id"] for o in grouped["t2"]] == ["p2-0", "p2-1"]


def test_429_with_retry_after_is_retried_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("langfuse_sync.http_retry.random.uniform", lambda *_: 0.0)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json=_page(1, 1))

    traces = asyncio.run(_fetch_traces(httpx.MockTransport(handler)))

    assert calls == 2
    assert len(traces) == 2


def test_client_errors_are_not_retried() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_fetch_traces(httpx.MockTransport(handler)))

    assert calls == 1
