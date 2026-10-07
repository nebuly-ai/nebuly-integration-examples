from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import MagicMock

import httpx
import pytest
from langfuse_sync.http_retry import (
    RateLimiter,
    parse_retry_after,
    should_retry,
    wait_retry_after_or_jitter,
)
from tenacity import RetryCallState


def _status_error(status: int, headers: dict[str, str] | None = None) -> Exception:
    return httpx.HTTPStatusError(
        "error",
        request=httpx.Request("GET", "https://example.com"),
        response=httpx.Response(status, headers=headers),
    )


def _retry_state(exc: BaseException, attempt: int = 1) -> RetryCallState:
    state = RetryCallState(retry_object=MagicMock(), fn=None, args=(), kwargs={})
    state.attempt_number = attempt
    state.set_exception((type(exc), exc, None))
    return state


def test_parse_retry_after_seconds() -> None:
    assert parse_retry_after("12") == 12.0
    assert parse_retry_after("0.5") == 0.5


def test_parse_retry_after_http_date() -> None:
    value = format_datetime(datetime.now(UTC) + timedelta(seconds=30), usegmt=True)
    parsed = parse_retry_after(value)
    assert parsed is not None
    assert 25 <= parsed <= 30


def test_parse_retry_after_past_date_and_negative_clamp_to_zero() -> None:
    past = format_datetime(datetime.now(UTC) - timedelta(seconds=30), usegmt=True)
    assert parse_retry_after(past) == 0.0
    assert parse_retry_after("-5") == 0.0


@pytest.mark.parametrize("value", [None, "", "soon", "nan", "inf"])
def test_parse_retry_after_garbage_returns_none(value: str | None) -> None:
    assert parse_retry_after(value) is None


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (httpx.ConnectError("boom"), True),
        (_status_error(429), True),
        (_status_error(500), True),
        (_status_error(503), True),
        (_status_error(400), False),
        (_status_error(404), False),
        (ValueError("nope"), False),
    ],
)
def test_should_retry(exc: BaseException, *, expected: bool) -> None:
    assert should_retry(exc) is expected


def test_wait_uses_retry_after_with_small_jitter() -> None:
    state = _retry_state(_status_error(429, {"Retry-After": "7"}))
    for _ in range(20):
        assert 7.0 <= wait_retry_after_or_jitter(state) <= 8.0


def test_wait_caps_retry_after() -> None:
    state = _retry_state(_status_error(429, {"Retry-After": "100000"}))
    assert wait_retry_after_or_jitter(state) == 120.0


def test_wait_without_retry_after_stays_within_exponential_bounds() -> None:
    state = _retry_state(_status_error(500), attempt=10)
    for _ in range(50):
        assert 0.0 <= wait_retry_after_or_jitter(state) <= 60.0


def test_pause_delays_slot() -> None:
    async def run() -> float:
        limiter = RateLimiter(2)
        loop = asyncio.get_running_loop()
        limiter.pause(0.05)
        started = loop.time()
        async with limiter.slot():
            return loop.time() - started

    assert asyncio.run(run()) >= 0.045


def test_slot_bounds_concurrency() -> None:
    async def run() -> int:
        limiter = RateLimiter(2)
        in_flight = 0
        peak = 0

        async def work() -> None:
            nonlocal in_flight, peak
            async with limiter.slot():
                in_flight += 1
                peak = max(peak, in_flight)
                await asyncio.sleep(0.01)
                in_flight -= 1

        await asyncio.gather(*(work() for _ in range(8)))
        return peak

    assert asyncio.run(run()) == 2
