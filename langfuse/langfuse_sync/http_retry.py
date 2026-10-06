from __future__ import annotations

import asyncio
import logging
import math
import random
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from tenacity import RetryCallState

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 10
_MAX_RETRY_AFTER_S = 120.0
_RETRY_AFTER_JITTER_S = 1.0

_exponential_jitter = wait_random_exponential(multiplier=1, max=60)


def should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, httpx.HTTPStatusError):
        return False
    status = exc.response.status_code
    return status == 429 or status >= 500


def parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header (delta-seconds or HTTP-date) into seconds."""
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - datetime.now(UTC)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(seconds, 0.0)


class RateLimiter:
    """Bounds in-flight requests and shares a cooldown across all workers."""

    def __init__(self, max_concurrency: int) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._resume_at = 0.0

    def pause(self, seconds: float) -> None:
        resume_at = asyncio.get_running_loop().time() + seconds
        self._resume_at = max(self._resume_at, resume_at)

    def _cooldown_remaining(self) -> float:
        return self._resume_at - asyncio.get_running_loop().time()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        while True:
            remaining = self._cooldown_remaining()
            if remaining > 0:
                await asyncio.sleep(remaining)
                continue
            await self._semaphore.acquire()
            if self._cooldown_remaining() <= 0:
                break
            self._semaphore.release()
        try:
            yield
        finally:
            self._semaphore.release()


def wait_retry_after_or_jitter(retry_state: RetryCallState) -> float:
    outcome = retry_state.outcome
    exc = outcome.exception() if outcome is not None else None
    if isinstance(exc, httpx.HTTPStatusError):
        retry_after = parse_retry_after(exc.response.headers.get("Retry-After"))
        if retry_after is not None:
            return min(
                retry_after + random.uniform(0, _RETRY_AFTER_JITTER_S),  # noqa: S311
                _MAX_RETRY_AFTER_S,
            )
    return _exponential_jitter(retry_state)


def _log_retry(retry_state: RetryCallState) -> None:
    outcome = retry_state.outcome
    exc = outcome.exception() if outcome is not None else None
    sleep_s = retry_state.next_action.sleep if retry_state.next_action else 0.0
    logger.warning(
        "Retrying after attempt %d failed (%r), sleeping %.1fs",
        retry_state.attempt_number,
        exc,
        sleep_s,
    )


http_retry = retry(
    retry=retry_if_exception(should_retry),
    stop=stop_after_attempt(_MAX_ATTEMPTS),
    wait=wait_retry_after_or_jitter,
    reraise=True,
    before_sleep=_log_retry,
)
