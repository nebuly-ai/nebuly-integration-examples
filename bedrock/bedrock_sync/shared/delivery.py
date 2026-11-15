"""Durable delivery with retries only for connection failure and rate limiting.

Shared by the AgentCore and runtime connectors. Nebuly does not deduplicate,
so a POST whose outcome is unknown is never replayed automatically:

- ``sent``: confirmed 2xx;
- ``rejected``: explicit permanent 4xx (401/403 also stop the run);
- ``uncertain``: Nebuly may have accepted it (read errors, 408, 5xx, a crash
  while ``sending``); left for an operator;
- ``retryable``: connection failure or 429, rejected before ingestion; the
  item stays ``pending`` for a later attempt.

The store (an outbox in SQLite) commits ``sending`` before each POST.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Literal, Protocol, Self

import httpx

from .errors import ConnectorError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from types import TracebackType

DeliveryStatus = Literal["pending", "sending", "sent", "rejected", "uncertain"]
Outcome = Literal["sent", "rejected", "uncertain", "retryable"]


@dataclass(frozen=True)
class Delivery:
    interaction_key: str
    status: DeliveryStatus
    payload: dict[str, object] = field(repr=False)


class DeliveryStore(Protocol):
    """What ``deliver`` needs from an outbox."""

    @property
    def dry_run(self) -> bool: ...

    def delivery_paused(self) -> bool: ...

    def begin_attempt(self, interaction_key: str) -> int: ...

    def finish_attempt(
        self,
        attempt_id: int,
        outcome: Outcome,
        *,
        http_status: int | None = None,
        retry_after_seconds: int = 60,
        finished_ms: int | None = None,
    ) -> None: ...


class DeliveryError(ConnectorError):
    """Sanitized delivery failure; raw requests/responses are never included."""


class AuthenticationError(DeliveryError):
    """Stop the run after an explicit authentication rejection."""


@dataclass(frozen=True)
class SendResult:
    outcome: Outcome
    http_status: int | None = None
    retry_after_seconds: int | None = None


@dataclass(frozen=True)
class DeliveryReport:
    sent: int = 0
    rejected: int = 0
    uncertain: int = 0
    retryable: int = 0
    paused: bool = False
    """Deliveries wait for an earlier Retry-After, from this run or a previous one."""


MAX_RETRY_AFTER_SECONDS = 86400
"""A longer Retry-After is capped, so one bad header cannot stop delivery."""


def _retry_after(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        if value.strip().isascii() and value.strip().isdigit():
            return min(int(value), MAX_RETRY_AFTER_SECONDS)
        deadline = parsedate_to_datetime(value)
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        seconds = math.ceil((deadline - datetime.now(UTC)).total_seconds())
        return min(max(0, seconds), MAX_RETRY_AFTER_SECONDS)
    except (ValueError, TypeError, OverflowError):
        return None


class SafeNebulyClient:
    """Own the transport so redirects and automatic POST retries stay disabled."""

    def __init__(
        self,
        api_key: str,
        endpoint: str,
        *,
        transport: httpx.BaseTransport | None = None,
        max_attempts: int = 3,
        sleep: Callable[[float], None] = time.sleep,
        retry_http_429: bool = True,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("At least one delivery attempt is required")
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._retry_http_429 = retry_http_429
        self._endpoint = endpoint
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        self._client = httpx.Client(
            transport=transport
            if transport is not None
            else httpx.HTTPTransport(retries=0),
            follow_redirects=False,
            timeout=httpx.Timeout(30, connect=10),
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def prepare(self, payload: Mapping[str, object]) -> httpx.Request:
        """Validate serialization and construct the request before attempt creation."""
        content = json.dumps(
            payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode()
        return self._client.build_request(
            "POST", self._endpoint, content=content, headers=self._headers
        )

    def send(self, request: httpx.Request) -> SendResult:
        for index in range(self._max_attempts):
            result = self._send_once(request)
            if result.outcome != "retryable":
                return result
            delay = (
                result.retry_after_seconds
                if result.retry_after_seconds is not None
                else 2**index
            )
            if index + 1 == self._max_attempts or delay > 5:
                return SendResult("retryable", result.http_status, max(60, delay))
            self._sleep(delay)
        raise AssertionError("Unreachable delivery retry state")

    def _send_once(self, request: httpx.Request) -> SendResult:
        try:
            response = self._client.send(request, follow_redirects=False, stream=True)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            return SendResult("retryable")
        except httpx.TransportError:
            return SendResult("uncertain")
        try:
            status = response.status_code
            retry_after = _retry_after(response.headers.get("Retry-After"))
        finally:
            response.close()
        if 200 <= status < 300:
            return SendResult("sent", status)
        if status == 429 and self._retry_http_429:
            return SendResult("retryable", status, retry_after)
        if 400 <= status < 500 and status not in {408, 429}:
            return SendResult("rejected", status)
        return SendResult("uncertain", status)


def deliver(
    store: DeliveryStore,
    client: SafeNebulyClient,
    deliveries: Iterable[Delivery],
    *,
    clock_ms: Callable[[], int] | None = None,
) -> DeliveryReport:
    """Persist sending before HTTP; never turn uncertainty into an automatic retry."""
    if store.dry_run:
        raise DeliveryError("Dry-run state cannot deliver interactions")
    counts: dict[str, int] = {"sent": 0, "rejected": 0, "uncertain": 0, "retryable": 0}
    for delivery in deliveries:
        request = client.prepare(delivery.payload)
        attempt = store.begin_attempt(delivery.interaction_key)
        try:
            result = client.send(request)
        except Exception:  # noqa: BLE001 - persist ambiguity at the transport boundary
            store.finish_attempt(attempt, "uncertain")
            raise DeliveryError(
                "Unexpected transport failure; delivery is uncertain"
            ) from None
        store.finish_attempt(
            attempt,
            result.outcome,
            http_status=result.http_status,
            retry_after_seconds=result.retry_after_seconds or 60,
            finished_ms=clock_ms() if clock_ms else None,
        )
        counts[result.outcome] += 1
        if result.outcome == "retryable":
            # Preserve the remainder without hammering a throttled/unreachable API.
            break
        if result.http_status in {401, 403}:
            raise AuthenticationError(
                "Nebuly authentication failed; correct credentials before another run"
            )
    return DeliveryReport(**counts, paused=store.delivery_paused())
