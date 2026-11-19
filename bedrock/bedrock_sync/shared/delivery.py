"""Durable delivery: retry unconfirmed POSTs and record terminal outcomes.

Shared by the AgentCore and runtime connectors. Nebuly updates the existing
interaction when one with the same input, end user and start time arrives
again, and a payload changes only to be completed with those values kept (an
AgentCore late span), so sending it twice is safe. The outcomes:

- ``sent``: confirmed 2xx;
- ``rejected``: permanent 4xx, or ambiguous failures spanning at least 7
  days. The latter can be retried explicitly while the payload is retained.
- ``retryable``: anything else; the item goes back to ``pending``.
  - Not received (connection failure, 429): short waits are retried in the
    same run, longer ones by a later run after the ``Retry-After`` delay.
  - Maybe received (timeout after sending, 408, 5xx): retried by a later run,
    at least a minute later, never at once, so the two copies do not reach
    Nebuly at the same time. Each failure of the same item waits longer (up
    to 6 hours), and after 7 days of failures the item is given up as
    ``rejected``.
  - Wrong setting (3xx, 401/403 bad key, 404/405 wrong endpoint): the run
    stops with an error to fix first.

A 429, a connection failure or a wrong setting stops the run at once, so an
outage costs one attempt, not the whole outbox. A 5xx or a timeout may come
from one bad payload: the run goes on with the next item, which goes first
from then on, and stops at the second such failure in a row. The store (an outbox in
SQLite) commits ``sending`` before each POST; a ``sending`` left by a crash
goes back to ``pending``.
"""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Literal, Protocol, Self

import httpx

from .errors import ConnectorError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from types import TracebackType

DeliveryStatus = Literal["pending", "sending", "sent", "rejected"]
Outcome = Literal["sent", "rejected", "retryable"]


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
        maybe_received: bool = False,
    ) -> DeliveryStatus: ...


class DeliveryError(ConnectorError):
    """Sanitized delivery failure; raw requests/responses are never included."""


class AuthenticationError(DeliveryError):
    """Stop the run after an explicit authentication rejection."""


@dataclass(frozen=True)
class SendResult:
    outcome: Outcome
    http_status: int | None = None
    retry_after_seconds: int | None = None
    maybe_received: bool = False
    """Nebuly may have the payload: retry it only in a later run."""


@dataclass(frozen=True)
class DeliveryReport:
    sent: int = 0
    rejected: int = 0
    retryable: int = 0
    paused: bool = False
    """Deliveries wait for an earlier Retry-After, from this run or a previous one."""


logger = logging.getLogger(__name__)

RETRYABLE_4XX = frozenset({401, 403, 404, 405, 408, 429})
"""4xx statuses that do not reject the payload itself."""

MIN_LATER_RETRY_SECONDS = 60
"""A payload Nebuly may have received waits at least this long."""


def needs_fix(status: int | None) -> bool:
    """Wrong key or endpoint: nothing was ingested, the setting must change."""
    return status is not None and (
        300 <= status < 400 or status in {401, 403, 404, 405}
    )


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
    ) -> None:
        if max_attempts < 1:
            raise ValueError("At least one delivery attempt is required")
        self._max_attempts = max_attempts
        self._sleep = sleep
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
            if result.outcome != "retryable" or needs_fix(result.http_status):
                return result
            if result.maybe_received:
                return SendResult(
                    "retryable",
                    result.http_status,
                    max(MIN_LATER_RETRY_SECONDS, result.retry_after_seconds or 0),
                    maybe_received=True,
                )
            delay = (
                result.retry_after_seconds
                if result.retry_after_seconds is not None
                else 2**index
            )
            if index + 1 == self._max_attempts or delay > 5:
                return SendResult(
                    "retryable", result.http_status, max(MIN_LATER_RETRY_SECONDS, delay)
                )
            self._sleep(delay)
        raise AssertionError("Unreachable delivery retry state")

    def _send_once(self, request: httpx.Request) -> SendResult:
        try:
            response = self._client.send(request, follow_redirects=False, stream=True)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            return SendResult("retryable")
        except httpx.TransportError:
            return SendResult("retryable", maybe_received=True)
        try:
            status = response.status_code
            retry_after = _retry_after(response.headers.get("Retry-After"))
        finally:
            response.close()
        if 200 <= status < 300:
            return SendResult("sent", status)
        if status == 429:
            return SendResult("retryable", status, retry_after)
        if needs_fix(status):
            return SendResult("retryable", status, 0)
        if 400 <= status < 500 and status not in RETRYABLE_4XX:
            return SendResult("rejected", status)
        return SendResult("retryable", status, retry_after, maybe_received=True)


def _log_given_up(delivery: Delivery, status: int | None) -> None:
    logger.warning(
        "Delivery %s was given up and counted as rejected after ambiguous failures "
        "spanning at least 7 days (last HTTP %s). Once the cause is fixed, use "
        "--retry-given-up %s with the same state file while it retains the payload",
        delivery.interaction_key,
        status if status is not None else "none, timeout",
        delivery.interaction_key,
    )


def _log_rejected(
    delivery: Delivery, request: httpx.Request, status: int | None
) -> None:
    """One line per rejected interaction, with what finds it in the logs."""
    interaction = delivery.payload.get("interaction")
    found = interaction if isinstance(interaction, dict) else {}
    where = (delivery.interaction_key, found.get("time_start"))
    if status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE:
        logger.warning(
            "Nebuly refused delivery %s at %s as too large "
            "(%d bytes, HTTP 413): it is not sent again; lower MAX_CONTENT_CHARS "
            "in user_defined.py to send interactions this big",
            *where,
            len(request.content),
        )
    else:
        logger.warning(
            "Nebuly rejected delivery %s at %s (HTTP %s): it is not sent again",
            *where,
            status,
        )


def deliver(
    store: DeliveryStore,
    client: SafeNebulyClient,
    deliveries: Iterable[Delivery],
    *,
    clock_ms: Callable[[], int] | None = None,
) -> DeliveryReport:
    """Persist sending before HTTP; stop at an outage or a wrong setting."""
    if store.dry_run:
        raise DeliveryError("Dry-run state cannot deliver interactions")
    counts: dict[str, int] = {"sent": 0, "rejected": 0, "retryable": 0}
    failed_in_row = 0
    for delivery in deliveries:
        request = client.prepare(delivery.payload)
        attempt = store.begin_attempt(delivery.interaction_key)
        try:
            result = client.send(request)
        except Exception:  # noqa: BLE001 - persist the attempt at the transport boundary
            store.finish_attempt(
                attempt, "retryable", retry_after_seconds=MIN_LATER_RETRY_SECONDS
            )
            raise DeliveryError(
                "Unexpected transport failure; a later run sends the item again"
            ) from None
        status = store.finish_attempt(
            attempt,
            result.outcome,
            http_status=result.http_status,
            retry_after_seconds=MIN_LATER_RETRY_SECONDS
            if result.retry_after_seconds is None
            else result.retry_after_seconds,
            finished_ms=clock_ms() if clock_ms else None,
            maybe_received=result.maybe_received,
        )
        if result.outcome == "retryable" and status == "rejected":
            counts["rejected"] += 1
            _log_given_up(delivery, result.http_status)
        else:
            counts[result.outcome] += 1
        if result.outcome == "rejected":
            _log_rejected(delivery, request, result.http_status)
        if result.http_status in {401, 403}:
            raise AuthenticationError(
                "Nebuly authentication failed; correct credentials before another run"
            )
        if needs_fix(result.http_status):
            raise DeliveryError(
                f"Nebuly endpoint not found or redirected (HTTP {result.http_status}); "
                "check NEBULY_ENDPOINT before another run"
            )
        if result.outcome != "retryable":
            failed_in_row = 0
            continue
        failed_in_row += 1
        # An unreachable or throttling API, or a second failure in a row,
        # costs one more attempt, not one per queued item.
        if not result.maybe_received or failed_in_row == 2:
            break
    return DeliveryReport(**counts, paused=store.delivery_paused())
