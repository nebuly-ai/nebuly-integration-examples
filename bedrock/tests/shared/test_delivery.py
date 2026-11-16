"""A remote POST cannot be made exactly-once by retrying uncertain attempts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import httpx
import pytest

from bedrock_sync.shared.delivery import (
    AuthenticationError,
    DeliveryError,
    DeliveryReport,
    SafeNebulyClient,
    deliver,
)
from tests.shared.stores import OutboxStore

if TYPE_CHECKING:
    from pathlib import Path


def deliver_pending(
    store: OutboxStore,
    client: SafeNebulyClient,
    **kwargs: Any,  # noqa: ANN401
) -> DeliveryReport:
    return deliver(store, client, store.pending_deliveries(), **kwargs)


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (200, "sent"),
        (202, "sent"),
        (204, "sent"),
        (301, "uncertain"),
        (307, "uncertain"),
        (308, "uncertain"),
        (400, "rejected"),
        (408, "uncertain"),
        (422, "rejected"),
        (429, "retryable"),
        (500, "uncertain"),
        (503, "uncertain"),
    ],
)
def test_one_post_after_durable_marker_and_no_replay(
    tmp_path: Path, status: int, outcome: str
) -> None:
    path = tmp_path / "delivery.db"
    calls = []
    with OutboxStore(path) as state:
        key = state.put("trace", {"synthetic": True})

        def respond(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            assert state.get(key).status == "sending"
            assert request.headers["Authorization"] == "Bearer synthetic-key"
            return httpx.Response(
                status,
                headers={"Location": "https://example.test/redirect"},
                text="private response",
            )

        with SafeNebulyClient(
            "synthetic-key",
            "https://example.test/events",
            transport=httpx.MockTransport(respond),
            max_attempts=1,
        ) as client:
            report = deliver_pending(state, client)
            assert getattr(report, outcome) == 1
            assert state.get(key).status == (
                "pending" if outcome == "retryable" else outcome
            )
            deliver_pending(state, client)
            assert len(calls) == 1
    with OutboxStore(path) as state:
        assert not state.pending_deliveries()


@pytest.mark.parametrize(
    "exception", [httpx.ReadTimeout, httpx.ReadError, httpx.WriteError]
)
def test_transport_failure_is_uncertain_once(
    tmp_path: Path, exception: type[httpx.TransportError]
) -> None:
    calls = []

    def fail(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise exception("private response or credentials", request=request)

    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key", "https://example.test", transport=httpx.MockTransport(fail)
        ) as client:
            report = deliver_pending(state, client)
            assert report.uncertain == 1
            assert state.get(key).status == "uncertain"
            deliver_pending(state, client)
            assert len(calls) == 1


def test_preparation_failure_is_still_pending(tmp_path: Path) -> None:
    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient("key", "https://example.test") as client,
            patch.object(
                client, "prepare", side_effect=ValueError("invalid serialization")
            ),
            pytest.raises(ValueError, match="serialization"),
        ):
            deliver_pending(state, client)
        assert state.get(key).status == "pending"


def test_unexpected_transport_error_is_sanitized_and_uncertain(tmp_path: Path) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("private-response-body")

    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient(
                "key", "https://example.test", transport=httpx.MockTransport(fail)
            ) as client,
            pytest.raises(DeliveryError, match="uncertain") as error,
        ):
            deliver_pending(state, client)
        assert "private-response-body" not in str(error.value)
        assert state.get(key).status == "uncertain"


def test_auth_failure_stops_before_other_pending_deliveries(tmp_path: Path) -> None:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(401)

    with OutboxStore(tmp_path / "db") as state:
        state.put("first", {"synthetic": True})
        state.put("second", {"synthetic": True})
        with (
            SafeNebulyClient(
                "key", "https://example.test", transport=httpx.MockTransport(respond)
            ) as client,
            pytest.raises(AuthenticationError, match="authentication failed"),
        ):
            deliver_pending(state, client)
        assert len(requests) == 1
        # Nothing was ingested: both stay pending for the run with the right key.
        assert len(state.pending_deliveries()) == 2


@pytest.mark.parametrize(
    ("status", "error", "match"),
    [
        (401, AuthenticationError, "authentication failed"),
        (403, AuthenticationError, "authentication failed"),
        (404, DeliveryError, "NEBULY_ENDPOINT"),
        (405, DeliveryError, "NEBULY_ENDPOINT"),
    ],
)
def test_wrong_key_or_endpoint_keeps_the_item_pending(
    tmp_path: Path, status: int, error: type[Exception], match: str
) -> None:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status)

    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient(
                "key", "https://example.test", transport=httpx.MockTransport(respond)
            ) as client,
            pytest.raises(error, match=match),
        ):
            deliver_pending(state, client)
        assert len(requests) == 1
        assert state.get(key).status == "pending"
        assert not state.delivery_paused()


@pytest.mark.parametrize("status", [308, 502, 503])
def test_first_uncertain_response_stops_the_run(tmp_path: Path, status: int) -> None:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status)

    with OutboxStore(tmp_path / "db") as state:
        for name in ("first", "second", "third"):
            state.put(name, {"synthetic": True})
        with SafeNebulyClient(
            "key", "https://example.test", transport=httpx.MockTransport(respond)
        ) as client:
            report = deliver_pending(state, client)
        assert len(requests) == 1
        assert report.uncertain == 1
        assert len(state.pending_deliveries()) == 2


def test_dry_run_state_cannot_send(tmp_path: Path) -> None:
    with OutboxStore(tmp_path / "missing" / "db", dry_run=True) as state:
        state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient("key", "https://example.test") as client,
            pytest.raises(DeliveryError, match="Dry-run"),
        ):
            deliver_pending(state, client)
    assert not (tmp_path / "missing").exists()


def test_process_interruption_is_recovered_without_replay(tmp_path: Path) -> None:
    def interrupt(request: httpx.Request) -> httpx.Response:
        raise KeyboardInterrupt

    path = tmp_path / "db"
    with OutboxStore(path) as state:
        key = state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient(
                "key", "https://example.test", transport=httpx.MockTransport(interrupt)
            ) as client,
            pytest.raises(KeyboardInterrupt),
        ):
            deliver_pending(state, client)
        assert state.get(key).status == "sending"
    with OutboxStore(path) as state:
        assert state.get(key).status == "uncertain"
        assert not state.pending_deliveries()


@pytest.mark.parametrize(
    "exception", [httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout]
)
def test_safe_connection_failures_retry_then_succeed(
    tmp_path: Path, exception: type[httpx.TransportError]
) -> None:
    calls = []
    delays: list[float] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            raise exception("not connected", request=request)
        return httpx.Response(200)

    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(respond),
            sleep=delays.append,
        ) as client:
            assert deliver_pending(state, client).sent == 1
        assert state.get(key).status == "sent"
        assert len(calls) == 2
        assert delays == [1]


def test_rate_limit_retries_with_retry_after(tmp_path: Path) -> None:
    calls = []
    delays: list[float] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return (
            httpx.Response(429, headers={"Retry-After": "2"})
            if len(calls) == 1
            else httpx.Response(200)
        )

    with OutboxStore(tmp_path / "db") as state:
        state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(respond),
            sleep=delays.append,
        ) as client:
            assert deliver_pending(state, client).sent == 1
        assert len(calls) == 2
        assert delays == [2]


def test_long_retry_after_is_persisted_across_runs(tmp_path: Path) -> None:
    path = tmp_path / "db"
    delays: list[float] = []
    with OutboxStore(path, now_ms=1000) as state:
        key = state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(429, headers={"Retry-After": "120"})
            ),
            sleep=delays.append,
        ) as client:
            assert deliver_pending(state, client).retryable == 1
        assert state.get(key).status == "pending"
        assert not state.pending_deliveries()
    assert not delays
    with OutboxStore(path, now_ms=120999) as state:
        assert not state.pending_deliveries()
        # A later run during the cooldown sends nothing but is still incomplete.
        with SafeNebulyClient("key", "https://example.test") as client:
            report = deliver_pending(state, client)
        assert report == DeliveryReport(paused=True)
    with OutboxStore(path, now_ms=121000) as state:
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(lambda _: httpx.Response(200)),
        ) as client:
            assert deliver_pending(state, client).sent == 1
        assert state.get(key).status == "sent"


def test_retry_exhaustion_stays_pending_and_ambiguous_next_attempt_stops(
    tmp_path: Path,
) -> None:
    path = tmp_path / "db"
    calls = []
    delays: list[float] = []

    def fail(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ConnectError("no connection", request=request)

    with OutboxStore(path, now_ms=1000) as state:
        key = state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(fail),
            sleep=delays.append,
        ) as client:
            assert deliver_pending(state, client).retryable == 1
        assert state.get(key).status == "pending"
        assert len(calls) == 3
        assert delays == [1, 2]
    with OutboxStore(path, now_ms=61000) as state:
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(lambda _: httpx.Response(500)),
        ) as client:
            assert deliver_pending(state, client).uncertain == 1
        assert state.get(key).status == "uncertain"
        assert not state.pending_deliveries()


def test_ambiguous_error_after_connection_retry_is_not_retried(tmp_path: Path) -> None:
    calls = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ConnectError("no connection", request=request)
        raise httpx.ReadTimeout("response lost", request=request)

    with OutboxStore(tmp_path / "db") as state:
        state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(respond),
            sleep=lambda _: None,
        ) as client:
            assert deliver_pending(state, client).uncertain == 1
        assert len(calls) == 2


def test_rate_limit_retry_can_be_disabled(tmp_path: Path) -> None:
    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(lambda _: httpx.Response(429)),
            retry_http_429=False,
        ) as client:
            assert deliver_pending(state, client).uncertain == 1
        assert state.get(key).status == "uncertain"


def test_retry_deadline_is_relative_to_response_time_not_run_start(
    tmp_path: Path,
) -> None:
    with OutboxStore(tmp_path / "db", now_ms=1000) as state:
        state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(429, headers={"Retry-After": "120"})
            ),
        ) as client:
            assert (
                deliver_pending(state, client, clock_ms=lambda: 100000).retryable == 1
            )
        state.now_ms = 219999
        assert not state.pending_deliveries()
        state.now_ms = 220000
        assert len(state.pending_deliveries()) == 1


def test_throttle_stops_remaining_deliveries_and_persists_source_cooldown(
    tmp_path: Path,
) -> None:
    path = tmp_path / "db"
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(429, headers={"Retry-After": "120"})

    with OutboxStore(path, now_ms=1000) as state:
        for trace in ("first", "second"):
            state.put(trace, {"synthetic": True})
        with SafeNebulyClient(
            "key", "https://example.test", transport=httpx.MockTransport(respond)
        ) as client:
            assert deliver_pending(state, client).retryable == 1
        assert len(requests) == 1
        assert state.delivery_counts() == {"pending": 2}
    with OutboxStore(path, now_ms=120999) as state:
        assert not state.pending_deliveries()
    with OutboxStore(path, now_ms=121000) as state:
        assert len(state.pending_deliveries()) == 2


def test_http_date_retry_after_is_deferred_without_waiting(tmp_path: Path) -> None:
    deadline = format_datetime(datetime.now(UTC) + timedelta(hours=1), usegmt=True)
    delays: list[float] = []
    with OutboxStore(tmp_path / "db", now_ms=1000) as state:
        state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(429, headers={"Retry-After": deadline})
            ),
            sleep=delays.append,
        ) as client:
            assert deliver_pending(state, client).retryable == 1
        assert not delays
        state.now_ms = 3500000
        assert not state.pending_deliveries()
        state.now_ms = 3700000
        assert len(state.pending_deliveries()) == 1


def test_huge_retry_after_is_capped_at_one_day(tmp_path: Path) -> None:
    with OutboxStore(tmp_path / "db", now_ms=1000) as state:
        state.put("trace")
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(429, headers={"Retry-After": "999999999"})
            ),
        ) as client:
            assert deliver_pending(state, client).retryable == 1
    with OutboxStore(tmp_path / "db", now_ms=1000 + 86400 * 1000) as state:
        assert state.pending_deliveries()
        assert not state.delivery_paused()
