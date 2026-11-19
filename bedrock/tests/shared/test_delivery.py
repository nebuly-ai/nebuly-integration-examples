"""Every POST Nebuly did not confirm is retried, never twice in the same moment."""

from __future__ import annotations

import logging
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
        (400, "rejected"),
        (408, "retryable"),
        (422, "rejected"),
        (429, "retryable"),
        (500, "retryable"),
        (503, "retryable"),
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
def test_transport_failure_is_retried_by_a_later_run_only(
    tmp_path: Path, exception: type[httpx.TransportError]
) -> None:
    calls = []

    def fail(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise exception("private response or credentials", request=request)

    path = tmp_path / "db"
    with OutboxStore(path, now_ms=1000) as state:
        key = state.put("trace", {"synthetic": True})
        with SafeNebulyClient(
            "key", "https://example.test", transport=httpx.MockTransport(fail)
        ) as client:
            report = deliver_pending(state, client)
            assert report.retryable == 1
            assert state.get(key).status == "pending"
            # Nebuly may have it: not again in this run, nor within a minute.
            deliver_pending(state, client)
            assert len(calls) == 1
            assert not state.pending_deliveries()
    with OutboxStore(path, now_ms=61000) as state:
        assert len(state.pending_deliveries()) == 1


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


def test_unexpected_transport_error_is_sanitized_and_retried_later(
    tmp_path: Path,
) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("private-response-body")

    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient(
                "key", "https://example.test", transport=httpx.MockTransport(fail)
            ) as client,
            pytest.raises(DeliveryError, match="later run") as error,
        ):
            deliver_pending(state, client)
        assert "private-response-body" not in str(error.value)
        assert state.get(key).status == "pending"


def test_unexpected_transport_error_does_not_hold_the_others(tmp_path: Path) -> None:
    """A payload that always breaks the client backs off alone, like a 5xx:
    the source is not paused and the next run sends the others first."""
    requests: list[bytes] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        if b'"bad"' in request.content:
            raise RuntimeError("private-response-body")
        return httpx.Response(200)

    transport = httpx.MockTransport(respond)
    with OutboxStore(tmp_path / "db", now_ms=1000) as state:
        state.put("bad", _payload("bad", 0))
        state.put("good", _payload("good", 1))
        with (
            SafeNebulyClient("key", "https://example.test", transport=transport) as c,
            pytest.raises(DeliveryError),
        ):
            deliver_pending(state, c)
        assert not state.delivery_paused()
        state.now_ms = 1000 + 61_000
        assert [d.interaction_key for d in state.pending_deliveries()] == [
            "good",
            "bad",
        ]


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
        (301, DeliveryError, "NEBULY_ENDPOINT"),
        (308, DeliveryError, "NEBULY_ENDPOINT"),
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


@pytest.mark.parametrize("status", [408, 502, 503])
def test_second_failed_response_in_a_row_stops_the_run(
    tmp_path: Path, status: int
) -> None:
    """Nebuly failing two different items is an outage: stop, one more
    attempt only. Each item waits for itself; the source is not paused."""
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
        assert len(requests) == 2
        assert report.retryable == 2
        assert not report.paused
        assert state.delivery_counts() == {"pending": 3}


@pytest.mark.parametrize("status", [413, 422])
def test_rejected_interaction_is_logged_with_where_to_find_it(
    tmp_path: Path, status: int, caplog: pytest.LogCaptureFixture
) -> None:
    payload = {
        "interaction": {
            "conversation_id": "private@example.test",
            "time_start": "2026-11-11T10:00:00Z",
            "input": "secret",
        }
    }
    with OutboxStore(tmp_path / "db") as state:
        key = state.put("trace", payload)
        with (
            SafeNebulyClient(
                "key",
                "https://example.test",
                transport=httpx.MockTransport(lambda _: httpx.Response(status)),
            ) as client,
            caplog.at_level(logging.WARNING, logger="bedrock_sync.shared.delivery"),
        ):
            assert deliver_pending(state, client).rejected == 1
        assert state.get(key).status == "rejected"
    [line] = [
        r.getMessage()
        for r in caplog.records
        if r.name == "bedrock_sync.shared.delivery"
    ]
    assert "delivery trace" in line
    assert "private@example.test" not in line
    assert "2026-11-11T10:00:00Z" in line
    assert f"HTTP {status}" in line
    assert "secret" not in line
    assert ("too large" in line) == (status == 413)


def test_dry_run_state_cannot_send(tmp_path: Path) -> None:
    with OutboxStore(tmp_path / "missing" / "db", dry_run=True) as state:
        state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient("key", "https://example.test") as client,
            pytest.raises(DeliveryError, match="Dry-run"),
        ):
            deliver_pending(state, client)
    assert not (tmp_path / "missing").exists()


def test_process_interruption_is_sent_again_by_the_next_run(tmp_path: Path) -> None:
    def interrupt(request: httpx.Request) -> httpx.Response:
        raise KeyboardInterrupt

    path = tmp_path / "db"
    with OutboxStore(path, now_ms=1_000) as state:
        key = state.put("trace", {"synthetic": True})
        with (
            SafeNebulyClient(
                "key", "https://example.test", transport=httpx.MockTransport(interrupt)
            ) as client,
            pytest.raises(KeyboardInterrupt),
        ):
            deliver_pending(state, client)
        assert state.get(key).status == "sending"
    with OutboxStore(path, now_ms=2_000) as state:
        assert state.get(key).status == "pending"
        assert not state.pending_deliveries()
    with OutboxStore(path, now_ms=62_000) as state:
        assert len(state.pending_deliveries()) == 1


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


def test_retry_exhaustion_stays_pending_and_a_server_error_waits_again(
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
            assert deliver_pending(state, client).retryable == 1
        assert state.get(key).status == "pending"
        assert not state.pending_deliveries()
    with OutboxStore(path, now_ms=121000) as state:
        assert len(state.pending_deliveries()) == 1


def test_ambiguous_error_after_connection_retry_waits_for_a_later_run(
    tmp_path: Path,
) -> None:
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
            assert deliver_pending(state, client).retryable == 1
        assert len(calls) == 2


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


def _respond_by_key(
    statuses: dict[bytes, int], requests: list[bytes]
) -> httpx.MockTransport:
    """Answer each payload by a marker in its body (default 200)."""

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        for marker, status in statuses.items():
            if marker in request.content:
                return httpx.Response(status)
        return httpx.Response(200)

    return httpx.MockTransport(respond)


def _payload(name: str, minute: int) -> dict[str, Any]:
    return {
        "interaction": {"time_start": f"2026-11-11T10:{minute:02d}:00Z"},
        "name": name,
    }


def test_one_payload_failing_forever_does_not_hold_the_others(
    tmp_path: Path,
) -> None:
    """The oldest payload always gets a 504: the newer ones are sent in the
    same run, and from then on the failing one goes last."""
    path = tmp_path / "db"
    requests: list[bytes] = []
    transport = _respond_by_key({b'"bad"': 504}, requests)
    with OutboxStore(path, now_ms=1000) as state:
        state.put("bad", _payload("bad", 0))
        state.put("good", _payload("good", 1))
        with SafeNebulyClient("key", "https://example.test", transport=transport) as c:
            report = deliver_pending(state, c)
        assert (report.sent, report.retryable, report.paused) == (1, 1, False)
        assert state.delivery_counts() == {"pending": 1, "sent": 1}
        state.put("newer", _payload("newer", 2))
        state.now_ms = 1000 + 61_000
        assert [d.interaction_key for d in state.pending_deliveries()] == [
            "newer",
            "bad",
        ]


def test_failing_payload_waits_longer_each_time_then_is_given_up(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    requests: list[bytes] = []
    transport = _respond_by_key({b'"bad"': 503}, requests)
    with OutboxStore(tmp_path / "db", now_ms=0) as state:
        key = state.put("bad", _payload("bad", 0))
        waits = []
        with SafeNebulyClient("key", "https://example.test", transport=transport) as c:
            for _ in range(100):
                if state.get(key).status != "pending":
                    break
                due = state._db.execute(
                    "SELECT next_attempt_ms FROM delivery_policy"
                ).fetchone()[0]
                waits.append(due - state.now_ms)
                state.now_ms = due
                with caplog.at_level(logging.WARNING):
                    report = deliver_pending(state, c)
        # One minute, then the time it has been failing, up to six hours.
        assert waits[:5] == [0, 60_000, 60_000, 120_000, 240_000]
        assert max(waits) == 6 * 3600 * 1000
        assert state.now_ms >= 7 * 86400 * 1000
        assert report.rejected == 1
        assert state.delivery_counts() == {"rejected": 1}
        assert "given up" in caplog.text
        assert "--retry-given-up bad" in caplog.text
        assert "while it accepted others" not in caplog.text
        assert len(requests) < 50


def test_connection_failure_still_pauses_the_source_and_is_never_given_up(
    tmp_path: Path,
) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no connection", request=request)

    with OutboxStore(tmp_path / "db", now_ms=0) as state:
        state.put("first", _payload("first", 0))
        state.put("second", _payload("second", 1))
        with SafeNebulyClient(
            "key",
            "https://example.test",
            transport=httpx.MockTransport(fail),
            sleep=lambda _: None,
        ) as client:
            for day in range(9):
                state.now_ms = day * 86400 * 1000
                report = deliver_pending(state, client)
                assert report.retryable == 1
                assert report.paused
        assert state.delivery_counts() == {"pending": 2}
