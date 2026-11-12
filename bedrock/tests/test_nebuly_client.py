from __future__ import annotations

import json
from typing import TYPE_CHECKING

import httpx
import pytest
from bedrock_sync.nebuly_client import NebulyClient, PermanentRejectionError
from tenacity import wait_none

if TYPE_CHECKING:
    from collections.abc import Callable

ENDPOINT = (
    "https://backend.example.test/event-ingestion/api/v2/events/trace_interaction"
)


@pytest.fixture(autouse=True)
def _no_retry_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(NebulyClient.send_interaction.retry, "wait", wait_none())


def _client(
    statuses: list[int], seen: list[httpx.Request], *, dry_run: bool = False
) -> NebulyClient:
    replies = iter(statuses)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(next(replies), json={})

    http = httpx.Client(transport=httpx.MockTransport(handler))
    return NebulyClient(http, "nb-key", ENDPOINT, dry_run=dry_run)


def test_posts_the_payload_with_the_api_key() -> None:
    seen: list[httpx.Request] = []
    _client([200], seen).send_interaction({"interaction": {"input": "Q"}})

    assert len(seen) == 1
    assert str(seen[0].url) == ENDPOINT
    assert seen[0].headers["Authorization"] == "Bearer nb-key"
    assert json.loads(seen[0].content) == {"interaction": {"input": "Q"}}


@pytest.mark.parametrize("status", [429, 408, 500, 503])
def test_transient_errors_are_retried(status: int) -> None:
    seen: list[httpx.Request] = []
    _client([status, status, 200], seen).send_interaction({})
    assert len(seen) == 3


@pytest.mark.parametrize("status", [400, 401, 403, 422])
def test_permanent_rejection_is_not_retried(status: int) -> None:
    seen: list[httpx.Request] = []
    with pytest.raises(PermanentRejectionError) as info:
        _client([status, 200], seen).send_interaction({})
    assert info.value.status_code == status
    assert len(seen) == 1


def test_transient_errors_give_up_after_ten_attempts() -> None:
    seen: list[httpx.Request] = []
    with pytest.raises(httpx.HTTPStatusError):
        _client([500] * 10, seen).send_interaction({})
    assert len(seen) == 10


def test_network_errors_are_retried() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ConnectError("down", request=request)
        return httpx.Response(200)

    send: Callable[[dict[str, object]], None] = NebulyClient(
        httpx.Client(transport=httpx.MockTransport(handler)), "nb-key", ENDPOINT
    ).send_interaction
    send({})
    assert len(calls) == 2


def test_dry_run_sends_nothing() -> None:
    seen: list[httpx.Request] = []
    _client([], seen, dry_run=True).send_interaction({})
    assert seen == []
