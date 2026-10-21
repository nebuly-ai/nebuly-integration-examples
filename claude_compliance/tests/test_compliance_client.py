from __future__ import annotations

import httpx
import pytest
from compliance_sync.compliance_client import (
    ComplianceClient,
    SourceUnavailableError,
    _should_retry,
)
from httpx import HTTPStatusError, Request, Response


def _client(transport: httpx.BaseTransport) -> ComplianceClient:
    return ComplianceClient(
        httpx.Client(base_url="https://api.example/v1/compliance", transport=transport),
        "key",
    )


def test_should_retry_honors_x_should_retry_false() -> None:
    response = Response(
        500, headers={"x-should-retry": "false"}, request=Request("GET", "https://x")
    )
    exc = HTTPStatusError("err", request=response.request, response=response)
    assert _should_retry(exc) is False


def test_should_retry_on_429() -> None:
    response = Response(429, request=Request("GET", "https://x"))
    exc = HTTPStatusError("err", request=response.request, response=response)
    assert _should_retry(exc) is True


def test_list_chats_org_wide_params() -> None:
    seen: list[list[tuple[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(list(request.url.params.multi_items()))
        if "after_id" in request.url.params:
            return httpx.Response(200, json={"data": [], "has_more": False})
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "c1",
                        "name": "n",
                        "created_at": "2025-01-01T00:00:00Z",
                        "updated_at": "2025-01-01T00:00:00Z",
                        "href": "h",
                        "organization_uuid": "org_demo",
                        "model": None,
                    }
                ],
                "has_more": True,
                "last_id": "c1",
            },
        )

    client = _client(httpx.MockTransport(handler))
    pages = list(
        client.iter_chats(
            ["org_demo"],
            updated_at_gte="2025-01-01T00:00:00Z",
            updated_at_lte="2025-01-02T00:00:00Z",
        )
    )
    assert len(pages) == 2
    assert ("organization_ids[]", "org_demo") in seen[0]
    assert ("order_by", "updated_at") in seen[0]
    assert ("updated_at.gte", "2025-01-01T00:00:00Z") in seen[0]
    assert ("updated_at.lte", "2025-01-02T00:00:00Z") in seen[0]
    assert all("user_ids[]" not in k for k, _ in seen[0])
    assert ("after_id", "c1") in seen[1]


def test_iter_local_sessions_404_raises_source_unavailable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "Local sessions are not available."})

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(SourceUnavailableError):
        list(client.iter_local_sessions())


def test_iter_local_sessions_next_page_walk() -> None:
    pages_seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page")
        pages_seen.append(page)
        if page is None:
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "clls_1",
                            "organization_uuid": "org_demo",
                            "created_at": "2025-01-01T00:00:00Z",
                            "updated_at": "2025-01-01T00:00:00Z",
                        }
                    ],
                    "next_page": "p2",
                },
            )
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "clls_2",
                        "organization_uuid": "org_demo",
                        "created_at": "2025-01-02T00:00:00Z",
                        "updated_at": "2025-01-02T00:00:00Z",
                    }
                ],
                "next_page": None,
            },
        )

    client = _client(httpx.MockTransport(handler))
    sessions = list(client.iter_local_sessions(updated_at_gte="2025-01-01T00:00:00Z"))
    assert [s.id for s in sessions] == ["clls_1", "clls_2"]
    assert pages_seen == [None, "p2"]


def test_iter_remote_sessions_created_at_gte() -> None:
    captured: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.extend(request.url.params.multi_items())
        return httpx.Response(200, json={"data": [], "next_page": None})

    client = _client(httpx.MockTransport(handler))
    list(
        client.iter_remote_sessions(["org_demo"], created_at_gte="2025-03-01T00:00:00Z")
    )
    assert ("organization_ids[]", "org_demo") in captured
    assert ("created_at.gte", "2025-03-01T00:00:00Z") in captured


def test_list_session_messages_pages() -> None:
    pages: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page")
        pages.append(page)
        if page is None:
            return httpx.Response(
                200,
                json={
                    "session": {
                        "id": "clls_1",
                        "organization_uuid": "org_demo",
                        "created_at": "2025-01-01T00:00:00Z",
                        "updated_at": "2025-01-01T00:00:00Z",
                    },
                    "data": [
                        {
                            "id": "m1",
                            "role": "user",
                            "created_at": "2025-01-01T00:00:00Z",
                            "content": [{"type": "text", "text": "hi"}],
                        }
                    ],
                    "next_page": "p2",
                },
            )
        return httpx.Response(
            200,
            json={
                "session": {
                    "id": "clls_1",
                    "organization_uuid": "org_demo",
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-01T00:00:00Z",
                },
                "data": [
                    {
                        "id": "m2",
                        "role": "assistant",
                        "created_at": "2025-01-01T00:01:00Z",
                        "content": [{"type": "text", "text": "hey"}],
                    }
                ],
                "next_page": None,
            },
        )

    client = _client(httpx.MockTransport(handler))
    session, messages = client.list_session_messages("local", "clls_1")
    assert session.id == "clls_1"
    assert [m.id for m in messages] == ["m1", "m2"]
    assert pages == [None, "p2"]


def test_429_retries_with_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, json={"data": [], "has_more": False})

    client = _client(httpx.MockTransport(handler))
    client.list_chats(["org_demo"])
    assert attempts == 2
