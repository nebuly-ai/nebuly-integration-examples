from __future__ import annotations

import httpx
from httpx import HTTPStatusError, Request, Response
from openai_compliance_sync.compliance_client import (
    ComplianceClient,
    _should_retry,
    principal_scope_segment,
)


def _client(
    transport: httpx.BaseTransport, *, principal_id: str = "ws_demo"
) -> ComplianceClient:
    return ComplianceClient(
        httpx.Client(
            base_url="https://api.chatgpt.com/v1/compliance", transport=transport
        ),
        "key",
        principal_id,
    )


def test_should_retry_on_429() -> None:
    response = Response(429, request=Request("GET", "https://x"))
    exc = HTTPStatusError("err", request=response.request, response=response)
    assert _should_retry(exc) is True


def test_principal_scope_org() -> None:
    assert principal_scope_segment("org-abc") == "organizations"
    assert principal_scope_segment("ws-abc") == "workspaces"


def test_iter_log_files_pagination() -> None:
    seen_after: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        after = request.url.params.get("after")
        seen_after.append(after)
        assert request.url.path.endswith("/workspaces/ws_demo/logs")
        assert request.headers.get("Authorization") == "Bearer key"
        if after == "2025-01-01T00:00:00Z":
            return httpx.Response(
                200,
                json={
                    "data": [{"id": "file-1"}],
                    "has_more": True,
                    "last_end_time": "2025-01-02T00:00:00Z",
                },
            )
        return httpx.Response(
            200,
            json={"data": [{"id": "file-2"}], "has_more": False},
        )

    client = _client(httpx.MockTransport(handler))
    pages = list(
        client.iter_log_files("CONVERSATION_MESSAGE", after="2025-01-01T00:00:00Z")
    )
    assert len(pages) == 2
    assert [f.id for f in pages[0].data] == ["file-1"]
    assert seen_after == ["2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"]


def test_list_log_files_uses_organizations_for_org_principal() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"data": [], "has_more": False})

    client = _client(httpx.MockTransport(handler), principal_id="org-acme")
    client.list_log_files("CODEX_LOG")
    assert paths[0].endswith("/organizations/org-acme/logs")


def test_download_log_file_follows_redirect_and_parses_jsonl() -> None:
    redirect_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal redirect_count
        if request.url.path.endswith("/logs/file-1"):
            redirect_count += 1
            return httpx.Response(
                302,
                headers={"Location": "https://storage.example/files/file-1.jsonl"},
                request=request,
            )
        return httpx.Response(
            200,
            text='{"event_id":"a"}\n\nnot-json\n{"event_id":"b"}\n',
            request=request,
        )

    client = _client(httpx.MockTransport(handler))
    rows = list(client.download_log_file("file-1"))
    assert redirect_count == 1
    assert rows == [{"event_id": "a"}, {"event_id": "b"}]


def test_429_retries_with_retry_after() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, json={"data": [], "has_more": False})

    client = _client(httpx.MockTransport(handler))
    client.list_log_files("CONVERSATION_MESSAGE")
    assert attempts == 2
