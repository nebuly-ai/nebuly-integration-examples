from __future__ import annotations

import httpx
from httpx import HTTPStatusError, Request, Response
from openai_compliance_sync.compliance_client import ComplianceClient, _should_retry

WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"


def _client(
    transport: httpx.BaseTransport, *, workspace_id: str = WORKSPACE_ID
) -> ComplianceClient:
    return ComplianceClient(
        httpx.Client(
            base_url="https://api.chatgpt.com/v1/compliance", transport=transport
        ),
        "key",
        workspace_id,
    )


def test_should_retry_on_429() -> None:
    response = Response(429, request=Request("GET", "https://x"))
    exc = HTTPStatusError("err", request=response.request, response=response)
    assert _should_retry(exc) is True


def test_list_log_files_uses_workspace_path() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        assert "before" not in request.url.params
        return httpx.Response(200, json={"data": [], "has_more": False})

    client = _client(httpx.MockTransport(handler))
    client.list_log_files("CONVERSATION_MESSAGE")
    assert paths[0].endswith(f"/workspaces/{WORKSPACE_ID}/logs")


def test_list_log_files_sends_before_only_when_set() -> None:
    before_values: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        before_values.append(request.url.params.get("before"))
        return httpx.Response(200, json={"data": [], "has_more": False})

    client = _client(httpx.MockTransport(handler))
    client.list_log_files("APP_LOG", before="2025-06-01T00:00:00Z")
    assert before_values == ["2025-06-01T00:00:00Z"]


def test_iter_log_files_pagination() -> None:
    seen_after: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        after = request.url.params.get("after")
        seen_after.append(after)
        assert request.url.path.endswith(f"/workspaces/{WORKSPACE_ID}/logs")
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


def test_download_log_file_follows_307_without_leaking_auth() -> None:
    redirect_count = 0
    storage_auth: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal redirect_count
        if request.url.path.endswith("/logs/file-1"):
            redirect_count += 1
            return httpx.Response(
                307,
                headers={"Location": "https://storage.example/files/file-1.jsonl"},
                request=request,
            )
        storage_auth.append(request.headers.get("Authorization"))
        return httpx.Response(
            200,
            text='{"event_id":"a"}\n\nnot-json\n{"event_id":"b"}\n',
            request=request,
        )

    client = _client(httpx.MockTransport(handler))
    rows = list(client.download_log_file("file-1"))
    assert redirect_count == 1
    assert storage_auth == [None]
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
