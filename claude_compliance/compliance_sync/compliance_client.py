from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Iterator

import httpx
from httpx import HTTPStatusError
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from .models import (
    ChatMessagesResponse,
    LocalSession,
    LocalSessionMessagesResponse,
    PaginatedChatsResponse,
    PaginatedLocalSessionsResponse,
    PaginatedRemoteSessionsResponse,
    RemoteSession,
    RemoteSessionMessagesResponse,
    SessionMessage,
)

logger = logging.getLogger(__name__)

API_KEY_HEADER = "x-api-key"
DEFAULT_TIMEOUT_SECONDS = 300.0


class SourceUnavailableError(Exception):
    """Compliance API reports this source is disabled for the organization."""


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    if status == 429:
        return True
    if status >= 500:
        header = str(exc.response.headers.get("x-should-retry", "")).lower()
        return header != "false"
    return False


def _retry_after_seconds(retry_state: RetryCallState) -> float:
    if retry_state.outcome is None:
        return 60.0
    exc = retry_state.outcome.exception()
    if isinstance(exc, HTTPStatusError) and exc.response.status_code == 429:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                return float(retry_after)
            except ValueError:
                pass
        logger.warning("Rate limited (429), will retry")
    return 60.0


class _RateLimiter:
    def __init__(self, max_requests_per_minute: int) -> None:
        self._min_interval = 60.0 / max_requests_per_minute
        self._lock = threading.Lock()
        self._last_request_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_at
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request_at = time.monotonic()


class ComplianceClient:
    def __init__(
        self,
        client: httpx.Client,
        api_key: str,
        *,
        max_requests_per_minute: int = 600,
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._rate_limiter = _RateLimiter(max_requests_per_minute)

    def _headers(self) -> dict[str, str]:
        return {API_KEY_HEADER: self._api_key}

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=_retry_after_seconds,
        reraise=True,
    )
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        self._rate_limiter.wait()
        headers = self._headers()
        try:
            if params is None:
                resp = self._client.request(method, path, headers=headers)
            else:
                resp = self._client.request(
                    method, path, params=cast(Any, params), headers=headers
                )
            resp.raise_for_status()
            data = resp.json()
        except HTTPStatusError as e:
            # Logged once per retry attempt (429 and 5xx are retried).
            if e.response.status_code != 429:
                body_preview = e.response.text[:200]
                logger.exception(
                    "HTTP error from Compliance API %s %s: status=%s body=%r",
                    method,
                    path,
                    e.response.status_code,
                    body_preview,
                )
            raise
        except httpx.TransportError as e:
            logger.warning("Transport error on %s %s: %s, will retry", method, path, e)
            raise

        if not isinstance(data, dict):
            return {}
        return data

    def list_chats(
        self,
        organization_ids: list[str],
        *,
        order_by: str = "updated_at",
        updated_at_gte: str | None = None,
        updated_at_lte: str | None = None,
        after_id: str | None = None,
        limit: int = 100,
    ) -> PaginatedChatsResponse:
        params: list[tuple[str, str]] = [
            ("limit", str(min(limit, 100))),
            ("order_by", order_by),
        ]
        params.extend(("organization_ids[]", org_id) for org_id in organization_ids)
        if updated_at_gte is not None:
            params.append(("updated_at.gte", updated_at_gte))
        if updated_at_lte is not None:
            params.append(("updated_at.lte", updated_at_lte))
        if after_id is not None:
            params.append(("after_id", after_id))
        raw = self._request("GET", "apps/chats", params=params)
        return PaginatedChatsResponse.model_validate(raw)

    def iter_chats(
        self,
        organization_ids: list[str],
        *,
        order_by: str = "updated_at",
        updated_at_gte: str | None = None,
        updated_at_lte: str | None = None,
        limit: int = 100,
    ) -> Iterator[PaginatedChatsResponse]:
        after_id: str | None = None
        while True:
            page = self.list_chats(
                organization_ids,
                order_by=order_by,
                updated_at_gte=updated_at_gte,
                updated_at_lte=updated_at_lte,
                after_id=after_id,
                limit=limit,
            )
            yield page
            if not page.has_more:
                break
            after_id = page.last_id
            if after_id is None:
                break

    def list_chat_messages(
        self,
        chat_id: str,
        *,
        created_at_gte: str | None = None,
        created_at_lte: str | None = None,
        after_id: str | None = None,
        order: str = "asc",
        limit: int = 1000,
    ) -> ChatMessagesResponse:
        merged: ChatMessagesResponse | None = None
        page_after_id = after_id
        while True:
            params: list[tuple[str, str]] = [
                ("order", order),
                ("limit", str(limit)),
            ]
            if created_at_gte is not None:
                params.append(("created_at.gte", created_at_gte))
            if created_at_lte is not None:
                params.append(("created_at.lte", created_at_lte))
            if page_after_id is not None:
                params.append(("after_id", page_after_id))
            raw = self._request(
                "GET",
                f"apps/chats/{chat_id}/messages",
                params=params,
            )
            page = ChatMessagesResponse.model_validate(raw)
            if merged is None:
                merged = page
            else:
                merged.chat_messages.extend(page.chat_messages)
            if not page.has_more:
                break
            page_after_id = page.last_id
            if page_after_id is None:
                break

        if merged is None:
            raise RuntimeError("list_chat_messages returned no pages")
        return merged

    def _list_local_sessions_page(
        self,
        *,
        updated_at_gte: str | None = None,
        page: str | None = None,
        limit: int = 500,
    ) -> PaginatedLocalSessionsResponse:
        params: list[tuple[str, str]] = [("limit", str(min(limit, 500)))]
        if updated_at_gte is not None:
            params.append(("updated_at.gte", updated_at_gte))
        if page is not None:
            params.append(("page", page))
        try:
            raw = self._request("GET", "apps/sessions/local", params=params)
        except HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise SourceUnavailableError(
                    "Local sessions are not available."
                ) from exc
            raise
        return PaginatedLocalSessionsResponse.model_validate(raw)

    def iter_local_sessions(
        self,
        *,
        updated_at_gte: str | None = None,
        limit: int = 500,
    ) -> Iterator[LocalSession]:
        page_token: str | None = None
        while True:
            response = self._list_local_sessions_page(
                updated_at_gte=updated_at_gte,
                page=page_token,
                limit=limit,
            )
            yield from response.data
            if response.next_page is None:
                break
            page_token = response.next_page

    def _list_remote_sessions_page(
        self,
        organization_ids: list[str],
        *,
        created_at_gte: str | None = None,
        page: str | None = None,
        limit: int = 500,
    ) -> PaginatedRemoteSessionsResponse:
        params: list[tuple[str, str]] = [("limit", str(min(limit, 500)))]
        params.extend(("organization_ids[]", org_id) for org_id in organization_ids)
        if created_at_gte is not None:
            params.append(("created_at.gte", created_at_gte))
        if page is not None:
            params.append(("page", page))
        raw = self._request("GET", "apps/sessions/remote", params=params)
        return PaginatedRemoteSessionsResponse.model_validate(raw)

    def iter_remote_sessions(
        self,
        organization_ids: list[str],
        *,
        created_at_gte: str | None = None,
        limit: int = 500,
    ) -> Iterator[RemoteSession]:
        page_token: str | None = None
        while True:
            response = self._list_remote_sessions_page(
                organization_ids,
                created_at_gte=created_at_gte,
                page=page_token,
                limit=limit,
            )
            yield from response.data
            if response.next_page is None:
                break
            page_token = response.next_page

    def list_session_messages(
        self,
        kind: Literal["local", "remote"],
        session_id: str,
        *,
        limit: int = 1000,
    ) -> tuple[LocalSession | RemoteSession, list[SessionMessage]]:
        path = (
            f"apps/sessions/local/{session_id}/messages"
            if kind == "local"
            else f"apps/sessions/remote/{session_id}/messages"
        )
        messages: list[SessionMessage] = []
        session: LocalSession | RemoteSession | None = None
        page_token: str | None = None
        while True:
            params: list[tuple[str, str]] = [("order", "asc"), ("limit", str(limit))]
            if page_token is not None:
                params.append(("page", page_token))
            raw = self._request("GET", path, params=params)
            page: LocalSessionMessagesResponse | RemoteSessionMessagesResponse
            if kind == "local":
                page = LocalSessionMessagesResponse.model_validate(raw)
            else:
                page = RemoteSessionMessagesResponse.model_validate(raw)
            if session is None:
                session = page.session
            messages.extend(page.data)
            if page.next_page is None:
                break
            page_token = page.next_page
        if session is None:
            raise RuntimeError(f"Session {session_id} transcript returned no session")
        return session, messages
