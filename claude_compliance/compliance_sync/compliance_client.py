from __future__ import annotations

import logging
import threading
import time
from typing import Any, cast

import httpx
from httpx import ConnectTimeout, HTTPStatusError, ReadTimeout
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from .models import (
    ChatMessagesResponse,
    OrganizationUser,
    PaginatedChatsResponse,
    PaginatedUsersResponse,
)

logger = logging.getLogger(__name__)

API_KEY_HEADER = "x-api-key"


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
        retry=retry_if_exception(
            lambda e: (
                isinstance(e, (ReadTimeout, ConnectTimeout, HTTPStatusError))
                and (
                    not isinstance(e, HTTPStatusError) or e.response.status_code == 429
                )
            )
        ),
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
            if e.response.status_code != 429:
                body_preview = e.response.text[:200]
                logger.error(
                    "HTTP error from Compliance API %s %s: status=%s body=%r",
                    method,
                    path,
                    e.response.status_code,
                    body_preview,
                )
            raise
        except (ReadTimeout, ConnectTimeout) as e:
            logger.warning("Timeout on %s %s: %s, will retry", method, path, e)
            raise

        if not isinstance(data, dict):
            return {}
        return data

    def list_users(
        self,
        org_uuid: str,
        *,
        page: str | None = None,
        limit: int = 500,
    ) -> PaginatedUsersResponse:
        params: list[tuple[str, str]] = [("limit", str(limit))]
        if page is not None:
            params.append(("page", page))
        raw = self._request(
            "GET",
            f"v1/compliance/organizations/{org_uuid}/users",
            params=params,
        )
        return PaginatedUsersResponse.model_validate(raw)

    def list_all_users(self, org_uuid: str) -> list[OrganizationUser]:
        users: list[OrganizationUser] = []
        page: str | None = None
        while True:
            response = self.list_users(org_uuid, page=page)
            users.extend(response.data)
            if not response.has_more:
                break
            page = response.next_page
            if page is None:
                break
        return users

    def list_chats(
        self,
        user_ids: list[str],
        *,
        updated_at_gte: str | None = None,
        updated_at_lte: str | None = None,
        after_id: str | None = None,
        limit: int = 100,
    ) -> PaginatedChatsResponse:
        params: list[tuple[str, str]] = [("limit", str(min(limit, 100)))]
        for user_id in user_ids:
            params.append(("user_ids[]", user_id))
        if updated_at_gte is not None:
            params.append(("updated_at.gte", updated_at_gte))
        if updated_at_lte is not None:
            params.append(("updated_at.lte", updated_at_lte))
        if after_id is not None:
            params.append(("after_id", after_id))
        raw = self._request("GET", "v1/compliance/apps/chats", params=params)
        return PaginatedChatsResponse.model_validate(raw)

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
                f"v1/compliance/apps/chats/{chat_id}/messages",
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
            return ChatMessagesResponse(
                chat_messages=[],
                has_more=False,
                first_id=None,
                last_id=None,
            )

        return merged
