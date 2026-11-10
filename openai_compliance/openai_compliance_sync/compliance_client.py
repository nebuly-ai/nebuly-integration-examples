from __future__ import annotations

import json
import logging
import threading
import time
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Iterator

import httpx
from httpx import HTTPStatusError
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from .models import LogFileList

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 300.0

EVENT_CONVERSATION_MESSAGE = "CONVERSATION_MESSAGE"
EVENT_CODEX_LOG = "CODEX_LOG"
EVENT_APP_LOG = "APP_LOG"


class SourceUnavailableError(Exception):
    """Compliance API reports this source is disabled for the principal."""


def principal_scope_segment(principal_id: str) -> str:
    if principal_id.startswith("org-"):
        return "organizations"
    return "workspaces"


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    if status == 429:
        return True
    return status >= 500


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
        principal_id: str,
        *,
        max_requests_per_minute: int = 200,
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._principal_id = principal_id
        self._scope = principal_scope_segment(principal_id)
        self._rate_limiter = _RateLimiter(max_requests_per_minute)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._api_key}"}

    def _logs_collection_path(self) -> str:
        return f"{self._scope}/{self._principal_id}/logs"

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
        follow_redirects: bool = True,
    ) -> httpx.Response:
        self._rate_limiter.wait()
        headers = self._headers()
        try:
            if params is None:
                resp = self._client.request(
                    method,
                    path,
                    headers=headers,
                    follow_redirects=follow_redirects,
                )
            else:
                resp = self._client.request(
                    method,
                    path,
                    params=cast(Any, params),
                    headers=headers,
                    follow_redirects=follow_redirects,
                )
            resp.raise_for_status()
        except HTTPStatusError as e:
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
        return resp

    def list_log_files(
        self,
        event_type: str,
        *,
        after: str | None = None,
        limit: int = 100,
    ) -> LogFileList:
        params: list[tuple[str, str]] = [
            ("event_type", event_type),
            ("limit", str(limit)),
        ]
        if after is not None:
            params.append(("after", after))
        try:
            resp = self._request("GET", self._logs_collection_path(), params=params)
        except HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise SourceUnavailableError(
                    f"Event type {event_type!r} is not available."
                ) from exc
            raise
        data = resp.json()
        if not isinstance(data, dict):
            return LogFileList()
        return LogFileList.model_validate(data)

    def iter_log_files(
        self,
        event_type: str,
        *,
        after: str | None = None,
        limit: int = 100,
    ) -> Iterator[LogFileList]:
        cursor = after
        while True:
            page = self.list_log_files(event_type, after=cursor, limit=limit)
            yield page
            if not page.has_more:
                break
            if page.last_end_time is None:
                break
            cursor = page.last_end_time

    def download_log_file(self, file_id: str) -> Iterator[dict[str, Any]]:
        path = f"{self._logs_collection_path()}/{file_id}"
        resp = self._request("GET", path, follow_redirects=True)
        for line_no, line in enumerate(resp.text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError:
                logger.warning(
                    "Skipping invalid JSONL line %s in log file %s",
                    line_no,
                    file_id,
                )
                continue
            if isinstance(row, dict):
                yield row
