from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any, cast

import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError, RequestException
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from .config import datetime_to_timestamp_str
from .models import Session, SessionListResponse

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

logger = logging.getLogger(__name__)

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def _api_host(location: str) -> str:
    if location == "global":
        return "https://discoveryengine.googleapis.com/v1"
    return f"https://{location}-discoveryengine.googleapis.com/v1"


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, RequestException) and not isinstance(exc, HTTPError):
        return True
    if isinstance(exc, HTTPError):
        response = exc.response
        if response is None:
            return False
        return response.status_code == 429 or response.status_code >= 500
    return False


def _retry_after_seconds(retry_state: RetryCallState) -> float:
    if retry_state.outcome is None:
        return 60.0
    exc = retry_state.outcome.exception()
    if (
        isinstance(exc, HTTPError)
        and exc.response is not None
        and exc.response.status_code == 429
    ):
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


class DiscoveryClient:
    def __init__(
        self,
        project_id: str,
        location: str,
        collection: str,
        engine_id: str,
        *,
        max_requests_per_minute: int = 600,
        session: AuthorizedSession | None = None,
    ) -> None:
        self._project_id = project_id
        self._location = location
        self._collection = collection
        self._engine_id = engine_id
        self._host = _api_host(location)
        self._parent = (
            f"projects/{project_id}/locations/{location}"
            f"/collections/{collection}/engines/{engine_id}"
        )
        self._sessions_url = f"{self._host}/{self._parent}/sessions"
        self._rate_limiter = _RateLimiter(max_requests_per_minute)
        if session is not None:
            self._session = session
        else:
            credentials, _ = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
            self._session = AuthorizedSession(credentials)  # type: ignore[no-untyped-call]

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=_retry_after_seconds,
        reraise=True,
    )
    def _request(
        self, method: str, url: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        self._rate_limiter.wait()
        response = self._session.request(method, url, params=params)  # type: ignore[no-untyped-call]
        if response.status_code >= 400:
            logger.error(
                "Discovery Engine request failed: %s %s status=%s body=%r",
                method,
                url,
                response.status_code,
                response.text[:500],
            )
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    def list_sessions(
        self,
        *,
        since: datetime | None = None,
        order_by: str = "update_time desc",
        page_size: int = 100,
    ) -> Iterator[Session]:
        page_token: str | None = None
        while True:
            params: dict[str, Any] = {
                "pageSize": page_size,
                "orderBy": order_by,
            }
            if since is not None:
                params["filter"] = f'update_time > "{datetime_to_timestamp_str(since)}"'
            if page_token:
                params["pageToken"] = page_token
            data = self._request("GET", self._sessions_url, params=params)
            parsed = SessionListResponse.model_validate(data)
            yield from parsed.sessions
            page_token = parsed.next_page_token
            if not page_token:
                break

    def get_session(self, session: Session) -> Session:
        data = self._request(
            "GET",
            f"{self._host}/{session.name}",
            params={"includeAnswerDetails": "true"},
        )
        return Session.model_validate(data)
