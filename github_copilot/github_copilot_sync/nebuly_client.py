from __future__ import annotations

import logging
from typing import Any

import httpx
from httpx import HTTPStatusError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class PermanentRejectionError(Exception):
    """Nebuly refused the payload with a non-retryable 4xx."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"Nebuly rejected payload with status {status_code}")


def is_permanent_status(status: int) -> bool:
    return 400 <= status < 500 and status not in {408, 429}


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    return status in {408, 429} or status >= 500


class NebulyClient:
    def __init__(
        self,
        client: httpx.Client,
        api_key: str,
        endpoint: str,
        *,
        dry_run: bool = False,
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._endpoint = endpoint
        self._dry_run = dry_run

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    def send_interaction(self, payload: dict[str, Any]) -> None:
        if self._dry_run:
            return

        resp = self._client.post(
            self._endpoint,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        if is_permanent_status(resp.status_code):
            logger.error(
                "Nebuly rejected payload: status=%s body=%r",
                resp.status_code,
                resp.text[:500],
            )
            raise PermanentRejectionError(resp.status_code)
        if resp.is_error:
            logger.error(
                "Nebuly POST failed: status=%s body=%r",
                resp.status_code,
                resp.text[:500],
            )
            resp.raise_for_status()
