from __future__ import annotations

import logging
from enum import Enum
from typing import Any

import httpx
from httpx import HTTPStatusError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class SendResult(Enum):
    SENT = "sent"
    TOO_LARGE = "too_large"


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    return status == 429 or status >= 500


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
    def send_interaction(self, payload: dict[str, Any]) -> SendResult:
        if self._dry_run:
            return SendResult.SENT

        resp = self._client.post(
            self._endpoint,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        if resp.status_code == 413:
            logger.warning(
                "Skipping interaction conversation_id=%s: payload too large (413)",
                payload.get("interaction", {}).get("conversation_id"),
            )
            return SendResult.TOO_LARGE
        if resp.is_error:
            logger.error(
                "Nebuly POST failed: status=%s body=%r",
                resp.status_code,
                resp.text[:500],
            )
            resp.raise_for_status()
        return SendResult.SENT
