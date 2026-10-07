from __future__ import annotations

import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

from langfuse_sync.http_retry import http_retry, parse_retry_after

if TYPE_CHECKING:
    import httpx

    from langfuse_sync.http_retry import RateLimiter

logger = logging.getLogger(__name__)


class SendResult(Enum):
    SENT = "sent"
    TOO_LARGE = "too_large"


class NebulyClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        endpoint: str,
        limiter: RateLimiter,
        *,
        dry_run: bool = False,
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._endpoint = endpoint
        self._limiter = limiter
        self._dry_run = dry_run

    @http_retry
    async def send_interaction(self, payload: dict[str, Any]) -> SendResult:
        if self._dry_run:
            return SendResult.SENT

        async with self._limiter.slot():
            resp = await self._client.post(
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
        if resp.status_code == 429:
            retry_after = parse_retry_after(resp.headers.get("Retry-After"))
            if retry_after is not None:
                self._limiter.pause(retry_after)
        if resp.is_error:
            logger.error(
                "Nebuly POST failed: status=%s body=%r",
                resp.status_code,
                resp.text[:500],
            )
            resp.raise_for_status()
        return SendResult.SENT
