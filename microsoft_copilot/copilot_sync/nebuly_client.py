from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .utils import should_retry

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger(__name__)


class NebulyClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        endpoint: str,
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._endpoint = endpoint

    @retry(
        retry=retry_if_exception(should_retry),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    async def send_interaction(self, payload: dict[str, Any]) -> None:
        resp = await self._client.post(
            self._endpoint,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        if resp.is_error:
            logger.error(
                "Nebuly POST failed: status=%s body=%r",
                resp.status_code,
                resp.text,
            )
            resp.raise_for_status()
