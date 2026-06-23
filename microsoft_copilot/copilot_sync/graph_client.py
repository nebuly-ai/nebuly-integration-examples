from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import httpx
from azure.identity.aio import ClientSecretCredential
from httpx import HTTPStatusError
from msgraph import GraphServiceClient
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from .config import datetime_to_timestamp_str
from .models import CopilotUser

if TYPE_CHECKING:
    from datetime import datetime

logger = logging.getLogger(__name__)

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
INTERACTIONS_PATH = (
    "https://graph.microsoft.com/v1.0/copilot/users/{user_id}"
    "/interactionHistory/getAllEnterpriseInteractions"
)
BATCH_TOP = 100


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    return status == 429 or status >= 500


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


class _AsyncRateLimiter:
    def __init__(self, max_requests_per_minute: int) -> None:
        self._min_interval = 60.0 / max_requests_per_minute
        self._lock = asyncio.Lock()
        self._last_request_at = 0.0

    async def wait(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            elapsed = now - self._last_request_at
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_request_at = loop.time()


class GraphClient:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        copilot_sku: str,
        *,
        max_requests_per_minute: int = 600,
    ) -> None:
        self._copilot_sku = copilot_sku
        self._cred = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        self._graph = GraphServiceClient(
            credentials=self._cred,
            scopes=[GRAPH_SCOPE],
        )
        self._http = httpx.AsyncClient()
        self._rate_limiter = _AsyncRateLimiter(max_requests_per_minute)

    async def close(self) -> None:
        await self._cred.close()
        await self._http.aclose()

    async def _get_token(self) -> str:
        token = await self._cred.get_token(GRAPH_SCOPE)
        return token.token

    async def list_copilot_users(self) -> list[CopilotUser]:
        users: list[CopilotUser] = []
        sku_filter = f"assignedLicenses/any(u:u/skuId eq {self._copilot_sku})"
        query_params = UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
            select=["id", "displayName", "mail", "userPrincipalName"],
            filter=sku_filter,
            top=BATCH_TOP,
            count=True,
        )
        request_config = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
            query_parameters=query_params,
        )
        request_config.headers.add("ConsistencyLevel", "eventual")

        page = await self._graph.users.get(request_configuration=request_config)
        while page is not None:
            if page.value:
                users.extend(
                    [
                        CopilotUser(
                            id=user.id or "",
                            mail=user.mail,
                            # I'm using the alias name to make mypy happy
                            userPrincipalName=user.user_principal_name,
                        )
                        for user in page.value
                    ]
                )
            if not page.odata_next_link:
                break
            page = await self._graph.users.with_url(page.odata_next_link).get()

        return users

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=_retry_after_seconds,
        reraise=True,
    )
    async def _fetch_page(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._rate_limiter.wait()
        response = await self._http.get(url, headers=headers, params=params)
        if response.is_error:
            response.raise_for_status()
        return response.json()

    async def fetch_interactions(
        self,
        user_id: str,
        gte: datetime,
        lte: datetime,
    ) -> list[dict[str, Any]]:
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        gte_str = datetime_to_timestamp_str(gte)
        lte_str = datetime_to_timestamp_str(lte)
        params: dict[str, Any] = {
            "$top": BATCH_TOP,
            "$filter": (
                f"createdDateTime ge {gte_str} and createdDateTime le {lte_str}"
            ),
        }
        url = INTERACTIONS_PATH.format(user_id=user_id)
        items: list[dict[str, Any]] = []

        data = await self._fetch_page(url, headers=headers, params=params)
        if data.get("value"):
            items.extend(data["value"])

        while next_link := data.get("@odata.nextLink"):
            data = await self._fetch_page(next_link, headers=headers)
            if data.get("value"):
                items.extend(data["value"])

        return items
