from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, cast

import httpx
from azure.identity.aio import ClientSecretCredential
from httpx import HTTPStatusError
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph.graph_service_client import GraphServiceClient
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from .config import datetime_to_timestamp_str
from .models import CopilotUser
from .utils import should_retry

logger = logging.getLogger(__name__)

_FILTER_EPSILON = timedelta(microseconds=1)

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
INTERACTIONS_PATH = (
    "https://graph.microsoft.com/v1.0/copilot/users/{user_id}"
    "/interactionHistory/getAllEnterpriseInteractions"
)
BATCH_TOP = 100


def _interactions_filter(gte: datetime, lte: datetime) -> str:
    """Build the createdDateTime filter.

    The Graph endpoint only supports strict gt/lt, so the inclusive [gte, lte]
    window is expressed by shifting each bound one tick outward.
    """
    gte_str = datetime_to_timestamp_str(gte - _FILTER_EPSILON)
    lte_str = datetime_to_timestamp_str(lte + _FILTER_EPSILON)
    return f"createdDateTime gt {gte_str} and createdDateTime lt {lte_str}"


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
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        copilot_sku: str,
        max_requests_per_minute: int = 600,
    ) -> None:
        self._copilot_sku = copilot_sku
        self._cred_kwargs = {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        self._cred = ClientSecretCredential(**self._cred_kwargs)
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
        )
        request_config = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
            query_parameters=query_params,
        )

        # Kiota closes async credentials after token fetch; scope the SDK to this call.
        graph_cred = ClientSecretCredential(**self._cred_kwargs)
        graph = GraphServiceClient(credentials=graph_cred, scopes=[GRAPH_SCOPE])
        try:
            page = await graph.users.get(request_configuration=request_config)
            while page is not None:
                if page.value:
                    for user in page.value:
                        if user.id is None:
                            logger.warning(
                                "User %s has no ID, skipping",
                                user.user_principal_name,
                            )
                            continue
                        users.append(
                            CopilotUser(
                                id=user.id,
                                mail=user.mail,
                                userPrincipalName=user.user_principal_name,
                            )
                        )

                if not page.odata_next_link:
                    break
                page = await graph.users.with_url(page.odata_next_link).get()
        finally:
            await graph_cred.close()

        return users

    @retry(
        retry=retry_if_exception(should_retry),
        stop=stop_after_attempt(10),
        wait=_retry_after_seconds,
        reraise=True,
    )
    async def _fetch_page(
        self,
        *,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._rate_limiter.wait()
        response = await self._http.get(url, headers=headers, params=params)
        if response.is_error:
            response.raise_for_status()
        return cast(dict[str, Any], response.json())

    async def fetch_interactions(
        self,
        *,
        user_id: str,
        gte: datetime,
        lte: datetime,
    ) -> list[dict[str, Any]]:
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        params: dict[str, Any] = {
            "$top": BATCH_TOP,
            "$filter": _interactions_filter(gte, lte),
        }
        url = INTERACTIONS_PATH.format(user_id=user_id)
        items: list[dict[str, Any]] = []

        data = await self._fetch_page(url=url, headers=headers, params=params)
        if data.get("value"):
            items.extend(data["value"])

        while next_link := data.get("@odata.nextLink"):
            token = await self._get_token()
            headers["Authorization"] = f"Bearer {token}"
            data = await self._fetch_page(url=next_link, headers=headers)
            if data.get("value"):
                items.extend(data["value"])

        return items
