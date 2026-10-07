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
from .models import CopilotAuditRecord, CopilotUser
from .utils import should_retry

logger = logging.getLogger(__name__)

_FILTER_EPSILON = timedelta(microseconds=1)

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
INTERACTIONS_PATH = (
    "https://graph.microsoft.com/v1.0/copilot/users/{user_id}"
    "/interactionHistory/getAllEnterpriseInteractions"
)
AUDIT_QUERIES_PATH = "https://graph.microsoft.com/v1.0/security/auditLog/queries"
AUDIT_RECORDS_TOP = 999
BATCH_TOP = 100
# Plans that gate enterprise Copilot (excludes e.g. WORKPLACE_ANALYTICS_* stubs).
_COPILOT_ENTERPRISE_SERVICE_PLAN_PREFIX = "M365_COPILOT_"


class AuditQueryError(RuntimeError):
    """Raised when a Graph audit log query fails or times out."""


class AuditFetchProgress:
    """Cross-query progress for async audit polling (shared across parallel chunks)."""

    _LOG_EVERY_N_POLLS = 10

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.started_at = 0.0
        self.polls = 0
        self.active_queries = 0
        self.total_rows = 0

    def mark_started(self, loop: asyncio.AbstractEventLoop) -> None:
        self.started_at = loop.time()

    async def query_started(self) -> None:
        async with self._lock:
            self.active_queries += 1

    async def query_finished(self, row_count: int) -> None:
        async with self._lock:
            self.active_queries -= 1
            self.total_rows += row_count

    async def query_failed(self) -> None:
        async with self._lock:
            self.active_queries -= 1

    async def register_poll(self, loop: asyncio.AbstractEventLoop) -> None:
        async with self._lock:
            self.polls += 1
            if self.polls % self._LOG_EVERY_N_POLLS != 0:
                return
            elapsed = loop.time() - self.started_at
            logger.info(
                "Audit queries polling: %d in flight, %d record(s) loaded, "
                "%.0fs elapsed",
                self.active_queries,
                self.total_rows,
                elapsed,
            )


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

    async def _user_has_usable_copilot_license(self, user_id: str) -> bool:
        """True when the Copilot SKU has at least one enabled, provisioned service plan.

        Requires at least one M365 Copilot feature plan (name prefix M365_COPILOT_) that
        has provisioningStatus Success and is not in assignedLicenses.disabledPlans.
        """
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        sku = self._copilot_sku.lower()
        user_url = (
            f"https://graph.microsoft.com/v1.0/users/{user_id}?$select=assignedLicenses"
        )
        user_data = await self._fetch_page(url=user_url, headers=headers)
        disabled_plan_ids: set[str] = set()
        for assignment in cast(
            list[dict[str, Any]],
            user_data.get("assignedLicenses") or [],
        ):
            if str(assignment.get("skuId", "")).lower() != sku:
                continue
            for plan_id in assignment.get("disabledPlans") or []:
                disabled_plan_ids.add(str(plan_id).lower())

        details_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/licenseDetails"
        data = await self._fetch_page(url=details_url, headers=headers)
        for detail in cast(list[dict[str, Any]], data.get("value") or []):
            if str(detail.get("skuId", "")).lower() != sku:
                continue
            for plan in cast(list[dict[str, Any]], detail.get("servicePlans") or []):
                plan_id = str(plan.get("servicePlanId", "")).lower()
                if plan_id in disabled_plan_ids:
                    continue
                plan_name = str(plan.get("servicePlanName", ""))
                if not plan_name.startswith(_COPILOT_ENTERPRISE_SERVICE_PLAN_PREFIX):
                    continue
                if str(plan.get("provisioningStatus", "")).lower() == "success":
                    return True
        return False

    async def list_copilot_users(self) -> list[CopilotUser]:
        users: list[CopilotUser] = []
        sku_filter = f"assignedLicenses/any(u:u/skuId eq {self._copilot_sku})"
        query_params = UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
            select=[
                "id",
                "displayName",
                "mail",
                "userPrincipalName",
                "department",
                "jobTitle",
                "officeLocation",
                "city",
                "country",
                "usageLocation",
                "companyName",
                "employeeHireDate",
            ],
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
                        if not await self._user_has_usable_copilot_license(user.id):
                            logger.warning(
                                "User %s has Copilot SKU but no provisioned service "
                                "plans — skipping",
                                user.user_principal_name,
                            )
                            continue
                        users.append(
                            CopilotUser(
                                id=user.id,
                                mail=user.mail,
                                userPrincipalName=user.user_principal_name,
                                department=user.department,
                                jobTitle=user.job_title,
                                officeLocation=user.office_location,
                                city=user.city,
                                country=user.country,
                                usageLocation=user.usage_location,
                                companyName=user.company_name,
                                employeeHireDate=user.employee_hire_date,
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

    @retry(
        retry=retry_if_exception(should_retry),
        stop=stop_after_attempt(10),
        wait=_retry_after_seconds,
        reraise=True,
    )
    async def _post_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        json_body: dict[str, Any],
    ) -> dict[str, Any]:
        await self._rate_limiter.wait()
        response = await self._http.post(url, headers=headers, json=json_body)
        if response.is_error:
            response.raise_for_status()
        return cast(dict[str, Any], response.json())

    async def create_audit_query(self, gte: datetime, lte: datetime) -> str:
        if gte > lte:
            raise ValueError(
                f"Audit query start ({datetime_to_timestamp_str(gte)}) "
                f"cannot be after end ({datetime_to_timestamp_str(lte)})"
            )
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        body = {
            "displayName": (
                f"nebuly-copilot-audit-{datetime_to_timestamp_str(gte)}-"
                f"{datetime_to_timestamp_str(lte)}"
            ),
            "filterStartDateTime": datetime_to_timestamp_str(gte),
            "filterEndDateTime": datetime_to_timestamp_str(lte),
            "operationFilters": ["CopilotInteraction"],
        }
        data = await self._post_json(
            url=AUDIT_QUERIES_PATH, headers=headers, json_body=body
        )
        query_id = data.get("id")
        if not query_id:
            raise AuditQueryError("Audit query creation response missing id")
        return cast(str, query_id)

    async def wait_for_audit_query(
        self,
        query_id: str,
        *,
        poll_interval: float = 30.0,
        query_timeout_seconds: float = 3600.0,
        progress: AuditFetchProgress | None = None,
    ) -> None:
        url = f"{AUDIT_QUERIES_PATH}/{query_id}"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + query_timeout_seconds
        while True:
            token = await self._get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            data = await self._fetch_page(url=url, headers=headers)
            status = (data.get("status") or "").lower()
            if status == "succeeded":
                return
            if status in {"failed", "cancelled"}:
                raise AuditQueryError(
                    f"Audit query {query_id} ended with status {status}"
                )
            if loop.time() >= deadline:
                raise AuditQueryError(
                    f"Audit query {query_id} timed out after {query_timeout_seconds}s"
                )
            if progress is not None:
                await progress.register_poll(loop)
            await asyncio.sleep(poll_interval)

    async def fetch_audit_records(self, query_id: str) -> list[dict[str, Any]]:
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        params: dict[str, Any] = {"$top": AUDIT_RECORDS_TOP}
        url = f"{AUDIT_QUERIES_PATH}/{query_id}/records"
        items: list[dict[str, Any]] = []

        data = await self._fetch_page(url=url, headers=headers, params=params)
        if data.get("value"):
            items.extend(cast(list[dict[str, Any]], data["value"]))

        while next_link := data.get("@odata.nextLink"):
            token = await self._get_token()
            headers["Authorization"] = f"Bearer {token}"
            data = await self._fetch_page(url=next_link, headers=headers)
            if data.get("value"):
                items.extend(cast(list[dict[str, Any]], data["value"]))

        return items

    async def fetch_copilot_audit_records(
        self,
        gte: datetime,
        lte: datetime,
        *,
        poll_interval: float = 30.0,
        query_timeout_seconds: float = 3600.0,
        progress: AuditFetchProgress | None = None,
    ) -> list[CopilotAuditRecord]:
        if progress is not None:
            await progress.query_started()
        try:
            query_id = await self.create_audit_query(gte, lte)
            await self.wait_for_audit_query(
                query_id,
                poll_interval=poll_interval,
                query_timeout_seconds=query_timeout_seconds,
                progress=progress,
            )
            raw_records = await self.fetch_audit_records(query_id)
            records = [
                CopilotAuditRecord.model_validate(record) for record in raw_records
            ]
        except Exception:
            if progress is not None:
                await progress.query_failed()
            raise
        else:
            if progress is not None:
                await progress.query_finished(len(records))
            return records

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
