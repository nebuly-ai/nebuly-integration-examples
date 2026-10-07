from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from copilot_sync.config import timestamp_str_to_datetime
from copilot_sync.graph_client import (
    _FILTER_EPSILON,
    AuditQueryError,
    GraphClient,
    _interactions_filter,
)
from copilot_sync.models import CopilotUser


def test_interactions_filter_maps_inclusive_bounds_to_strict_operators() -> None:
    gte = datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC)
    lte = datetime(2025, 6, 15, 11, 0, 0, tzinfo=UTC)

    result = _interactions_filter(gte, lte)

    assert result == (
        "createdDateTime gt 2025-06-15T09:59:59.999999Z "
        "and createdDateTime lt 2025-06-15T11:00:00.000001Z"
    )
    assert " ge " not in result
    assert " le " not in result


def test_interactions_filter_preserves_boundary_inclusivity() -> None:
    gte = datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC)
    lte = datetime(2025, 6, 15, 11, 0, 0, tzinfo=UTC)
    filter_str = _interactions_filter(gte, lte)

    lower_bound = timestamp_str_to_datetime(filter_str.split("gt ")[1].split(" and")[0])
    upper_bound = timestamp_str_to_datetime(filter_str.split("lt ")[1])

    assert lower_bound < gte
    assert gte - lower_bound == _FILTER_EPSILON
    assert upper_bound > lte
    assert upper_bound - lte == _FILTER_EPSILON

    assert lower_bound < gte < upper_bound
    assert lower_bound < lte < upper_bound


def test_list_copilot_users_selects_and_maps_org_fields() -> None:
    class FakeGraphUser:
        id = "user-abc"
        mail = "alice@contoso.com"
        user_principal_name = "alice@contoso.com"
        department = "Sales"
        job_title = "Account Executive"
        office_location = "HQ"
        city = "Seattle"
        country = None
        usage_location = "US"
        company_name = "Contoso Ltd"
        employee_hire_date = None

    class FakePage:
        odata_next_link = None

        def __init__(self) -> None:
            self.value = [FakeGraphUser()]

    captured_select: list[str] = []

    async def fake_get(*, request_configuration: object) -> FakePage:
        query_params = request_configuration.query_parameters  # type: ignore[attr-defined]
        captured_select.extend(query_params.select)
        return FakePage()

    mock_graph = AsyncMock()
    mock_graph.users.get = fake_get
    mock_graph_cred = AsyncMock()

    client = GraphClient(
        tenant_id="00000000-0000-0000-0000-000000000001",
        client_id="00000000-0000-0000-0000-000000000002",
        client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5c441c4b0cb",
    )

    with (
        patch(
            "copilot_sync.graph_client.ClientSecretCredential",
            return_value=mock_graph_cred,
        ),
        patch(
            "copilot_sync.graph_client.GraphServiceClient",
            return_value=mock_graph,
        ),
        patch.object(
            client,
            "_user_has_usable_copilot_license",
            new_callable=AsyncMock,
            return_value=True,
        ),
    ):
        users = asyncio.run(client.list_copilot_users())

    assert captured_select == [
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
    ]
    assert len(users) == 1
    assert users[0] == CopilotUser(
        id="user-abc",
        mail="alice@contoso.com",
        userPrincipalName="alice@contoso.com",
        department="Sales",
        jobTitle="Account Executive",
        officeLocation="HQ",
        city="Seattle",
        country=None,
        usageLocation="US",
        companyName="Contoso Ltd",
        employeeHireDate=None,
    )
    mock_graph_cred.close.assert_awaited_once()


def test_pagination_refreshes_token_before_each_page() -> None:
    client = GraphClient(
        tenant_id="00000000-0000-0000-0000-000000000001",
        client_id="00000000-0000-0000-0000-000000000002",
        client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5c441c4b0cb",
    )
    page1 = {
        "value": [{"id": "item_1"}],
        "@odata.nextLink": "https://graph.microsoft.com/next",
    }
    page2 = {"value": [{"id": "item_2"}]}
    auth_headers: list[str] = []

    async def fetch_page_side_effect(
        *,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        auth_headers.append(headers["Authorization"])
        if params is not None:
            return page1
        return page2

    with (
        patch.object(client, "_get_token", new_callable=AsyncMock) as get_token,
        patch.object(client, "_fetch_page", new_callable=AsyncMock) as fetch_page,
    ):
        get_token.side_effect = ["token_page_1", "token_page_2"]
        fetch_page.side_effect = fetch_page_side_effect

        items = asyncio.run(
            client.fetch_interactions(
                user_id="user_1",
                gte=datetime(2025, 6, 15, 8, 0, tzinfo=UTC),
                lte=datetime(2025, 6, 15, 12, 0, tzinfo=UTC),
            )
        )

    assert len(items) == 2
    assert get_token.call_count == 2
    assert auth_headers == ["Bearer token_page_1", "Bearer token_page_2"]


def test_wait_for_audit_query_succeeds_on_status_succeeded() -> None:
    client = GraphClient(
        tenant_id="00000000-0000-0000-0000-000000000001",
        client_id="00000000-0000-0000-0000-000000000002",
        client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5c441c4b0cb",
    )

    with (
        patch.object(
            client, "_get_token", new_callable=AsyncMock, return_value="token"
        ),
        patch.object(
            client,
            "_fetch_page",
            new_callable=AsyncMock,
            side_effect=[
                {"status": "running"},
                {"status": "succeeded"},
            ],
        ),
        patch("copilot_sync.graph_client.asyncio.sleep", new_callable=AsyncMock),
    ):
        asyncio.run(
            client.wait_for_audit_query(
                "query-1",
                poll_interval=1.0,
                query_timeout_seconds=60.0,
            )
        )


def test_wait_for_audit_query_raises_on_failed_status() -> None:
    client = GraphClient(
        tenant_id="00000000-0000-0000-0000-000000000001",
        client_id="00000000-0000-0000-0000-000000000002",
        client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5c441c4b0cb",
    )

    with (
        patch.object(
            client, "_get_token", new_callable=AsyncMock, return_value="token"
        ),
        patch.object(
            client,
            "_fetch_page",
            new_callable=AsyncMock,
            return_value={"status": "failed"},
        ),
        pytest.raises(AuditQueryError),
    ):
        asyncio.run(
            client.wait_for_audit_query(
                "query-1",
                poll_interval=1.0,
                query_timeout_seconds=60.0,
            )
        )


def test_fetch_copilot_audit_records_create_wait_and_page() -> None:
    client = GraphClient(
        tenant_id="00000000-0000-0000-0000-000000000001",
        client_id="00000000-0000-0000-0000-000000000002",
        client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5c441c4b0cb",
    )
    gte = datetime(2025, 6, 15, 8, 0, tzinfo=UTC)
    lte = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)
    page1 = {
        "value": [
            {
                "id": "audit-1",
                "createdDateTime": "2025-06-15T09:00:00Z",
                "auditData": {
                    "CopilotEventData": {
                        "ThreadId": "19:thread@thread.v2",
                        "Messages": [{"Id": "111", "isPrompt": True}],
                    }
                },
            }
        ],
        "@odata.nextLink": "https://graph.microsoft.com/next-records",
    }
    page2: dict[str, Any] = {"value": []}

    with (
        patch.object(
            client,
            "create_audit_query",
            new_callable=AsyncMock,
            return_value="query-abc",
        ) as create_query,
        patch.object(
            client, "wait_for_audit_query", new_callable=AsyncMock
        ) as wait_query,
        patch.object(
            client, "_get_token", new_callable=AsyncMock, return_value="token"
        ),
        patch.object(
            client,
            "_fetch_page",
            new_callable=AsyncMock,
            side_effect=[page1, page2],
        ),
    ):
        records = asyncio.run(client.fetch_copilot_audit_records(gte, lte))

    create_query.assert_awaited_once_with(gte, lte)
    wait_query.assert_awaited_once()
    assert len(records) == 1
    assert records[0].thread_id == "19:thread@thread.v2"
    assert records[0].message_ids == ["111"]


def test_user_has_usable_copilot_license_requires_success_service_plan() -> None:
    client = GraphClient(
        tenant_id="00000000-0000-0000-0000-000000000001",
        client_id="00000000-0000-0000-0000-000000000002",
        client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5f441c4b0cb",
    )
    sku = "639dec6b-bb19-468b-871c-c5f441c4b0cb"
    plan_id = "11111111-1111-1111-1111-111111111111"

    def assigned(disabled: list[str]) -> dict[str, Any]:
        return {
            "assignedLicenses": [
                {"skuId": sku, "disabledPlans": disabled},
            ],
        }

    def details(status: str) -> dict[str, Any]:
        return {
            "value": [
                {
                    "skuId": sku,
                    "servicePlans": [
                        {
                            "servicePlanId": plan_id,
                            "servicePlanName": "M365_COPILOT_BUSINESS_CHAT",
                            "provisioningStatus": status,
                        },
                    ],
                }
            ],
        }

    with (
        patch.object(
            client, "_get_token", new_callable=AsyncMock, return_value="token"
        ),
        patch.object(
            client,
            "_fetch_page",
            new_callable=AsyncMock,
            side_effect=[assigned([]), details("Disabled")],
        ),
    ):
        assert asyncio.run(client._user_has_usable_copilot_license("user-1")) is False

    with (
        patch.object(
            client, "_get_token", new_callable=AsyncMock, return_value="token"
        ),
        patch.object(
            client,
            "_fetch_page",
            new_callable=AsyncMock,
            side_effect=[assigned([]), details("Success")],
        ),
    ):
        assert asyncio.run(client._user_has_usable_copilot_license("user-1")) is True

    with (
        patch.object(
            client, "_get_token", new_callable=AsyncMock, return_value="token"
        ),
        patch.object(
            client,
            "_fetch_page",
            new_callable=AsyncMock,
            side_effect=[assigned([plan_id]), details("Success")],
        ),
    ):
        assert asyncio.run(client._user_has_usable_copilot_license("user-1")) is False

    def analytics_only() -> dict[str, Any]:
        return {
            "value": [
                {
                    "skuId": sku,
                    "servicePlans": [
                        {
                            "servicePlanId": plan_id,
                            "servicePlanName": "WORKPLACE_ANALYTICS_INSIGHTS_BACKEND",
                            "provisioningStatus": "Success",
                        },
                    ],
                }
            ],
        }

    with (
        patch.object(
            client, "_get_token", new_callable=AsyncMock, return_value="token"
        ),
        patch.object(
            client,
            "_fetch_page",
            new_callable=AsyncMock,
            side_effect=[assigned([]), analytics_only()],
        ),
    ):
        assert asyncio.run(client._user_has_usable_copilot_license("user-1")) is False
