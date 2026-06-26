from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

from copilot_sync.config import timestamp_str_to_datetime
from copilot_sync.graph_client import _FILTER_EPSILON, GraphClient, _interactions_filter


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
