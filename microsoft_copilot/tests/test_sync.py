from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from copilot_sync.cache import SyncCache
from copilot_sync.config import Config
from copilot_sync.models import CopilotUser
from copilot_sync.sync import FirstRunRequiresFromDateError, run_sync

if TYPE_CHECKING:
    from pathlib import Path


def _ts(hour: int) -> datetime:
    return datetime(2025, 6, 15, hour, 0, tzinfo=UTC)


def _config(
    tmp_path: Path,
    *,
    from_date: datetime | None = None,
    dry_run: bool = False,
) -> Config:
    if from_date is None:
        from_date = _ts(8)

    return Config(
        azure_tenant_id="tenant_1",
        azure_client_id="client_1",
        azure_client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5c441c4b0cb",
        graph_max_requests_per_minute=600,
        ingestion_lag_minutes=15,
        nebuly_api_key="nebuly_key",
        nebuly_endpoint="https://example.com/events",
        anonymize=False,
        from_date=from_date,
        to_date=_ts(12),
        cache_dir=tmp_path,
        dry_run=dry_run,
        verbose=False,
    )


def _interaction_dict(request_id: str = "req_1") -> dict[str, object]:
    return {
        "requestId": request_id,
        "sessionId": "sess_1",
        "interactionType": "userPrompt",
        "createdDateTime": "2025-06-15T10:00:00Z",
        "body": {"contentType": "text", "content": "hello"},
    }


def _response_dict(request_id: str = "req_1") -> dict[str, object]:
    return {
        "requestId": request_id,
        "sessionId": "sess_1",
        "interactionType": "aiResponse",
        "createdDateTime": "2025-06-15T10:01:00Z",
        "body": {"contentType": "text", "content": "hi"},
    }


def _users() -> list[CopilotUser]:
    return [
        CopilotUser(id="user_a", mail="a@example.com"),
        CopilotUser(id="user_b", mail="b@example.com"),
    ]


def test_first_run_without_from_date_raises(tmp_path: Path) -> None:
    config = _config(tmp_path, from_date=None)

    with pytest.raises(FirstRunRequiresFromDateError):
        asyncio.run(run_sync(config))


def test_dry_run_sends_nothing(tmp_path: Path) -> None:
    config = _config(tmp_path, dry_run=True)
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=_users())
    mock_graph.fetch_interactions = AsyncMock(
        return_value=[_interaction_dict(), _response_dict()],
    )
    mock_graph.close = AsyncMock()

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        asyncio.run(run_sync(config))

    nebuly.send_interaction.assert_not_called()


def test_failed_user_does_not_advance_coverage_earlier_user_committed(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=_users())
    mock_graph.close = AsyncMock()

    async def fetch_side_effect(
        user_id: str,
        gte: datetime,
        lte: datetime,
    ) -> list[dict[str, object]]:
        if user_id == "user_a":
            return [_interaction_dict("req_a"), _response_dict("req_a")]
        raise httpx.HTTPStatusError(
            "fail",
            request=MagicMock(),
            response=MagicMock(status_code=500),
        )

    mock_graph.fetch_interactions = AsyncMock(side_effect=fetch_side_effect)

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        with pytest.raises(httpx.HTTPStatusError):
            asyncio.run(run_sync(config))

    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        user_a = cache.get_user_coverage("user_a")
        user_b = cache.get_user_coverage("user_b")
        assert user_a is not None
        assert user_a.coverage_from == _ts(8)
        assert user_a.coverage_until == _ts(12)
        assert user_b is None
    finally:
        cache.close()


def test_403_skips_user_without_advancing_coverage(tmp_path: Path) -> None:
    config = _config(tmp_path)
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=_users())
    mock_graph.close = AsyncMock()

    async def fetch_side_effect(
        user_id: str,
        gte: datetime,
        lte: datetime,
    ) -> list[dict[str, object]]:
        if user_id == "user_a":
            return [_interaction_dict("req_a"), _response_dict("req_a")]
        raise httpx.HTTPStatusError(
            "forbidden",
            request=MagicMock(),
            response=MagicMock(status_code=403),
        )

    mock_graph.fetch_interactions = AsyncMock(side_effect=fetch_side_effect)

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        asyncio.run(run_sync(config))

    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        assert cache.get_user_coverage("user_a") is not None
        assert cache.get_user_coverage("user_b") is None
    finally:
        cache.close()
