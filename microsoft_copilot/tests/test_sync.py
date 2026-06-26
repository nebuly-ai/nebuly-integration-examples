from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
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


def _ts(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2025, 6, 15, hour, minute, second, tzinfo=UTC)


def _config(
    tmp_path: Path,
    *,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    dry_run: bool = False,
) -> Config:
    return Config(
        azure_tenant_id="tenant_1",
        azure_client_id="client_1",
        azure_client_secret="secret_1",
        copilot_sku="639dec6b-bb19-468b-871c-c5c441c4b0cb",
        graph_max_requests_per_minute=1800,
        nebuly_api_key="nebuly_key",
        nebuly_endpoint="https://example.com/events",
        anonymize=False,
        from_date=from_date,
        to_date=to_date,
        cache_dir=tmp_path,
        dry_run=dry_run,
        verbose=False,
    )


def _interaction_dict(request_id: str = "req_1") -> dict[str, object]:
    return {
        "id": f"prompt_{request_id}",
        "requestId": request_id,
        "sessionId": "sess_1",
        "interactionType": "userPrompt",
        "conversationType": "appchat",
        "appClass": "IPM.SkypeTeams.Message.Copilot.Word",
        "locale": "en-US",
        "createdDateTime": "2025-06-15T10:00:00Z",
        "completedDateTime": "2025-06-15T10:00:00Z",
        "body": {"contentType": "text", "content": "hello"},
    }


def _interaction_dict_at(
    request_id: str,
    *,
    hour: int,
    minute: int = 0,
    second: int = 0,
) -> dict[str, object]:
    ts = f"2025-06-15T{hour:02d}:{minute:02d}:{second:02d}Z"
    data = _interaction_dict(request_id)
    data["createdDateTime"] = ts
    data["completedDateTime"] = ts
    return data


def _response_dict(request_id: str = "req_1") -> dict[str, object]:
    return {
        "id": f"response_{request_id}",
        "requestId": request_id,
        "sessionId": "sess_1",
        "interactionType": "aiResponse",
        "conversationType": "appchat",
        "appClass": "IPM.SkypeTeams.Message.Copilot.Word",
        "locale": "en-US",
        "createdDateTime": "2025-06-15T10:01:00Z",
        "completedDateTime": "2025-06-15T10:01:00Z",
        "body": {"contentType": "text", "content": "hi"},
    }


def _response_dict_at(
    request_id: str,
    *,
    hour: int,
    minute: int = 0,
    second: int = 0,
) -> dict[str, object]:
    ts = f"2025-06-15T{hour:02d}:{minute:02d}:{second:02d}Z"
    data = _response_dict(request_id)
    data["createdDateTime"] = ts
    data["completedDateTime"] = ts
    return data


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
    config = _config(tmp_path, dry_run=True, from_date=_ts(8))
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
    config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
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
        summary = asyncio.run(run_sync(config))

    assert summary.totals.users_failed == 1
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
    config = _config(tmp_path, from_date=_ts(8))
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


def _response_dict_html_no_card(
    request_id: str,
    *,
    hour: int,
    minute: int = 0,
) -> dict[str, object]:
    data = _response_dict_at(request_id, hour=hour, minute=minute)
    data["body"] = {
        "contentType": "html",
        "content": '<attachment id="x"></attachment>',
    }
    return data


def test_empty_output_turn_counted_as_empty(tmp_path: Path) -> None:
    config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
    user = [CopilotUser(id="user_a", mail="a@example.com")]
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=user)
    mock_graph.close = AsyncMock()
    mock_graph.fetch_interactions = AsyncMock(
        return_value=[
            _interaction_dict_at("req_empty", hour=10, minute=0),
            _response_dict_html_no_card("req_empty", hour=10, minute=1),
        ],
    )

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        summary = asyncio.run(run_sync(config))

    nebuly.send_interaction.assert_not_called()
    assert summary.totals.empty == 1
    assert summary.totals.sent == 0


def test_in_flight_turn_deferred_then_sent(tmp_path: Path) -> None:
    user = [CopilotUser(id="user_a", mail="a@example.com")]
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=user)
    mock_graph.close = AsyncMock()

    interactions = [
        _interaction_dict_at("req_x", hour=11, minute=59, second=30),
        _response_dict_at("req_x", hour=11, minute=59, second=50),
    ]

    run1_config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
    mock_graph.fetch_interactions = AsyncMock(return_value=interactions)

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        asyncio.run(run_sync(run1_config))

    nebuly.send_interaction.assert_not_called()
    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        coverage = cache.get_user_coverage("user_a")
        assert coverage is not None
        assert coverage.coverage_until == _ts(11, 59, 30) - timedelta(microseconds=1)
    finally:
        cache.close()

    run2_config = _config(tmp_path, from_date=None, to_date=_ts(13))
    mock_graph.fetch_interactions = AsyncMock(return_value=interactions)

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        asyncio.run(run_sync(run2_config))

    nebuly.send_interaction.assert_called_once()


def test_split_pair_watermark_holds_back_coverage(tmp_path: Path) -> None:
    user = [CopilotUser(id="user_a", mail="a@example.com")]
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=user)
    mock_graph.close = AsyncMock()

    run1_config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
    mock_graph.fetch_interactions = AsyncMock(
        return_value=[
            _interaction_dict_at("req_complete", hour=10, minute=0),
            _response_dict_at("req_complete", hour=10, minute=1),
            _interaction_dict_at("req_dangling", hour=11, minute=59),
        ],
    )

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        asyncio.run(run_sync(run1_config))

    nebuly.send_interaction.assert_called_once()
    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        coverage = cache.get_user_coverage("user_a")
        assert coverage is not None
        assert coverage.coverage_until == _ts(11, 59) - timedelta(microseconds=1)
    finally:
        cache.close()

    run2_config = _config(tmp_path, from_date=None, to_date=_ts(13))
    mock_graph.fetch_interactions = AsyncMock(
        return_value=[
            _interaction_dict_at("req_dangling", hour=11, minute=59),
            _response_dict_at("req_dangling", hour=12, minute=30),
        ],
    )

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        asyncio.run(run_sync(run2_config))

    nebuly.send_interaction.assert_called_once()
    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        coverage = cache.get_user_coverage("user_a")
        assert coverage is not None
        assert coverage.coverage_until == _ts(13)
    finally:
        cache.close()


def test_failed_turn_holds_back_coverage_then_resent(tmp_path: Path) -> None:
    user = [CopilotUser(id="user_a", mail="a@example.com")]
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=user)
    mock_graph.close = AsyncMock()
    mock_graph.fetch_interactions = AsyncMock(
        return_value=[
            _interaction_dict_at("req_fail", hour=10, minute=0),
            _response_dict_at("req_fail", hour=10, minute=1),
        ],
    )

    run1_config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock(side_effect=RuntimeError("send failed"))
        summary = asyncio.run(run_sync(run1_config))

    assert summary.totals.failed == 1
    assert summary.totals.sent == 0
    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        coverage = cache.get_user_coverage("user_a")
        assert coverage is not None
        assert coverage.coverage_until == _ts(10, 0) - timedelta(microseconds=1)
    finally:
        cache.close()

    run2_config = _config(tmp_path, from_date=None, to_date=_ts(13))
    nebuly.send_interaction = AsyncMock()
    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock()
        asyncio.run(run_sync(run2_config))

    nebuly.send_interaction.assert_called_once()
    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        coverage = cache.get_user_coverage("user_a")
        assert coverage is not None
        assert coverage.coverage_until == _ts(13)
    finally:
        cache.close()


def test_one_failed_turn_does_not_abort_interval(tmp_path: Path) -> None:
    config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
    user = [CopilotUser(id="user_a", mail="a@example.com")]
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=user)
    mock_graph.close = AsyncMock()
    mock_graph.fetch_interactions = AsyncMock(
        return_value=[
            _interaction_dict_at("req_fail", hour=10, minute=0),
            _response_dict_at("req_fail", hour=10, minute=1),
            _interaction_dict_at("req_ok", hour=10, minute=30),
            _response_dict_at("req_ok", hour=10, minute=31),
        ],
    )

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock(
            side_effect=[RuntimeError("send failed"), None],
        )
        summary = asyncio.run(run_sync(config))

    assert summary.totals.sent == 1
    assert summary.totals.failed == 1
    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        coverage = cache.get_user_coverage("user_a")
        assert coverage is not None
        assert coverage.coverage_until == _ts(10, 0) - timedelta(microseconds=1)
    finally:
        cache.close()


def test_earliest_hold_back_wins_across_categories(tmp_path: Path) -> None:
    config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
    user = [CopilotUser(id="user_a", mail="a@example.com")]
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=user)
    mock_graph.close = AsyncMock()
    mock_graph.fetch_interactions = AsyncMock(
        return_value=[
            _interaction_dict_at("req_fail", hour=10, minute=0),
            _response_dict_at("req_fail", hour=10, minute=1),
            _interaction_dict_at("req_dangling", hour=11, minute=59),
        ],
    )

    with (
        patch("copilot_sync.sync.GraphClient", return_value=mock_graph),
        patch("copilot_sync.sync.NebulyClient") as nebuly_cls,
    ):
        nebuly = nebuly_cls.return_value
        nebuly.send_interaction = AsyncMock(side_effect=RuntimeError("send failed"))
        summary = asyncio.run(run_sync(config))

    assert summary.totals.failed == 1
    cache = SyncCache(tmp_path / "sync_state.db", "tenant_1", dry_run=False)
    try:
        coverage = cache.get_user_coverage("user_a")
        assert coverage is not None
        assert coverage.coverage_until == _ts(10, 0) - timedelta(microseconds=1)
    finally:
        cache.close()


def test_per_turn_and_whole_user_counters_independent(tmp_path: Path) -> None:
    config = _config(tmp_path, from_date=_ts(8), to_date=_ts(12))
    mock_graph = MagicMock()
    mock_graph.list_copilot_users = AsyncMock(return_value=_users())
    mock_graph.close = AsyncMock()

    async def fetch_side_effect(
        user_id: str,
        gte: datetime,
        lte: datetime,
    ) -> list[dict[str, object]]:
        if user_id == "user_a":
            return [
                _interaction_dict_at("req_fail", hour=10, minute=0),
                _response_dict_at("req_fail", hour=10, minute=1),
            ]
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
        nebuly.send_interaction = AsyncMock(side_effect=RuntimeError("send failed"))
        summary = asyncio.run(run_sync(config))

    assert summary.totals.failed == 1
    assert summary.totals.users_failed == 1
