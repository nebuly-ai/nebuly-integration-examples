from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from gcp_agents_sync.cache import SyncCache
from gcp_agents_sync.config import Config
from gcp_agents_sync.models import Event, Session
from gcp_agents_sync.sync import _sync_session

if TYPE_CHECKING:
    from pathlib import Path


def _session(session_id: str = "sess_1") -> Session:
    return Session.model_validate(
        {
            "name": f"projects/p/locations/l/reasoningEngines/e/sessions/{session_id}",
            "userId": "user_1",
            "createTime": "2026-06-29T12:50:00.000000Z",
            "updateTime": "2026-06-29T12:50:07.000000Z",
        }
    )


def _turn_events(invocation: str, user_text: str, reply: str) -> list[Event]:
    return [
        Event.model_validate(
            {
                "author": "user",
                "invocationId": invocation,
                "timestamp": "2026-06-29T12:50:01.000000Z",
                "content": {"role": "user", "parts": [{"text": user_text}]},
            }
        ),
        Event.model_validate(
            {
                "author": "agent",
                "invocationId": invocation,
                "timestamp": "2026-06-29T12:50:03.000000Z",
                "content": {"role": "model", "parts": [{"text": reply}]},
            }
        ),
    ]


def _config(tmp_path: Path) -> Config:
    return Config(
        nebuly_api_key="key",
        nebuly_endpoint="https://example.com/trace",
        gcp_project_id="p",
        gcp_location="l",
        gcp_reasoning_engine_id="e",
        gcp_max_requests_per_minute=600,
        settle_lag_seconds=60,
        anonymize=False,
        from_date=None,
        to_date=None,
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
    )


def test_sync_session_sends_and_completes(tmp_path: Path) -> None:
    cache = SyncCache(tmp_path / "db", "p/l/e")
    gcp = MagicMock()
    gcp.list_events.return_value = _turn_events("inv-1", "hi", "hello")
    nebuly = MagicMock()
    config = _config(tmp_path)
    counts = MagicMock()

    _sync_session(
        _session(),
        gcp=gcp,
        nebuly=nebuly,
        cache=cache,
        config=config,
        counts=counts,
        now=datetime(2026, 6, 29, 13, 0, tzinfo=UTC),
    )

    nebuly.send_interaction.assert_called_once()
    state = cache.get("sess_1")
    assert state is not None
    assert state.status == "complete"


def test_sync_session_holds_back_recent_turn(tmp_path: Path) -> None:
    cache = SyncCache(tmp_path / "db", "p/l/e")
    gcp = MagicMock()
    gcp.list_events.return_value = _turn_events("inv-1", "hi", "hello")
    nebuly = MagicMock()
    config = _config(tmp_path)
    counts = MagicMock()
    now = datetime(2026, 6, 29, 12, 50, 3, 500000, tzinfo=UTC)

    _sync_session(
        _session(),
        gcp=gcp,
        nebuly=nebuly,
        cache=cache,
        config=config,
        counts=counts,
        now=now,
    )

    nebuly.send_interaction.assert_not_called()
    state = cache.get("sess_1")
    assert state is not None
    assert state.status == "partial"


def test_sync_session_skips_empty_output_turn(tmp_path: Path) -> None:
    cache = SyncCache(tmp_path / "db", "p/l/e")
    gcp = MagicMock()
    gcp.list_events.return_value = [
        Event.model_validate(
            {
                "author": "user",
                "invocationId": "inv-1",
                "timestamp": "2026-06-29T12:50:01.000000Z",
                "content": {"role": "user", "parts": [{"text": "hi"}]},
            }
        )
    ]
    nebuly = MagicMock()
    config = _config(tmp_path)
    counts = MagicMock()

    _sync_session(
        _session(),
        gcp=gcp,
        nebuly=nebuly,
        cache=cache,
        config=config,
        counts=counts,
        now=datetime(2026, 6, 29, 13, 0, tzinfo=UTC),
    )

    nebuly.send_interaction.assert_not_called()
    state = cache.get("sess_1")
    assert state is not None
    assert state.status == "complete"
    assert state.last_sent_invocation_id == "inv-1"
