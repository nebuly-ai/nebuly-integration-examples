from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from gemini_enterprise_sync.cache import SyncCache
from gemini_enterprise_sync.config import Config
from gemini_enterprise_sync.models import Session, SessionTurn
from gemini_enterprise_sync.sync import _sync_session

from .conftest import _session, plain_text_turn, thought_reply_turn

if TYPE_CHECKING:
    from pathlib import Path


def _config(tmp_path: Path) -> Config:
    return Config(
        nebuly_api_key="key",
        nebuly_endpoint="https://example.com/trace",
        gcp_project_id="p",
        gcp_location="eu",
        gcp_collection="default_collection",
        gcp_engine_id="e",
        gcp_max_requests_per_minute=600,
        settle_lag_seconds=60,
        anonymize=False,
        from_date=None,
        to_date=None,
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
    )


def _session_with_turn(turn: SessionTurn) -> Session:
    return _session(turns=[turn.model_dump(by_alias=True)])


def test_sync_session_sends_and_completes(tmp_path: Path) -> None:
    turn = plain_text_turn("hi", "hello", query_id="q-1")
    session = _session_with_turn(turn)
    cache = SyncCache(tmp_path / "db", "p/eu/default_collection/e")
    discovery = MagicMock()
    discovery.get_session.return_value = session
    nebuly = MagicMock()
    config = _config(tmp_path)
    counts = MagicMock()

    _sync_session(
        session,
        discovery=discovery,
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
    turn = plain_text_turn("hi", "hello", query_id="q-1")
    session = _session_with_turn(turn)
    cache = SyncCache(tmp_path / "db", "p/eu/default_collection/e")
    discovery = MagicMock()
    discovery.get_session.return_value = session
    nebuly = MagicMock()
    config = _config(tmp_path)
    counts = MagicMock()
    now = datetime(2026, 6, 29, 12, 50, 3, 500000, tzinfo=UTC)

    _sync_session(
        session,
        discovery=discovery,
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
    turn = thought_reply_turn(
        query_text="hello?",
        answer_text="",
        query_id="q-empty",
    )
    turn_dict = turn.model_dump(by_alias=True)
    turn_dict["detailedAssistAnswer"]["replies"] = turn_dict["detailedAssistAnswer"][
        "replies"
    ][:1]

    turn = SessionTurn.model_validate(turn_dict)
    session = _session_with_turn(turn)
    cache = SyncCache(tmp_path / "db", "p/eu/default_collection/e")
    discovery = MagicMock()
    discovery.get_session.return_value = session
    nebuly = MagicMock()
    config = _config(tmp_path)
    counts = MagicMock()

    _sync_session(
        session,
        discovery=discovery,
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
    assert state.last_sent_query_id == "q-empty"
