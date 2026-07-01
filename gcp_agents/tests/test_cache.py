from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gcp_agents_sync.cache import SyncCache
from gcp_agents_sync.models import Session

if TYPE_CHECKING:
    from pathlib import Path


def _session(
    session_id: str = "sess_1",
    *,
    update_time: str = "2026-06-29T12:50:07.000000Z",
    user_id: str = "user_1",
) -> Session:
    return Session.model_validate(
        {
            "name": f"projects/p/locations/l/reasoningEngines/e/sessions/{session_id}",
            "userId": user_id,
            "createTime": "2026-06-29T12:50:00.000000Z",
            "updateTime": update_time,
        }
    )


def _cache(
    tmp_path: Path, *, engine: str = "p/l/e", dry_run: bool = False
) -> SyncCache:
    return SyncCache(tmp_path / "sync_state.db", engine, dry_run=dry_run)


def test_should_skip_unchanged_session(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    session = _session()

    cache.mark_complete(session)
    assert cache.should_skip_fetch(session)


def test_changed_update_time_requires_fetch(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    session = _session()
    cache.mark_complete(session)

    updated = _session(update_time="2026-06-29T13:00:00.000000Z")
    assert not cache.should_skip_fetch(updated)


def test_checkpoint_advances_watermark(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    session = _session()
    event_time = datetime(2026, 6, 29, 12, 50, 3, tzinfo=UTC)

    cache.checkpoint(session, event_time=event_time, invocation_id="inv-1")

    state = cache.get(session.session_id)
    assert state is not None
    assert state.last_sent_event_time == "2026-06-29T12:50:03Z"
    assert state.last_sent_invocation_id == "inv-1"
    assert state.status == "partial"
