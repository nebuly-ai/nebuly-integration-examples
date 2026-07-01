from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gemini_enterprise_sync.cache import SyncCache

from .conftest import _session

if TYPE_CHECKING:
    from pathlib import Path


def _cache(
    tmp_path: Path, *, engine: str = "p/eu/default_collection/e", dry_run: bool = False
) -> SyncCache:
    return SyncCache(tmp_path / "sync_state.db", engine, dry_run=dry_run)


def test_should_skip_unchanged_session(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    session = _session()

    cache.mark_complete(session)
    assert cache.should_skip_fetch(session)


def test_changed_end_time_requires_fetch(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    session = _session()
    cache.mark_complete(session)

    updated = _session(end_time="2026-06-29T13:00:00.000000Z")
    assert not cache.should_skip_fetch(updated)


def test_checkpoint_advances_watermark(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    session = _session()
    turn_time = datetime(2026, 6, 29, 12, 50, 3, tzinfo=UTC)

    cache.checkpoint(session, turn_time=turn_time, query_id="query-plain")

    state = cache.get(session.session_id)
    assert state is not None
    assert state.last_sent_turn_time == "2026-06-29T12:50:03Z"
    assert state.last_sent_query_id == "query-plain"
    assert state.status == "partial"
