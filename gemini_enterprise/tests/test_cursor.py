from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from gemini_enterprise_sync.cursor import Cursor

if TYPE_CHECKING:
    from pathlib import Path


def test_advance_and_load_round_trip(tmp_path: Path) -> None:
    cursor_path = tmp_path / "cursor.json"
    cursor = Cursor(cursor_path)
    ts = datetime(2026, 7, 2, 16, 14, 38, 513765, tzinfo=UTC)
    cursor.advance(timestamp=ts, insert_id="abc123")
    loaded = Cursor(cursor_path).load()
    assert loaded.last_timestamp == ts
    assert loaded.last_insert_id == "abc123"
    assert not cursor_path.with_suffix(".json.tmp").exists()


def test_monotonic_guard_rejects_older_timestamp(tmp_path: Path) -> None:
    cursor = Cursor(tmp_path / "cursor.json")
    cursor.advance(
        timestamp=datetime(2026, 7, 2, 16, 0, 0, tzinfo=UTC),
        insert_id="later",
    )
    with pytest.raises(ValueError, match="cannot move backwards"):
        cursor.advance(
            timestamp=datetime(2026, 7, 1, 16, 0, 0, tzinfo=UTC),
            insert_id="earlier",
        )


def test_dry_run_keeps_state_in_memory(tmp_path: Path) -> None:
    cursor_path = tmp_path / "cursor.json"
    cursor = Cursor(cursor_path, dry_run=True)
    ts = datetime(2026, 7, 2, 16, 14, 38, tzinfo=UTC)
    cursor.advance(timestamp=ts, insert_id="dry")
    assert cursor.state.last_insert_id == "dry"
    assert not cursor_path.exists()
