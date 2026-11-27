"""What the runtime state adds to the shared store (tests/shared/test_store.py)."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from bedrock_sync.runtime.grouping import CallRef
from bedrock_sync.runtime.state import (
    QUEUED,
    Closure,
    Cursor,
    RuntimeState,
    StateError,
)

if TYPE_CHECKING:
    from pathlib import Path

SOURCE = "s3://bucket/AWSLogs/1/BedrockModelInvocationLogs/eu-north-1"
START = datetime(2026, 11, 11, 10, tzinfo=UTC)
LATER = datetime(2026, 11, 11, 11, tzinfo=UTC)
PAYLOAD = {"interaction": {"time_start": "2026-11-11T10:00:00Z"}, "traces": []}


def _state(tmp_path: Path, **kwargs: object) -> RuntimeState:
    return RuntimeState(tmp_path / "state.db", SOURCE, **kwargs)  # type: ignore[arg-type]


def _checkpoint(state: RuntimeState, *closures: Closure) -> None:
    state.checkpoint(
        Cursor(START, LATER, "key-2"), [CallRef("r1", "key-1", "open")], list(closures)
    )


def test_checkpoint_round_trip(tmp_path: Path) -> None:
    with _state(tmp_path) as state:
        assert state.cursor() is None
        _checkpoint(
            state,
            Closure("i-1", QUEUED, PAYLOAD),
            Closure("i-2", "empty_interaction"),
        )

    with _state(tmp_path) as state:
        assert state.cursor() == Cursor(START, LATER, "key-2")
        assert state.pending_calls() == [CallRef("r1", "key-1", "open")]
        assert state.is_closed("i-1")
        assert state.is_closed("i-2")  # skipped, still closed
        assert not state.is_closed("i-3")
        [delivery] = state.pending_deliveries()
        assert delivery.interaction_key == "i-1"
        assert delivery.payload == PAYLOAD


def test_dry_run_checkpoint_changes_nothing(tmp_path: Path) -> None:
    with _state(tmp_path) as state:
        _checkpoint(state)
    with _state(tmp_path, dry_run=True) as state:
        state.checkpoint(
            Cursor(START, START, None), [], [Closure("i-9", QUEUED, PAYLOAD)]
        )
    with _state(tmp_path) as state:
        assert state.cursor() == Cursor(START, LATER, "key-2")
        assert not state.is_closed("i-9")


def test_other_log_source_is_refused(tmp_path: Path) -> None:
    _state(tmp_path).close()
    with pytest.raises(StateError, match="another log source"):
        RuntimeState(tmp_path / "state.db", "s3://other")


def test_reset_keeps_closed_interactions_and_outbox(tmp_path: Path) -> None:
    with _state(tmp_path) as state:
        _checkpoint(state, Closure("i-1", QUEUED, PAYLOAD))
        state.reset()
        assert state.cursor() is None
        assert state.pending_calls() == []
        assert state.is_closed("i-1")
        assert len(state.pending_deliveries()) == 1


def test_closing_the_same_interaction_twice_fails_atomically(tmp_path: Path) -> None:
    with _state(tmp_path) as state:
        _checkpoint(state, Closure("i-1", QUEUED, PAYLOAD))
        with pytest.raises(sqlite3.IntegrityError):
            state.checkpoint(
                Cursor(START, START, None), [], [Closure("i-1", QUEUED, PAYLOAD)]
            )
        assert state.cursor() == Cursor(START, LATER, "key-2")
