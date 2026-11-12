from __future__ import annotations

import gzip
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from bedrock_sync.config import Config
from bedrock_sync.nebuly_client import PermanentRejectionError
from bedrock_sync.s3_client import LocalObjectStore
from bedrock_sync.state import DB_FILENAME, SyncState
from bedrock_sync.sync import (
    FirstRunRequiresFromDateError,
    GapAbortedError,
    SyncSummary,
    sync,
)

from tests.records import ACCOUNT, assistant, converse_record, user

if TYPE_CHECKING:
    from pathlib import Path

REGION = "eu-north-1"
ROOT = f"AWSLogs/{ACCOUNT}/BedrockModelInvocationLogs/{REGION}"
START = datetime(2026, 11, 11, 10, tzinfo=UTC)
_TOOL_USE = {"toolUse": {"toolUseId": "t1", "name": "search", "input": {"q": "x"}}}
_ASKED = {"role": "assistant", "content": [_TOOL_USE]}
_RESULT = {
    "role": "user",
    "content": [{"toolResult": {"toolUseId": "t1", "content": [{"text": "R"}]}}],
}


class FakeNebuly:
    """Records the payloads; ``fail_at`` / ``reject_at`` pick calls by index."""

    def __init__(
        self, *, fail_at: set[int] | None = None, reject_at: set[int] | None = None
    ) -> None:
        self.payloads: list[dict[str, Any]] = []
        self.calls = 0
        self._fail_at = fail_at or set()
        self._reject_at = reject_at or set()

    def send_interaction(self, payload: dict[str, Any]) -> None:
        call = self.calls
        self.calls += 1
        if call in self._fail_at:
            raise httpx.ConnectError("Nebuly unreachable")
        if call in self._reject_at:
            raise PermanentRejectionError(422)
        self.payloads.append(payload)

    @property
    def inputs(self) -> list[str]:
        return [p["interaction"]["input"] for p in self.payloads]


def _config(tmp_path: Path, **changes: Any) -> Config:  # noqa: ANN401
    config = Config(
        nebuly_api_key="nb-key",
        nebuly_endpoint="https://backend.example.test/trace_interaction",
        s3_bucket="bucket",
        s3_prefix="",
        aws_account_id=ACCOUNT,
        bedrock_region=REGION,
        aws_role_arn=None,
        aws_external_id=None,
        settle_lag_seconds=300,
        conversation_chain_ttl_days=30,
        anonymize=False,
        send_plain_end_user=True,
        user_hash_secret="",
        from_date=START,
        to_date=None,
        cache_dir=tmp_path / "cache",
        dry_run=False,
        verbose=False,
        force=False,
    )
    return replace(config, **changes)


def _record(
    request_id: str,
    at: datetime,
    messages: list[dict[str, Any]],
    answer: str | list[dict[str, Any]],
    *,
    stop: str = "end_turn",
) -> dict[str, Any]:
    return converse_record(
        messages,
        answer,
        stop=stop,
        request_id=request_id,
        timestamp=at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def _write(root: Path, delivered: datetime, records: list[dict[str, Any]]) -> None:
    key = (
        f"{ROOT}/{delivered:%Y/%m/%d/%H}/"
        f"{delivered:%Y%m%dT%H%M%S}000Z_{delivered:%H%M}.json.gz"
    )
    path = root / "logs" / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress("\n".join(json.dumps(r) for r in records).encode()))


def _question(root: Path, request_id: str, minute: int, text: str) -> None:
    at = START + timedelta(minutes=minute)
    _write(
        root, at + timedelta(minutes=1), [_record(request_id, at, [user(text)], "A")]
    )


def _run(
    tmp_path: Path,
    nebuly: FakeNebuly,
    now: datetime,
    **changes: Any,  # noqa: ANN401
) -> SyncSummary:
    return sync(
        _config(tmp_path, **changes),
        LocalObjectStore(tmp_path / "logs"),
        nebuly,
        now=now,
    )


def test_first_run_requires_from_date(tmp_path: Path) -> None:
    with pytest.raises(FirstRunRequiresFromDateError):
        _run(tmp_path, FakeNebuly(), START + timedelta(hours=1), from_date=None)


def test_sends_each_turn_once_across_runs(tmp_path: Path) -> None:
    _question(tmp_path, "q1", 0, "first")
    _question(tmp_path, "q2", 5, "second")
    nebuly = FakeNebuly()

    summary = _run(tmp_path, nebuly, START + timedelta(hours=1))
    assert nebuly.inputs == ["first", "second"]
    assert summary.totals.interactions_sent == 2
    assert summary.totals.files_read == 2

    _question(tmp_path, "q3", 70, "third")
    summary = _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)
    assert nebuly.inputs == ["first", "second", "third"]
    assert summary.totals.files_read == 1


def test_files_inside_the_settle_lag_wait_for_the_next_run(tmp_path: Path) -> None:
    _question(tmp_path, "q1", 0, "first")
    nebuly = FakeNebuly()

    # Delivered at 10:01, the window ends at 10:04 - 5 min.
    _run(
        tmp_path,
        nebuly,
        START + timedelta(minutes=4),
        from_date=START - timedelta(hours=1),
    )
    assert nebuly.inputs == []

    _run(tmp_path, nebuly, START + timedelta(hours=1), from_date=None)
    assert nebuly.inputs == ["first"]


def test_tool_turn_split_across_runs_is_sent_once_whole(tmp_path: Path) -> None:
    asked_at = START
    answered_at = START + timedelta(minutes=50)
    _write(
        tmp_path,
        asked_at + timedelta(minutes=1),
        [_record("c1", asked_at, [user("Q")], [_TOOL_USE], stop="tool_use")],
    )
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(minutes=40))
    assert nebuly.payloads == [], "the turn waits for its tool result"

    _write(
        tmp_path,
        answered_at + timedelta(minutes=1),
        [_record("c2", answered_at, [user("Q"), _ASKED, _RESULT], "done")],
    )
    _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)
    assert nebuly.inputs == ["Q"]
    assert nebuly.payloads[0]["interaction"]["output"] == "done"
    assert len(nebuly.payloads[0]["traces"]) >= 2


def test_follow_up_in_a_later_run_joins_the_conversation(tmp_path: Path) -> None:
    _question(tmp_path, "q1", 0, "first")
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=1))

    at = START + timedelta(minutes=70)
    _write(
        tmp_path,
        at + timedelta(minutes=1),
        [_record("q2", at, [user("first"), assistant("A"), user("next")], "B")],
    )
    _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)

    ids = [p["interaction"]["conversation_id"] for p in nebuly.payloads]
    assert ids == ["bedrock-q1", "bedrock-q1"]


def test_failed_post_stops_and_the_next_run_sends_the_rest(tmp_path: Path) -> None:
    _question(tmp_path, "q1", 0, "first")
    _question(tmp_path, "q2", 5, "second")
    now = START + timedelta(hours=1)

    failing = FakeNebuly(fail_at={1})
    summary = _run(tmp_path, failing, now)
    assert summary.stopped
    assert failing.inputs == ["first"]
    assert summary.totals.interactions_failed == 1

    nebuly = FakeNebuly()
    summary = _run(tmp_path, nebuly, now, from_date=None)
    assert nebuly.inputs == ["second"]
    assert summary.totals.interactions_already_sent == 1
    assert not summary.stopped


def test_rejected_turn_is_not_sent_again(tmp_path: Path) -> None:
    _question(tmp_path, "q1", 0, "first")
    now = START + timedelta(hours=1)

    summary = _run(tmp_path, FakeNebuly(reject_at={0}), now)
    assert summary.totals.interactions_rejected == 1
    assert not summary.stopped

    # Read again from scratch: the registry still knows the turn.
    nebuly = FakeNebuly()
    summary = _run(tmp_path, nebuly, now, from_date=START - timedelta(hours=1))
    assert nebuly.calls == 0
    assert summary.totals.interactions_already_sent == 1


def test_gap_needs_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _question(tmp_path, "q1", 0, "first")
    _run(tmp_path, FakeNebuly(), START + timedelta(hours=1))
    _question(tmp_path, "q2", 300, "later")
    later = START + timedelta(hours=4)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    with pytest.raises(GapAbortedError):
        _run(tmp_path, FakeNebuly(), START + timedelta(hours=6), from_date=later)

    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=6), from_date=later, force=True)
    assert nebuly.inputs == ["later"]


def test_dry_run_goes_on_from_the_real_state_and_changes_nothing(
    tmp_path: Path,
) -> None:
    _question(tmp_path, "q1", 0, "first")
    _run(tmp_path, FakeNebuly(), START + timedelta(hours=1))
    db = tmp_path / "cache" / DB_FILENAME
    _question(tmp_path, "q2", 70, "second")

    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None, dry_run=True)
    assert nebuly.inputs == ["second"], "starts from the saved cursor"

    state = SyncState(db, f"s3://bucket/{ROOT}", dry_run=False)
    cursor = state.get_cursor()
    state.close()
    assert cursor is not None
    assert cursor.coverage_until == START + timedelta(minutes=55)

    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)
    assert nebuly.inputs == ["second"]


def test_nothing_to_sync_when_the_window_is_covered(tmp_path: Path) -> None:
    _question(tmp_path, "q1", 0, "first")
    now = START + timedelta(hours=1)
    _run(tmp_path, FakeNebuly(), now)

    nebuly = FakeNebuly()
    summary = _run(tmp_path, nebuly, now, from_date=None)
    assert nebuly.calls == 0
    assert summary.totals.files_read == 0


def test_backfill_months_later_does_not_send_old_turns_again(tmp_path: Path) -> None:
    _question(tmp_path, "q1", 0, "first")
    _run(tmp_path, FakeNebuly(), START + timedelta(hours=1))
    # The turn was sent long ago, and other runs went by since.
    db = sqlite3.connect(tmp_path / "cache" / DB_FILENAME)
    db.execute("UPDATE emitted_interaction SET updated_at = '2020-01-01T00:00:00Z'")
    db.commit()
    db.close()
    _run(tmp_path, FakeNebuly(), START + timedelta(hours=2), from_date=None)

    nebuly = FakeNebuly()
    _run(
        tmp_path,
        nebuly,
        START + timedelta(hours=2),
        from_date=START - timedelta(hours=1),
    )
    assert nebuly.calls == 0


def test_range_before_the_coverage_needs_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _question(tmp_path, "q1", 0, "first")
    _run(tmp_path, FakeNebuly(), START + timedelta(hours=1))
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    with pytest.raises(GapAbortedError):
        _run(
            tmp_path,
            FakeNebuly(),
            START + timedelta(hours=1),
            from_date=START - timedelta(days=2),
            to_date=START - timedelta(days=1),
        )


def test_resume_from_an_hourly_checkpoint_with_an_open_tool_turn(
    tmp_path: Path,
) -> None:
    # 10:00 asks for a tool; 11:04 gets the answer; 11:10 is a new question.
    _write(
        tmp_path,
        START + timedelta(minutes=1),
        [_record("c1", START, [user("Q")], [_TOOL_USE], stop="tool_use")],
    )
    answered_at = START + timedelta(minutes=64)
    _write(
        tmp_path,
        answered_at + timedelta(minutes=1),
        [_record("c2", answered_at, [user("Q"), _ASKED, _RESULT], "done")],
    )
    _question(tmp_path, "q3", 70, "third")
    now = START + timedelta(hours=2)

    # The first turn is sent, the second fails: the run stops after the
    # checkpoint of 10:01, which still holds the turn waiting for its tool.
    failing = FakeNebuly(fail_at={1})
    assert _run(tmp_path, failing, now).stopped
    assert failing.inputs == ["Q"]

    nebuly = FakeNebuly()
    summary = _run(tmp_path, nebuly, now, from_date=None)
    assert summary.totals.files_read == 2, "goes on after the 10:01 file"
    assert nebuly.inputs == ["third"]
    assert summary.totals.interactions_already_sent == 1
    assert not summary.stopped
    assert failing.payloads[0]["interaction"]["output"] == "done"
