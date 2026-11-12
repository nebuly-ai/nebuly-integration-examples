from __future__ import annotations

import gzip
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
from bedrock_sync.grouping import ConversationSource, Grouper, Turn
from bedrock_sync.models import Invocation
from bedrock_sync.parser import parse_record
from bedrock_sync.s3_client import (
    LocalObjectStore,
    LogObject,
    LogSource,
    parse_delivered_at,
)
from bedrock_sync.state import DB_FILENAME, Cursor, SyncState
from bedrock_sync.sync import reload_calls

from tests.records import assistant, converse_record, user

if TYPE_CHECKING:
    from pathlib import Path

ROOT = "logs/AWSLogs/123456789012/BedrockModelInvocationLogs/eu-north-1"
LOG_SOURCE = f"s3://bucket/{ROOT}"
_TOOL_USE = {"toolUse": {"toolUseId": "t1", "name": "search", "input": {"q": "x"}}}
_TOOL_RESULT = {
    "role": "user",
    "content": [{"toolResult": {"toolUseId": "t1", "content": [{"text": "R"}]}}],
}
_ASKED = {"role": "assistant", "content": [_TOOL_USE]}


def _record(
    request_id: str,
    minute: int,
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
        timestamp=f"2026-11-11T10:{minute:02d}:00Z",
    )


def _write(root: Path, minute: int, records: list[dict[str, Any]]) -> str:
    """A log file delivered one minute after ``minute``."""
    key = f"{ROOT}/2026/11/11/10/20261111T10{minute + 1:02d}00000Z_f{minute}.json.gz"
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(r) for r in records)
    path.write_bytes(gzip.compress(payload.encode()))
    return key


class _Run:
    """One sync run over some log files, with the state of the run before."""

    def __init__(self, root: Path, *, reorder_minutes: int = 0) -> None:
        self.source = LogSource(LocalObjectStore(root), root_prefix=ROOT)
        self.state = SyncState(root / DB_FILENAME, LOG_SOURCE, dry_run=False)
        self.grouper = Grouper(reorder_window=timedelta(minutes=reorder_minutes))
        snapshot = self.state.load_snapshot()
        self.grouper.restore(snapshot, reload_calls(self.source, snapshot))
        self.turns: list[Turn] = []

    def read(self, *keys: str) -> None:
        for key in keys:
            for record in self.source.read_object(_obj(key)):
                parsed = parse_record(record.record, source_key=record.key)
                assert isinstance(parsed, Invocation)
                self.grouper.add(parsed)
            delivered = parse_delivered_at(key)
            assert delivered is not None
            self.grouper.advance(delivered)
            self.turns += self.grouper.drain_complete()

    def checkpoint(self, last_key: str) -> None:
        delivered = parse_delivered_at(last_key)
        assert delivered is not None
        cursor = Cursor(_START, delivered, last_key)
        self.state.save_checkpoint(cursor, self.grouper.snapshot())
        self.state.close()

    def finish(self) -> list[Turn]:
        self.grouper.flush()
        self.turns += self.grouper.drain_complete()
        self.state.close()
        return self.turns


_START = datetime(2026, 11, 11, 10, tzinfo=UTC)


def _obj(key: str) -> LogObject:
    delivered = parse_delivered_at(key)
    assert delivered is not None
    return LogObject(key=key, delivered_at=delivered)


def _ids(turns: list[Turn]) -> list[list[str]]:
    return [[i.request_id for i in t.invocations] for t in turns]


def test_tool_turn_split_across_runs_is_one_turn(tmp_path: Path) -> None:
    f1 = _write(
        tmp_path, 0, [_record("c1", 0, [user("Q")], [_TOOL_USE], stop="tool_use")]
    )
    f2 = _write(tmp_path, 2, [_record("c2", 2, [user("Q"), _ASKED, _TOOL_RESULT], "A")])

    run = _Run(tmp_path)
    run.read(f1)
    assert run.turns == []
    run.checkpoint(f1)

    turns = _Run(tmp_path)
    turns.read(f2)
    (turn,) = turns.finish()

    assert _ids([turn]) == [["c1", "c2"]]
    assert not turn.partial_start
    assert turn.conversation_id == "bedrock-c1"


def test_follow_up_in_the_next_run_joins_the_conversation(tmp_path: Path) -> None:
    f1 = _write(tmp_path, 0, [_record("a", 0, [user("Q1")], "A1")])
    f2 = _write(
        tmp_path, 5, [_record("b", 5, [user("Q1"), assistant("A1"), user("Q2")], "A2")]
    )

    first = _Run(tmp_path)
    first.read(f1)
    (turn_1,) = first.turns
    first.checkpoint(f1)

    second = _Run(tmp_path)
    second.read(f2)
    (turn_2,) = second.finish()

    assert turn_2.conversation_id == turn_1.conversation_id
    assert turn_2.conversation_source is ConversationSource.HASH_CHAIN


def test_buffered_call_is_read_again_in_the_next_run(tmp_path: Path) -> None:
    f1 = _write(tmp_path, 0, [_record("a", 0, [user("Q1")], "A1")])

    first = _Run(tmp_path, reorder_minutes=15)
    first.read(f1)
    assert first.turns == []
    first.checkpoint(f1)

    (turn,) = _Run(tmp_path, reorder_minutes=15).finish()
    assert _ids([turn]) == [["a"]]


def test_duplicate_in_the_next_run_is_dropped(tmp_path: Path) -> None:
    record = _record("a", 0, [user("Q1")], "A1")
    f1 = _write(tmp_path, 0, [record])
    f2 = _write(tmp_path, 3, [record])

    first = _Run(tmp_path)
    first.read(f1)
    first.checkpoint(f1)

    second = _Run(tmp_path)
    second.read(f2)
    assert second.finish() == []
    assert second.grouper.stats.duplicates == 1


def test_missing_file_is_logged_and_its_calls_dropped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    f1 = _write(
        tmp_path, 0, [_record("c1", 0, [user("Q")], [_TOOL_USE], stop="tool_use")]
    )
    run = _Run(tmp_path)
    run.read(f1)
    run.checkpoint(f1)
    (tmp_path / f1).unlink()

    with caplog.at_level(logging.WARNING):
        turns = _Run(tmp_path).finish()

    assert turns == []
    assert "c1 not found again" in caplog.text


def test_lost_first_call_makes_the_turn_partial(tmp_path: Path) -> None:
    c1 = _record("c1", 0, [user("Q")], [_TOOL_USE], stop="tool_use")
    asked_again = {
        "toolUse": {"toolUseId": "t2", "name": "search", "input": {"q": "y"}}
    }
    c2 = _record(
        "c2", 1, [user("Q"), _ASKED, _TOOL_RESULT], [asked_again], stop="tool_use"
    )
    f1 = _write(tmp_path, 0, [c1])
    f2 = _write(tmp_path, 1, [c2])
    run = _Run(tmp_path)
    run.read(f1, f2)
    run.checkpoint(f2)
    (tmp_path / f1).unlink()

    resumed = _Run(tmp_path)
    (turn,) = resumed.grouper.open_turns()
    assert _ids([turn]) == [["c2"]]
    assert turn.partial_start
    resumed.state.close()


def test_snapshot_refuses_turns_not_drained(tmp_path: Path) -> None:
    f1 = _write(tmp_path, 0, [_record("a", 0, [user("Q1")], "A1")])
    run = _Run(tmp_path)
    for record in run.source.read_object(_obj(f1)):
        parsed = parse_record(record.record, source_key=record.key)
        assert isinstance(parsed, Invocation)
        run.grouper.add(parsed)
    run.grouper.flush()

    with pytest.raises(RuntimeError):
        run.grouper.snapshot()
    run.state.close()


def test_cursor_round_trip_and_reset(tmp_path: Path) -> None:
    state = SyncState(tmp_path / DB_FILENAME, LOG_SOURCE, dry_run=False)
    assert state.get_cursor() is None
    cursor = Cursor(_START, _START + timedelta(hours=1), "some/key.json.gz")
    state.save_checkpoint(cursor, Grouper().snapshot())
    state.close()

    state = SyncState(tmp_path / DB_FILENAME, LOG_SOURCE, dry_run=False)
    assert state.get_cursor() == cursor
    assert (
        SyncState(tmp_path / DB_FILENAME, "s3://other", dry_run=False).get_cursor()
        is None
    )
    state.reset()
    assert state.get_cursor() is None
    state.close()


def test_emitted_registry_keeps_the_first_status(tmp_path: Path) -> None:
    state = SyncState(tmp_path / DB_FILENAME, LOG_SOURCE, dry_run=False)
    assert not state.is_emitted("req-1")
    state.record_emitted("conv", "req-1", "rejected")
    state.record_emitted("conv", "req-1", "sent")
    state.commit()
    state.close()

    state = SyncState(tmp_path / DB_FILENAME, LOG_SOURCE, dry_run=False)
    assert state.is_emitted("req-1")
    status = state._conn.execute("SELECT status FROM emitted_interaction").fetchone()[0]
    assert status == "rejected"
    state.reset()
    assert state.is_emitted("req-1"), "reset keeps the sent registry"
    state.close()


def test_a_turn_sent_before_a_crash_is_not_sent_again(tmp_path: Path) -> None:
    """A repeated run can see a late file the first run did not, and give an
    already sent turn another hash-chain conversation id: the registry still
    knows it by its first request id."""
    late_question = _record("a", 18, [user("Q1")], "A1")
    follow_up = _record("b", 20, [user("Q1"), assistant("A1"), user("Q2")], "A2")
    state = SyncState(tmp_path / DB_FILENAME, LOG_SOURCE, dry_run=False)

    def send(turns: list[Turn]) -> list[str]:
        sent = []
        for turn in turns:
            if not state.is_emitted(turn.first.request_id):
                key = turn.first.request_id
                state.record_emitted(turn.conversation_id, key, "sent")
                state.commit()
                sent.append(turn.first.request_id)
        return sent

    # First run: the question is not delivered yet. It stops before a checkpoint.
    first = Grouper()
    first.add(_parsed(follow_up))
    first.advance(_START.replace(minute=40))
    first_turns = first.drain_complete()
    assert [t.conversation_id for t in first_turns] == ["bedrock-b"]
    assert send(first_turns) == ["b"]

    # The repeated run sees the question, and the follow-up joins its chain.
    second = Grouper()
    second.add(_parsed(follow_up))
    second.advance(_START.replace(minute=21))
    second.add(_parsed(late_question))
    second.advance(_START.replace(minute=41))
    second_turns = second.drain_complete()
    assert [t.conversation_id for t in second_turns] == ["bedrock-a", "bedrock-a"]
    assert send(second_turns) == ["a"]
    state.close()


def _parsed(record: dict[str, Any]) -> Invocation:
    parsed = parse_record(record)
    assert isinstance(parsed, Invocation)
    return parsed


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    state = SyncState(tmp_path / "cache" / DB_FILENAME, LOG_SOURCE, dry_run=True)
    state.record_emitted("conv", "req-1", "sent")
    state.commit()
    state.close()

    assert not (tmp_path / "cache").exists()


def test_unreadable_file_stops_the_run_instead_of_dropping_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f1 = _write(
        tmp_path, 0, [_record("c1", 0, [user("Q")], [_TOOL_USE], stop="tool_use")]
    )
    run = _Run(tmp_path)
    run.read(f1)
    run.checkpoint(f1)

    def denied(self: LocalObjectStore, key: str) -> bytes:
        raise PermissionError(key)

    monkeypatch.setattr(LocalObjectStore, "get_bytes", denied)
    with pytest.raises(PermissionError):
        _Run(tmp_path)

    monkeypatch.undo()
    (turn,) = _Run(tmp_path).grouper.open_turns()
    assert _ids([turn]) == [["c1"]], "the state still points to the call"


def test_turns_waiting_for_the_same_tool_id_survive_a_checkpoint(
    tmp_path: Path,
) -> None:
    def call(query: str) -> dict[str, Any]:
        return {
            "toolUse": {
                "toolUseId": "functions.search:0",
                "name": "search",
                "input": {"q": query},
            }
        }

    def answered(query: str, result: str) -> list[dict[str, Any]]:
        return [
            user("Q"),
            {"role": "assistant", "content": [call(query)]},
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "functions.search:0",
                            "content": [{"text": result}],
                        }
                    }
                ],
            },
        ]

    f1 = _write(
        tmp_path,
        0,
        [
            _record("a1", 0, [user("Q")], [call("x")], stop="tool_use"),
            _record("b1", 0, [user("Q")], [call("y")], stop="tool_use"),
        ],
    )
    f2 = _write(
        tmp_path,
        2,
        [
            _record("b2", 2, answered("y", "RB"), "B"),
            _record("a2", 2, answered("x", "RA"), "A"),
        ],
    )
    run = _Run(tmp_path)
    run.read(f1)
    assert len(run.grouper.open_turns()) == 2
    run.checkpoint(f1)

    resumed = _Run(tmp_path)
    resumed.read(f2)
    turns = resumed.finish()
    assert sorted(_ids(turns)) == [["a1", "a2"], ["b1", "b2"]]


def test_stored_times_sort_in_time_order(tmp_path: Path) -> None:
    whole = datetime(2026, 11, 11, 10, tzinfo=UTC)
    state = SyncState(tmp_path / DB_FILENAME, LOG_SOURCE, dry_run=False)
    grouper_snapshot = Grouper().snapshot()
    grouper_snapshot.seen = {
        "late": whole + timedelta(microseconds=500),
        "early": whole,
    }
    state.save_checkpoint(Cursor(whole, whole, None), grouper_snapshot)
    rows = state._conn.execute(
        "SELECT request_id FROM seen_request ORDER BY call_time"
    ).fetchall()
    assert [r[0] for r in rows] == ["early", "late"]
    assert state.load_snapshot().seen == grouper_snapshot.seen
    state.close()
