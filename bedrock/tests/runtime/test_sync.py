from __future__ import annotations

import gzip
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any

import httpx
import pytest

from bedrock_sync.runtime.config import Config
from bedrock_sync.runtime.s3_client import LocalObjectStore
from bedrock_sync.runtime.state import Cursor, RuntimeState
from bedrock_sync.runtime.sync import (
    FirstRunRequiresFromDateError,
    GapAbortedError,
    SyncSummary,
    sync,
)
from bedrock_sync.shared.delivery import AuthenticationError, SafeNebulyClient
from tests.runtime.records import ACCOUNT, converse_record, user

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

REGION = "eu-north-1"
ROOT = f"AWSLogs/{ACCOUNT}/BedrockModelInvocationLogs/{REGION}"
START = datetime(2026, 11, 11, 10, tzinfo=UTC)


class FakeNebuly:
    """Mock transport: answers each POST with the next status (default 200)."""

    def __init__(self, statuses: list[int | Exception] | None = None) -> None:
        self.statuses = list(statuses or [])
        self.payloads: list[dict[str, Any]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        status = self.statuses.pop(0) if self.statuses else 200
        if isinstance(status, Exception):
            raise status
        if status < 300:
            self.payloads.append(json.loads(request.content))
        return httpx.Response(status)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    @property
    def inputs(self) -> list[str]:
        return [p["interaction"]["input"] for p in self.payloads]


def _config(tmp_path: Path, **changes: Any) -> Config:  # noqa: ANN401
    config = Config(
        account_id=ACCOUNT,
        region=REGION,
        aws_role_arn=None,
        aws_external_id=None,
        nebuly_api_key="nb-key",
        nebuly_endpoint="https://backend.example.test/trace_interaction",
        user_hash_secret="",
        send_plain_end_user=True,
        anonymize=False,
        from_date=START,
        to_date=None,
        state_db=tmp_path / "state.db",
        content_retention_seconds=604800,
        dry_run=False,
        verbose=False,
        retry_http_429=True,
        s3_bucket="bucket",
        s3_prefix="",
    )
    return replace(config, **changes)


def _call(
    root: Path,
    request_id: str,
    minute: float,
    interaction: str | None,
    question: str,
    *,
    conversation: str = "c-1",
    user_id: str = "u-1",
    delivered_after: float = 1,
) -> None:
    """One call, delivered to S3 ``delivered_after`` minutes after it started."""
    at = START + timedelta(minutes=minute)
    metadata = (
        None
        if interaction is None
        else {
            "interaction_id": interaction,
            "conversation_id": conversation,
            "user_id": user_id,
        }
    )
    record = converse_record(
        [user(question)],
        f"answer to {question}",
        request_id=request_id,
        timestamp=at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        metadata=metadata,
    )
    delivered = at + timedelta(minutes=delivered_after)
    key = (
        f"{ROOT}/{delivered:%Y/%m/%d/%H}/"
        f"{delivered:%Y%m%dT%H%M%S}000Z_{request_id}.json.gz"
    )
    path = root / "logs" / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json.dumps(record).encode()))


def _run(
    tmp_path: Path,
    nebuly: FakeNebuly,
    now: datetime,
    **changes: Any,  # noqa: ANN401
) -> SyncSummary:
    return sync(
        _config(tmp_path, **changes),
        LocalObjectStore(tmp_path / "logs"),
        transport=nebuly.transport,
        now=now,
    )


def test_first_run_requires_from_date(tmp_path: Path) -> None:
    with pytest.raises(FirstRunRequiresFromDateError):
        _run(tmp_path, FakeNebuly(), START + timedelta(hours=1), from_date=None)


def test_next_interaction_closes_the_previous_and_each_is_sent_once(
    tmp_path: Path,
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    _call(tmp_path, "b1", 2, "i-b", "second")
    nebuly = FakeNebuly()

    # until = 10:12: the margin after i-b (10:02 + 5 min) has passed, its idle not
    summary = _run(tmp_path, nebuly, START + timedelta(minutes=17))
    assert nebuly.inputs == ["first"]
    assert summary.grouping.closed_next_interaction == 1
    assert summary.totals.open_interactions == 1

    # i-b stays open across runs, then closes for inactivity
    again = FakeNebuly()
    _run(tmp_path, again, START + timedelta(hours=2), from_date=None)
    assert again.inputs == ["second"]

    third = FakeNebuly()
    _run(tmp_path, third, START + timedelta(hours=3), from_date=None)
    assert third.payloads == []


def test_calls_of_an_open_interaction_join_across_runs(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "question")
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(minutes=10))
    assert nebuly.payloads == []

    _call(tmp_path, "a2", 6, "i-a", "follow-up step")
    _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)

    [payload] = nebuly.payloads
    assert payload["interaction"]["input"] == "question"
    assert payload["interaction"]["output"] == "answer to follow-up step"


def test_missing_triplet_and_conflicts_are_counted(tmp_path: Path) -> None:
    _call(tmp_path, "x1", 0, None, "no metadata")
    _call(tmp_path, "a1", 1, "i-a", "mine")
    _call(tmp_path, "a2", 2, "i-a", "reused", conversation="c-2")
    nebuly = FakeNebuly()

    summary = _run(tmp_path, nebuly, START + timedelta(hours=2))

    assert nebuly.inputs == ["mine"]
    assert summary.grouping.missing_triplet == 1
    assert summary.grouping.interaction_id_conflicts == 1


def test_rejected_is_final_and_auth_failure_stops(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    _call(tmp_path, "b1", 2, "i-b", "second", conversation="c-2")

    summary = _run(tmp_path, FakeNebuly([422, 200]), START + timedelta(hours=2))
    assert (summary.delivery.rejected, summary.delivery.sent) == (1, 1)

    _call(tmp_path, "c1", 125, "i-c", "third", conversation="c-3")
    with pytest.raises(AuthenticationError):
        _run(tmp_path, FakeNebuly([401]), START + timedelta(hours=5), from_date=None)


def test_connection_failure_is_retried_on_a_later_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "bedrock_sync.runtime.sync.SafeNebulyClient",
        partial(SafeNebulyClient, sleep=lambda _: None),
    )
    _call(tmp_path, "a1", 0, "i-a", "first")
    error = httpx.ConnectError("down")

    summary = _run(
        tmp_path, FakeNebuly([error, error, error]), START + timedelta(hours=2)
    )
    assert summary.delivery.retryable == 1
    assert summary.outbox == {"pending": 1}

    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=3), from_date=None)
    assert nebuly.inputs == ["first"]


def test_server_error_is_uncertain_and_never_replayed(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")

    summary = _run(tmp_path, FakeNebuly([500]), START + timedelta(hours=2))
    assert summary.delivery.uncertain == 1

    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=3), from_date=None)
    assert nebuly.payloads == []


def test_dry_run_sends_nothing_and_saves_nothing(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    nebuly = FakeNebuly()

    summary = _run(tmp_path, nebuly, START + timedelta(hours=2), dry_run=True)

    assert summary.totals.interactions_queued == 1
    assert nebuly.payloads == []
    assert not (tmp_path / "state.db").exists()


def test_backfill_does_not_queue_closed_interactions_again(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=2))
    assert nebuly.inputs == ["first"]

    later = FakeNebuly()
    summary = _run(
        tmp_path,
        later,
        START + timedelta(days=60),
        from_date=START - timedelta(days=1),
    )
    assert later.payloads == []
    assert summary.grouping.calls_for_closed_interaction == 1


def test_gap_needs_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    _run(tmp_path, FakeNebuly(), START + timedelta(hours=2))
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    with pytest.raises(GapAbortedError):
        _run(
            tmp_path,
            FakeNebuly(),
            START + timedelta(days=3),
            from_date=START + timedelta(days=2),
        )


def test_confirmed_gap_restarts_from_the_new_date(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    _call(tmp_path, "b1", 100, "i-b", "open", conversation="c-2")
    first = FakeNebuly()
    _run(tmp_path, first, START + timedelta(hours=2))
    assert first.inputs == ["first"]
    with _saved_state(tmp_path) as state:
        assert [call.request_id for call in state.pending_calls()] == ["b1"]

    _call(tmp_path, "c1", 60 * 48 + 10, "i-c", "after the gap", conversation="c-3")
    second = FakeNebuly()
    _run(
        tmp_path,
        second,
        START + timedelta(days=3),
        from_date=START + timedelta(days=2),
        force=True,
    )
    # The open pointers of the old range are dropped, nothing is sent twice.
    assert second.inputs == ["after the gap"]
    with _saved_state(tmp_path) as state:
        cursor = state.cursor()
        assert cursor is not None
        assert cursor.coverage_from == START + timedelta(days=2)
        assert not state.pending_calls()


def _failing_on(
    monkeypatch: pytest.MonkeyPatch, suffix: str
) -> Callable[[LocalObjectStore, str], bytes]:
    """Make reading the file ending with ``suffix`` fail; return the original."""
    original: Callable[[LocalObjectStore, str], bytes] = LocalObjectStore.get_bytes

    def flaky(self: LocalObjectStore, key: str) -> bytes:
        if key.endswith(suffix):
            raise OSError("S3 down")
        return original(self, key)

    monkeypatch.setattr(LocalObjectStore, "get_bytes", flaky)
    return original


def _saved_state(tmp_path: Path) -> RuntimeState:
    return RuntimeState(tmp_path / "state.db", "s3://bucket/" + ROOT)


def test_failed_read_before_any_hour_ends_keeps_the_initial_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    _call(tmp_path, "b1", 70, "i-b", "second", conversation="c-2")
    original = _failing_on(monkeypatch, "b1.json.gz")
    with pytest.raises(OSError, match="S3 down"):
        _run(tmp_path, FakeNebuly(), START + timedelta(hours=4))

    # Files are read ahead in parallel: the failure on the first file of the
    # second hour stops the run before that hour's checkpoint.
    with _saved_state(tmp_path) as state:
        assert state.cursor() == Cursor(START, START, None)
        assert state.closed_owner("i-a") is None

    monkeypatch.setattr(LocalObjectStore, "get_bytes", original)
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=4), from_date=None)
    assert nebuly.inputs == ["first", "second"]


def test_failed_read_resumes_from_the_last_hourly_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    _call(tmp_path, "b1", 70, "i-b", "second", conversation="c-2")
    _call(tmp_path, "b2", 75, "i-b2", "third", conversation="c-3")
    original = _failing_on(monkeypatch, "b2.json.gz")
    with pytest.raises(OSError, match="S3 down"):
        _run(tmp_path, FakeNebuly(), START + timedelta(hours=4))

    # The hour of a1 was checkpointed when b1 (next hour) was read; i-a was
    # still open then, so it is saved as a pointer, not yet closed.
    with _saved_state(tmp_path) as state:
        cursor = state.cursor()
        assert cursor is not None
        assert cursor.last_key is not None
        assert cursor.last_key.endswith("a1.json.gz")
        assert [call.request_id for call in state.pending_calls()] == ["a1"]

    monkeypatch.setattr(LocalObjectStore, "get_bytes", original)
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=4), from_date=None)
    assert nebuly.inputs == ["first", "second", "third"]


def test_conversation_of_another_user_is_dropped_across_runs(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "alice first")
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=2))
    assert nebuly.inputs == ["alice first"]

    _call(tmp_path, "b1", 130, "i-b", "bob in alice's chat", user_id="u-2")
    _call(tmp_path, "a2", 131, "i-c", "alice again")
    later = FakeNebuly()
    summary = _run(tmp_path, later, START + timedelta(hours=5), from_date=None)

    assert later.inputs == ["alice again"]
    assert summary.grouping.conversation_user_conflicts == 1


def test_older_call_read_in_a_later_run_takes_the_conversation_over(
    tmp_path: Path,
) -> None:
    """Bob's newer call was read first and left open; alice's older call of
    the same conversation arrives in the next run and takes it over."""
    _call(tmp_path, "b1", 5, "i-bob", "bob's question", user_id="u-bob")
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(minutes=12))
    assert nebuly.payloads == []

    _call(
        tmp_path,
        "a1",
        4,
        "i-alice",
        "alice's question",
        user_id="u-alice",
        delivered_after=6,
    )
    summary = _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)

    assert nebuly.inputs == ["alice's question"]
    assert summary.grouping.conversation_user_conflicts == 1
    assert summary.grouping.accepted == 1  # bob's call was counted in run 1


def test_summary_is_incomplete_when_nebuly_did_not_confirm(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    assert _run(tmp_path, FakeNebuly([500]), START + timedelta(hours=2)).incomplete
    assert not _run(
        tmp_path, FakeNebuly(), START + timedelta(hours=3), from_date=None
    ).incomplete


def test_summary_serializes_to_json_with_reasons_as_keys(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    path = next((tmp_path / "logs").rglob("*a1.json.gz"))
    record = json.loads(gzip.decompress(path.read_bytes()))
    path.write_bytes(gzip.compress(json.dumps({**record, "errorCode": "X"}).encode()))

    summary = _run(tmp_path, FakeNebuly(), START + timedelta(hours=2))

    as_json = json.loads(json.dumps(asdict(summary)))
    assert as_json["totals"]["records_skipped"] == {"error_record": 1}
