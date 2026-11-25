from __future__ import annotations

import gzip
import json
import logging
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from bedrock_sync.runtime import user_defined
from bedrock_sync.runtime.config import Config
from bedrock_sync.runtime.grouping import Triplet
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

    from bedrock_sync.runtime.grouping import Interaction

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
        pseudonymize_end_user=False,
        anonymize=False,
        from_date=START,
        to_date=None,
        state_db=tmp_path / "state.db",
        content_retention_seconds=604800,
        dry_run=False,
        verbose=False,
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


def test_late_listing_finds_unseen_files_before_the_cursor_without_downloading_seen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _call(tmp_path, "seen", 46, "seen", "seen")
    _run(tmp_path, FakeNebuly(), START + timedelta(minutes=55))
    _call(tmp_path, "late", 45, "late", "late", conversation="c-2")
    _call(tmp_path, "laterkey", 48, "later", "later", conversation="c-3")
    downloads: list[str] = []
    original = LocalObjectStore.get_bytes

    def tracked(self: LocalObjectStore, key: str) -> bytes:
        downloads.append(key.rsplit("_", 1)[-1])
        return original(self, key)

    monkeypatch.setattr(LocalObjectStore, "get_bytes", tracked)
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)
    # Restoring the still-open "seen" request reads its body once. The overlap
    # does not download that file a second time.
    assert downloads.count("seen.json.gz") == 1
    assert sorted(downloads) == ["late.json.gz", "laterkey.json.gz", "seen.json.gz"]
    assert sorted(nebuly.inputs) == ["late", "later", "seen"]
    with _saved_state(tmp_path) as state:
        assert not state.read_object_keys(START, START + timedelta(hours=2))


def test_out_of_order_calls_join_across_a_checkpoint(tmp_path: Path) -> None:
    _call(tmp_path, "r1", 0, "i-a", "question")
    _call(tmp_path, "r3", 31, "i-a", "final")
    nebuly = FakeNebuly()
    first = _run(tmp_path, nebuly, START + timedelta(minutes=38))
    assert nebuly.inputs == []
    assert first.grouping.calls_for_closed_interaction == 0

    _call(tmp_path, "r2", 29, "i-a", "middle", delivered_after=4)
    second = _run(tmp_path, nebuly, START + timedelta(hours=2), from_date=None)
    [payload] = nebuly.payloads
    assert payload["interaction"]["output"] == "answer to final"
    assert sum(t["input_tokens"] for t in payload["traces"]) == 30
    assert second.grouping.calls_for_closed_interaction == 0


def test_missing_triplet_is_counted_and_turn_ids_are_per_conversation(
    tmp_path: Path,
) -> None:
    _call(tmp_path, "x1", 0, None, "no metadata")
    _call(tmp_path, "a1", 1, "i-a", "mine")
    _call(tmp_path, "a2", 2, "i-a", "same id, other chat", conversation="c-2")
    nebuly = FakeNebuly()

    summary = _run(tmp_path, nebuly, START + timedelta(hours=2))

    assert sorted(nebuly.inputs) == ["mine", "same id, other chat"]
    assert summary.grouping.missing_triplet == 1


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


def test_server_error_is_sent_again_by_the_next_run(tmp_path: Path) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")

    summary = _run(tmp_path, FakeNebuly([500]), START + timedelta(hours=2))
    assert summary.delivery.retryable == 1
    cooldown = _run(
        tmp_path, FakeNebuly(), START + timedelta(hours=2, seconds=30), from_date=None
    )
    assert cooldown.delivery.retryable == 0
    assert cooldown.waiting_for_retry
    assert cooldown.incomplete

    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=3), from_date=None)
    assert nebuly.inputs == ["first"]


def test_retry_given_up_only_sends_requested_item_without_reading_s3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "given up")
    first = START + timedelta(hours=2)
    _run(tmp_path, FakeNebuly([503]), first)
    _call(tmp_path, "a2", 7 * 24 * 60, "i-b", "other")
    give_up = first + timedelta(days=7)
    after = _run(tmp_path, FakeNebuly([503, 503]), give_up, from_date=None)
    assert after.outbox == {"pending": 1, "rejected": 1}

    def no_s3(_: Config) -> LocalObjectStore:
        msg = "manual retry must not create an S3 client"
        raise AssertionError(msg)

    monkeypatch.setattr("bedrock_sync.runtime.sync.build_s3_store", no_s3)
    nebuly = FakeNebuly()
    result = sync(
        _config(
            tmp_path,
            from_date=None,
            retry_given_up=Triplet("i-a", "c-1", "u-1").turn_key,
        ),
        transport=nebuly.transport,
        now=give_up + timedelta(minutes=2),
    )
    assert nebuly.inputs == ["given up"]
    assert result.outbox == {"pending": 1, "sent": 1}
    assert not result.incomplete


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


def test_unreadable_log_file_is_counted_and_makes_the_run_incomplete(
    tmp_path: Path,
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    broken = START + timedelta(minutes=5)
    path = (
        tmp_path
        / "logs"
        / f"{ROOT}/{broken:%Y/%m/%d/%H}/{broken:%Y%m%dT%H%M%S}000Z_broken.json.gz"
    )
    path.write_bytes(gzip.compress(b"{}")[:-6])
    nebuly = FakeNebuly()

    summary = _run(tmp_path, nebuly, START + timedelta(hours=2))

    assert nebuly.inputs == ["first"]
    assert (summary.totals.unreadable_files, summary.totals.unreadable_lines) == (1, 0)
    assert summary.incomplete


def test_history_retention_forgets_old_interactions_and_a_backfill_resends(
    tmp_path: Path,
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "first")
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=2))
    assert nebuly.inputs == ["first"]
    with _saved_state(tmp_path) as state:
        assert state.delivery_counts() == {"sent": 1}
    _run(tmp_path, FakeNebuly(), START + timedelta(days=31))
    with _saved_state(tmp_path) as state:
        assert state.delivery_counts() == {}
        assert (
            state._db.execute("SELECT COUNT(*) FROM closed_interaction").fetchone()[0]
            == 0
        )

    later = FakeNebuly()
    _run(
        tmp_path,
        later,
        START + timedelta(days=32),
        from_date=START - timedelta(days=1),
        force=True,
    )
    # Same input, user and time_start: Nebuly updates the same interaction.
    assert later.payloads == nebuly.payloads


def test_history_retention_keeps_a_turn_closed_while_its_delivery_is_kept(
    tmp_path: Path,
) -> None:
    """Closed at hour 2, delivered a day later after a 503: the closed turn
    stays as long as its outbox row, so a backfill in between skips it."""
    _call(tmp_path, "a1", 0, "i-a", "first")
    _run(tmp_path, FakeNebuly([503]), START + timedelta(hours=2))
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(days=1))
    assert nebuly.inputs == ["first"]

    # 30 days after the close, not yet 30 days after the delivery.
    _run(tmp_path, FakeNebuly(), START + timedelta(days=30, hours=12))
    with _saved_state(tmp_path) as state:
        assert state.delivery_counts() == {"sent": 1}
        assert (
            state._db.execute("SELECT COUNT(*) FROM closed_interaction").fetchone()[0]
            == 1
        )
    between = FakeNebuly()
    _run(
        tmp_path,
        between,
        START + timedelta(days=30, hours=13),
        from_date=START - timedelta(days=1),
        force=True,
    )
    assert between.inputs == []

    # Both forgotten: a backfill sends it again once, and Nebuly updates it.
    _run(tmp_path, FakeNebuly(), START + timedelta(days=32))
    with _saved_state(tmp_path) as state:
        assert state.delivery_counts() == {}
    later = FakeNebuly()
    _run(
        tmp_path,
        later,
        START + timedelta(days=33),
        from_date=START - timedelta(days=2),
        force=True,
    )
    assert later.payloads == nebuly.payloads


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
        assert not state.is_closed(Triplet("i-a", "c-1", "u-1").turn_key)

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


def test_another_user_in_the_conversation_is_sent_as_their_own_across_runs(
    tmp_path: Path,
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "alice first")
    nebuly = FakeNebuly()
    _run(tmp_path, nebuly, START + timedelta(hours=2))
    assert nebuly.inputs == ["alice first"]

    _call(tmp_path, "b1", 130, "i-b", "bob in alice's chat", user_id="u-2")
    _call(tmp_path, "a2", 131, "i-c", "alice again")
    later = FakeNebuly()
    summary = _run(tmp_path, later, START + timedelta(hours=5), from_date=None)

    assert sorted(later.inputs) == ["alice again", "bob in alice's chat"]
    by_input = {p["interaction"]["input"]: p["interaction"] for p in later.payloads}
    assert by_input["bob in alice's chat"]["end_user"] == "u-2"
    assert by_input["bob in alice's chat"]["conversation_id"] == "c-1"
    assert summary.grouping.conversations_with_several_users == 1
    assert not summary.incomplete


def test_conversion_error_is_logged_and_sent_by_a_backfill_once_fixed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "secret question")

    def broken(*_: object, **__: object) -> dict[str, str]:
        raise KeyError("secret question")

    monkeypatch.setattr(user_defined, "build_tags", broken)
    nebuly = FakeNebuly()
    with caplog.at_level(logging.DEBUG, logger="bedrock_sync.runtime.sync"):
        failed = _run(tmp_path, nebuly, START + timedelta(hours=2))
    assert failed.totals.conversion_errors == 1
    assert failed.incomplete
    assert nebuly.payloads == []
    [line] = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert "a1" in line
    assert "KeyError" in line
    assert "--from-date" in line
    assert "secret question" not in caplog.text
    assert "in broken" in caplog.text

    # A late call of the failed interaction is dropped like any closed one.
    _call(tmp_path, "a2", 1, "i-a", "late", delivered_after=150)
    late = _run(tmp_path, nebuly, START + timedelta(hours=4), from_date=None)
    assert late.grouping.calls_for_closed_interaction == 1
    assert nebuly.payloads == []

    monkeypatch.undo()
    fixed = _run(
        tmp_path, nebuly, START + timedelta(hours=5), from_date=START - timedelta(1)
    )
    assert nebuly.inputs == ["secret question"]
    assert not fixed.incomplete


def test_non_json_hook_result_only_blocks_its_own_interaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _call(tmp_path, "a1", 0, "i-a", "broken")
    _call(tmp_path, "a2", 1, "i-b", "healthy")

    def with_bad_value(
        interaction: Interaction,
        traces: list[dict[str, Any]],
        _context: object,
    ) -> list[dict[str, Any]]:
        if interaction.first.request_id == "a1":
            traces[0]["bad"] = object()
        return traces

    monkeypatch.setattr(user_defined, "customize_traces", with_bad_value)
    nebuly = FakeNebuly()
    summary = _run(tmp_path, nebuly, START + timedelta(hours=2))
    assert nebuly.inputs == ["healthy"]
    assert summary.incomplete
    assert summary.totals.conversion_errors == 1


def test_backfill_inside_the_synced_range_rebuilds_a_conversion_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The documented recovery: fix the hook, then backfill from just before
    the interaction, well after the first run's --from-date. Interactions
    already sent are skipped, and an open one keeps its earlier calls."""
    _call(tmp_path, "a1", 0, "i-a", "sent before")
    _call(tmp_path, "b1", 60, "i-b", "broken", conversation="c-2")
    nebuly = FakeNebuly()
    first = START - timedelta(days=2)
    _run(tmp_path, nebuly, START + timedelta(minutes=30), from_date=first)

    real = user_defined.build_tags

    def broken(interaction: Any, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        if interaction.first.request_id == "b1":
            raise KeyError("broken")
        return real(interaction, *args, **kwargs)

    monkeypatch.setattr(user_defined, "build_tags", broken)
    # c-3 opens before the backfill date and stays open across it.
    _call(tmp_path, "c1", 100, "i-c", "open", conversation="c-3")
    failed = _run(tmp_path, nebuly, START + timedelta(minutes=110), from_date=None)
    assert failed.totals.conversion_errors == 1
    assert failed.totals.open_interactions == 1
    assert nebuly.inputs == ["sent before"]

    monkeypatch.undo()
    _call(tmp_path, "c2", 115, "i-c", "open", conversation="c-3")
    with caplog.at_level(logging.WARNING, logger="bedrock_sync.runtime.sync"):
        fixed = _run(
            tmp_path,
            nebuly,
            START + timedelta(minutes=120),
            from_date=START + timedelta(minutes=50),
        )
    assert "inside the synced range" in caplog.text
    assert nebuly.inputs == ["sent before", "broken"]
    assert not fixed.incomplete
    with _saved_state(tmp_path) as state:
        cursor = state.cursor()
        assert cursor is not None
        assert cursor.coverage_from == first

    # The open interaction closes later with both calls.
    _run(tmp_path, nebuly, START + timedelta(hours=4), from_date=None)
    assert nebuly.inputs == ["sent before", "broken", "open"]
    # One trace per model: the tokens of both calls (10 each).
    assert sum(t["input_tokens"] for t in nebuly.payloads[-1]["traces"]) == 20


@pytest.mark.parametrize("bad_file", [None, b"{}", b'{"requestId":"a1"}'])
def test_turn_with_a_saved_call_gone_from_s3_goes_on_without_it(
    tmp_path: Path, bad_file: bytes | None
) -> None:
    """A deleted or unreadable S3 record does not come back: the turn is not
    held, it closes as usual with the calls that are left, reported once."""
    api = FakeNebuly()
    _call(tmp_path, "a1", 0, "i-a", "original input")
    _call(tmp_path, "a2", 1, "i-a", "intermediate step")
    first = _run(tmp_path, api, START + timedelta(minutes=10))
    assert first.totals.open_interactions == 1
    path = next((tmp_path / "logs").rglob("*_a1.json.gz"))
    if bad_file is None:
        path.unlink()
    else:
        path.write_bytes(bad_file)
    _call(tmp_path, "a3", 12, "i-a", "last step")
    _call(tmp_path, "b1", 13, "i-b", "other turn", conversation="c-2")

    restored = _run(tmp_path, api, START + timedelta(minutes=20), from_date=None)
    assert restored.incomplete
    assert restored.totals.restore_missing_calls == 1
    assert restored.totals.open_interactions == 2
    with _saved_state(tmp_path) as state:
        assert {ref.request_id for ref in state.pending_calls()} == {
            "a2",
            "a3",
            "b1",
        }

    closed = _run(tmp_path, api, START + timedelta(hours=2), from_date=None)
    assert not closed.incomplete
    assert closed.totals.restore_missing_calls == 0
    assert closed.totals.open_interactions == 0
    assert sorted(api.inputs) == ["intermediate step", "other turn"]
    sent = next(p for p in api.payloads if p["interaction"]["input"] != "other turn")
    assert sent["interaction"]["output"] == "answer to last step"
    assert sum(t["input_tokens"] for t in sent["traces"]) == 20


def test_backfill_requires_the_original_first_call_to_reopen_failed_conversion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = FakeNebuly()
    _call(tmp_path, "a1", 0, "i-a", "original input")
    with monkeypatch.context() as patch:

        def broken_hook() -> None:
            raise ValueError("broken")

        patch.setattr(user_defined, "build_tags", lambda *_: broken_hook())
        failed = _run(tmp_path, api, START + timedelta(hours=2))
    assert failed.totals.conversion_errors == 1

    _call(tmp_path, "a2", 5, "i-a", "continuation", delivered_after=180)
    partial = _run(
        tmp_path, api, START + timedelta(hours=5), from_date=START + timedelta(hours=1)
    )
    assert partial.grouping.calls_for_closed_interaction == 1
    assert api.inputs == []

    _run(tmp_path, api, START + timedelta(hours=6), from_date=START)
    assert api.inputs == ["original input"]


def test_backfill_reopens_failed_conversion_with_earlier_delivered_continuation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = FakeNebuly()
    _call(tmp_path, "a1", 0, "i-a", "original input", delivered_after=15)
    _call(tmp_path, "a2", 5, "i-a", "continuation")
    with monkeypatch.context() as patch:

        def broken_hook() -> None:
            raise ValueError("broken")

        patch.setattr(user_defined, "build_tags", lambda *_: broken_hook())
        failed = _run(tmp_path, api, START + timedelta(hours=2))
    assert failed.totals.conversion_errors == 1
    assert api.inputs == []

    path = next((tmp_path / "logs").rglob("*_a1.json.gz"))
    original = path.read_bytes()
    path.write_bytes(b"{}")
    partial = _run(tmp_path, api, START + timedelta(hours=3), from_date=START)
    assert partial.grouping.calls_for_closed_interaction == 1
    assert api.inputs == []

    path.write_bytes(original)
    recovered = _run(tmp_path, api, START + timedelta(hours=4), from_date=START)
    assert not recovered.incomplete
    assert api.inputs == ["original input"]
    assert api.payloads[-1]["interaction"]["output"] == "answer to continuation"
    assert sum(t["input_tokens"] for t in api.payloads[-1]["traces"]) == 20


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
