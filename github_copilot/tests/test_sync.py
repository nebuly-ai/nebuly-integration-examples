from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import httpx
import pytest
from github_copilot_sync.__main__ import main
from github_copilot_sync.conversations import ConversationBuilder
from github_copilot_sync.models import UsageRecord
from github_copilot_sync.pairing import PairingEngine
from github_copilot_sync.sources import SourceStats
from github_copilot_sync.store import Store
from github_copilot_sync.sync import SyncSummary, _sync_source, run_sync
from github_copilot_sync.users import UserResolver

from tests.test_sources import _config

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from github_copilot_sync.config import Config


def _wrap(payload: object) -> str:
    return json.dumps(json.dumps(payload))


def _usage_pair(
    github_request_id: str,
    *,
    event_req: str,
    event_resp: str,
    user_text: str,
    assistant_text: str,
    timestamp_ms: int,
) -> list[dict[str, Any]]:
    body_req = _wrap(
        {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": user_text}],
        }
    )
    body_resp = _wrap(
        {
            "object": "chat.completion",
            "model": "gpt-4o",
            "choices": [
                {
                    "message": {"role": "assistant", "content": assistant_text},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2},
        }
    )
    return [
        {
            "type": "request",
            "user_id": 1,
            "enterprise_id": 9,
            "endpoint": "/enterprises/acme/copilot/chat/completions",
            "body": body_req,
            "@timestamp": timestamp_ms,
            "truncated": False,
            "event_id": event_req,
            "github_request_id": github_request_id,
        },
        {
            "type": "response",
            "user_id": 1,
            "enterprise_id": 9,
            "endpoint": "/enterprises/acme/copilot/chat/completions",
            "body": body_resp,
            "@timestamp": timestamp_ms + 100,
            "truncated": False,
            "event_id": event_resp,
            "github_request_id": github_request_id,
        },
    ]


_END = datetime(2026, 1, 3, tzinfo=UTC)


class RecordingNebuly:
    all_sent: ClassVar[list[dict[str, Any]]] = []

    def __init__(
        self, *_args: object, dry_run: bool = False, **_kwargs: object
    ) -> None:
        self.sent: list[dict[str, Any]] = []
        self._dry_run = dry_run

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if self._dry_run:
            return
        self.sent.append(payload)
        RecordingNebuly.all_sent.append(payload)


def _sync_config(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    to_date: datetime | None = None,
) -> Config:
    base = _config(
        sources=("rest",),
        from_date=datetime(2026, 1, 1, tzinfo=UTC),
        to_date=to_date,
        resolve_users=False,
        dry_run=dry_run,
    )
    return replace(base, cache_dir=tmp_path)


def _patch_sync(
    monkeypatch: pytest.MonkeyPatch,
    records: list[dict[str, Any]],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=records)

    _patch_sync_handler(monkeypatch, handler)


def _patch_sync_handler(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
) -> None:
    def github_client(config: Config) -> httpx.Client:
        return httpx.Client(
            base_url=config.github_api_url,
            transport=httpx.MockTransport(handler),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {config.github_token}",
                "X-GitHub-Api-Version": config.github_api_version,
            },
        )

    monkeypatch.setattr("github_copilot_sync.sync.NebulyClient", RecordingNebuly)
    monkeypatch.setattr("github_copilot_sync.sync._github_client", github_client)


@pytest.fixture(autouse=True)
def _reset_recording() -> Iterator[None]:
    RecordingNebuly.all_sent.clear()
    yield
    RecordingNebuly.all_sent.clear()


def test_sync_end_to_end_and_idempotent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ts = int(datetime(2026, 1, 2, 12, 0, tzinfo=UTC).timestamp() * 1000)
    records = _usage_pair(
        "pair-1",
        event_req="ev-req",
        event_resp="ev-resp",
        user_text="fix the bug",
        assistant_text="here is the fix",
        timestamp_ms=ts,
    )
    _patch_sync(monkeypatch, records)

    config = _sync_config(tmp_path, to_date=_END)
    summary1 = run_sync(config)
    assert summary1.sources["rest"].sent == 1
    assert len(RecordingNebuly.all_sent) == 1
    assert RecordingNebuly.all_sent[0]["interaction"]["input"] == "fix the bug"

    summary2 = run_sync(config)
    assert summary2.sources["rest"].sent == 0
    assert summary2.sources["rest"].records_deduped == 2


def test_sync_dry_run_does_not_post(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ts = int(datetime(2026, 1, 2, 12, 0, tzinfo=UTC).timestamp() * 1000)
    records = _usage_pair(
        "pair-2",
        event_req="ev-req-2",
        event_resp="ev-resp-2",
        user_text="hello",
        assistant_text="hi",
        timestamp_ms=ts,
    )
    _patch_sync(monkeypatch, records)

    config = _sync_config(tmp_path, dry_run=True, to_date=_END)
    summary = run_sync(config)
    assert summary.sources["rest"].sent == 1
    assert RecordingNebuly.all_sent == []
    assert not config.store_path.exists()

    real = _sync_config(tmp_path, to_date=_END)
    real_summary = run_sync(real)
    assert real_summary.sources["rest"].sent == 1
    assert len(RecordingNebuly.all_sent) == 1


def _completion(
    message: dict[str, Any], finish_reason: str, prompt_tokens: int, output: int
) -> str:
    return _wrap(
        {
            "object": "chat.completion",
            "model": "gpt-4o",
            "choices": [{"message": message, "finish_reason": finish_reason}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": output},
        }
    )


def _record(
    record_type: str, github_request_id: str, event_id: str, body: str, ts: int
) -> dict[str, Any]:
    return {
        "type": record_type,
        "user_id": 1,
        "enterprise_id": 9,
        "endpoint": "/enterprises/acme/copilot/chat/completions",
        "body": body,
        "@timestamp": ts,
        "truncated": False,
        "event_id": event_id,
        "github_request_id": github_request_id,
    }


_TOOL_CALL = {
    "role": "assistant",
    "content": None,
    "tool_calls": [
        {
            "id": "t1",
            "type": "function",
            "function": {"name": "grep", "arguments": "{}"},
        }
    ],
}


def _tool_loop_first_call(ts: int) -> list[dict[str, Any]]:
    request = _wrap(
        {"model": "gpt-4o", "messages": [{"role": "user", "content": "list files"}]}
    )
    response = _completion(_TOOL_CALL, "tool_calls", 3, 2)
    return [
        _record("request", "loop-1", "loop-1-req", request, ts),
        _record("response", "loop-1", "loop-1-resp", response, ts + 100),
    ]


def _tool_loop_second_call(ts: int) -> list[dict[str, Any]]:
    request = _wrap(
        {
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": "list files"},
                _TOOL_CALL,
                {"role": "tool", "tool_call_id": "t1", "content": "hits"},
            ],
        }
    )
    response = _completion(
        {"role": "assistant", "content": "final answer"}, "stop", 5, 4
    )
    return [
        _record("request", "loop-2", "loop-2-req", request, ts),
        _record("response", "loop-2", "loop-2-resp", response, ts + 100),
    ]


def _later_unrelated_pair(ts: int) -> list[dict[str, Any]]:
    return _usage_pair(
        "later",
        event_req="later-req",
        event_resp="later-resp",
        user_text="unrelated question",
        assistant_text="unrelated answer",
        timestamp_ms=ts,
    )


_TURN_START_MS = int(datetime(2026, 1, 2, 12, 0, tzinfo=UTC).timestamp() * 1000)


def _assert_full_tool_loop_turn(payload: dict[str, Any]) -> None:
    assert payload["interaction"]["input"] == "list files"
    assert payload["interaction"]["output"] == "final answer"
    assert payload["interaction"]["tags"]["tools used"] == "grep"
    assert payload["traces"][0]["input_tokens"] == 8
    assert payload["traces"][0]["output_tokens"] == 6


def test_turn_spanning_two_runs_is_emitted_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    records = _tool_loop_first_call(_TURN_START_MS)
    _patch_sync(monkeypatch, records)
    config = _sync_config(tmp_path)

    run_sync(config)
    assert RecordingNebuly.all_sent == []

    records.extend(_tool_loop_second_call(_TURN_START_MS + 1_000))
    records.extend(_later_unrelated_pair(_TURN_START_MS + 3_600_000))
    run_sync(config)

    assert len(RecordingNebuly.all_sent) == 1
    _assert_full_tool_loop_turn(RecordingNebuly.all_sent[0])


def test_failure_mid_source_then_rerun_emits_turn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    page_one = _tool_loop_first_call(_TURN_START_MS) + _tool_loop_second_call(
        _TURN_START_MS + 1_000
    )
    page_two = _later_unrelated_pair(_TURN_START_MS + 3_600_000)
    failing = {"on": True}
    next_link = (
        "<https://api.github.com/enterprises/acme/copilot/usage-records?page=2>;"
        ' rel="next"'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") == "2":
            if failing["on"]:
                return httpx.Response(400, request=request)
            return httpx.Response(200, json=page_two)
        return httpx.Response(200, json=page_one, headers={"Link": next_link})

    _patch_sync_handler(monkeypatch, handler)
    config = _sync_config(tmp_path)

    run_sync(config)
    assert RecordingNebuly.all_sent == []

    failing["on"] = False
    run_sync(config)

    assert len(RecordingNebuly.all_sent) == 1
    _assert_full_tool_loop_turn(RecordingNebuly.all_sent[0])


def test_sink_failure_is_logged_separately_and_turn_retried(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingNebuly:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def send_interaction(self, payload: dict[str, Any]) -> None:
            request = httpx.Request("POST", "https://nebuly.example/events")
            raise httpx.HTTPStatusError(
                "unavailable",
                request=request,
                response=httpx.Response(503, request=request),
            )

    records = _usage_pair(
        "pair-sink",
        event_req="ev-req-sink",
        event_resp="ev-resp-sink",
        user_text="hello sink",
        assistant_text="hi",
        timestamp_ms=_TURN_START_MS,
    )
    _patch_sync(monkeypatch, records)
    monkeypatch.setattr("github_copilot_sync.sync.NebulyClient", FailingNebuly)
    config = _sync_config(tmp_path, to_date=_END)

    run_sync(config)
    assert "Nebuly sink failure" in caplog.text
    assert "GitHub failure" not in caplog.text

    monkeypatch.setattr("github_copilot_sync.sync.NebulyClient", RecordingNebuly)
    summary = run_sync(config)

    assert summary.sources["rest"].sent == 1
    assert [p["interaction"]["input"] for p in RecordingNebuly.all_sent] == [
        "hello sink"
    ]


def test_same_first_prompt_after_gap_split_is_emitted_twice(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    nine_hours_ms = 9 * 3_600_000
    records = _usage_pair(
        "gap-1",
        event_req="gap-1-req",
        event_resp="gap-1-resp",
        user_text="same opener",
        assistant_text="first answer",
        timestamp_ms=_TURN_START_MS,
    ) + _usage_pair(
        "gap-2",
        event_req="gap-2-req",
        event_resp="gap-2-resp",
        user_text="same opener",
        assistant_text="second answer",
        timestamp_ms=_TURN_START_MS + nine_hours_ms,
    )
    _patch_sync(monkeypatch, records)

    run_sync(_sync_config(tmp_path, to_date=_END))

    sent = RecordingNebuly.all_sent
    assert [p["interaction"]["output"] for p in sent] == [
        "first answer",
        "second answer",
    ]
    assert (
        sent[0]["interaction"]["conversation_id"]
        != sent[1]["interaction"]["conversation_id"]
    )


def test_failed_run_is_reported_in_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class FailingNebuly:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def send_interaction(self, payload: dict[str, Any]) -> None:
            raise httpx.ConnectError("down")

    records = _usage_pair(
        "pair-fail",
        event_req="ev-req-fail",
        event_resp="ev-resp-fail",
        user_text="hello",
        assistant_text="hi",
        timestamp_ms=_TURN_START_MS,
    )
    _patch_sync(monkeypatch, records)
    config = _sync_config(tmp_path, to_date=_END)

    assert run_sync(config).failed is False

    monkeypatch.setattr("github_copilot_sync.sync.NebulyClient", FailingNebuly)
    retry_config = _sync_config(tmp_path / "retry", to_date=_END)
    assert run_sync(retry_config).failed is True


def test_main_exits_non_zero_when_sync_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "github_copilot_sync.__main__.Config.from_env_and_args", lambda: None
    )
    monkeypatch.setattr(
        "github_copilot_sync.__main__.run_sync",
        lambda _config: SyncSummary(failed=True),
    )
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1

    monkeypatch.setattr(
        "github_copilot_sync.__main__.run_sync",
        lambda _config: SyncSummary(),
    )
    main()


def test_csv_mode_creates_no_nebuly_http_client(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ts = int(datetime(2026, 1, 2, 12, 0, tzinfo=UTC).timestamp() * 1000)
    records = _usage_pair(
        "pair-csv",
        event_req="ev-req-csv",
        event_resp="ev-resp-csv",
        user_text="hello",
        assistant_text="hi",
        timestamp_ms=ts,
    )
    _patch_sync(monkeypatch, records)
    created: list[dict[str, Any]] = []
    real_client: Callable[..., httpx.Client] = httpx.Client

    def spy(*args: object, **kwargs: object) -> httpx.Client:
        created.append(kwargs)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", spy)

    config = replace(
        _sync_config(tmp_path, to_date=_END),
        csv_export=True,
        csv_export_path=tmp_path / "out.csv",
    )
    summary = run_sync(config)

    assert summary.sources["rest"].sent == 1
    assert created
    assert all("base_url" in kwargs for kwargs in created)


class _FakeSource:
    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records
        self.stats = SourceStats()

    def iter_records(self, cursor: str | None) -> Iterator[tuple[UsageRecord, str]]:
        for raw in self._records:
            record = UsageRecord.from_raw(raw)
            assert record is not None
            yield record, str(record.timestamp_ms)


def test_unpaired_counts_are_per_source(tmp_path: Path) -> None:
    hour = 3_600_000
    ts = int(datetime(2026, 1, 2, 12, 0, tzinfo=UTC).timestamp() * 1000)

    def source_records(offset: int, name: str) -> list[dict[str, Any]]:
        orphan = _record(
            "request", f"orphan-{name}", f"orphan-{name}-req", "{}", ts + offset
        )
        pair = _usage_pair(
            f"pair-{name}",
            event_req=f"{name}-req",
            event_resp=f"{name}-resp",
            user_text=f"hello {name}",
            assistant_text="hi",
            timestamp_ms=ts + offset + hour,
        )
        return [orphan, *pair]

    config = _sync_config(tmp_path, dry_run=True)
    store = Store(config.store_path, dry_run=True)
    pairing = PairingEngine(config.pair_wait_minutes, store)
    conversations = ConversationBuilder(
        store,
        conversation_gap_hours=config.conversation_gap_hours,
        interaction_idle_minutes=config.interaction_idle_minutes,
    )
    sink = RecordingNebuly(dry_run=True)
    resolver = UserResolver(config, httpx.Client(), store)

    counts = [
        _sync_source(
            name,
            _FakeSource(source_records(offset, name)),
            store=store,
            pairing=pairing,
            conversations=conversations,
            sink=sink,
            resolver=resolver,
            config=config,
        )
        for name, offset in (("a", 0), ("b", 4 * hour))
    ]
    store.close()

    assert [c.unpaired_requests for c in counts] == [1, 1]
    assert pairing.counters.unpaired_requests == 2
