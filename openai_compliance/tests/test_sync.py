from __future__ import annotations

import json
import types
from datetime import UTC, datetime, timedelta, tzinfo
from typing import TYPE_CHECKING, Any, ClassVar
from unittest.mock import MagicMock

import httpx

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
from openai_compliance_sync.compliance_client import (
    EVENT_CODEX_LOG,
    EVENT_CODEX_TURN,
    EVENT_CONVERSATION_MESSAGE,
)
from openai_compliance_sync.config import Config, datetime_to_timestamp_str
from openai_compliance_sync.nebuly_client import PermanentRejectionError
from openai_compliance_sync.store import ComplianceStore
from openai_compliance_sync.sync import run_sync

_REAL_HTTPX_CLIENT = httpx.Client


def _ago(*, hours_ago: int = 0, minutes_ago: int = 0) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours_ago, minutes=minutes_ago)


def _config(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    sources: tuple[str, ...] = ("chats",),
    codex_cost_wait_hours: int = 4,
    interaction_idle_minutes: int = 30,
) -> Config:
    return Config(
        nebuly_api_key="nebuly-key",
        nebuly_endpoint="https://nebuly.example/trace",
        compliance_api_key="compliance-key",
        compliance_base_url="https://api.chatgpt.com/v1/compliance",
        compliance_workspace_id="ws-demo",
        compliance_max_requests_per_minute=600,
        anonymize=False,
        from_date=_ago(hours_ago=72),
        to_date=None,
        cache_dir=tmp_path,
        dry_run=dry_run,
        verbose=False,
        sources=sources,
        interaction_idle_minutes=interaction_idle_minutes,
        codex_cost_wait_hours=codex_cost_wait_hours,
        entra_directory=None,
    )


def _codex_event(
    event_id: str,
    event_type: str,
    *,
    session_id: str = "sess-1",
    turn_id: str | None = "turn-1",
    prompt_text: str | None = None,
    response_text: str | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    details: dict[str, Any] = {"session_id": session_id, "turn_id": turn_id}
    if prompt_text is not None:
        details["prompt_text"] = prompt_text
    if response_text is not None:
        details["response_text"] = response_text
        details["status"] = "ok"
    row: dict[str, Any] = {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": timestamp or datetime_to_timestamp_str(_ago(hours_ago=2)),
        "actor": {"user_id": "u-codex", "user_email": "c@example.com"},
        "event_details": details,
    }
    if response_text is not None:
        details["model"] = "codex"
        details["token_usage"] = {"input_tokens": 3, "output_tokens": 5}
    return row


def _codex_turn_row(turn_id: str, *, cost_usd: float = 0.01) -> dict[str, Any]:
    return {
        "event_id": f"turn-file-{turn_id}",
        "timestamp": datetime_to_timestamp_str(_ago(hours_ago=1)),
        "payload": {
            "turn_id": turn_id,
            "cost_usd": cost_usd,
            "token_usage": {
                "uncached_input_tokens": 3,
                "output_tokens": 5,
            },
        },
    }


def _chat_row(
    msg_id: str,
    *,
    author_type: str,
    text: str,
    previous_message_id: str | None = None,
    created_at: str,
) -> dict[str, Any]:
    author: dict[str, Any] = {"type": author_type}
    if author_type == "assistant":
        author["model"] = "gpt-4o"
    return {
        "message": {
            "id": msg_id,
            "created_at": created_at,
            "author": author,
            "content": {"type": "text", "value": text},
        },
        "previous_message_id": previous_message_id,
        "conversation": {"id": "conv-1"},
        "actor": {"user_id": "u1", "user_email": "u@example.com"},
    }


class RecordingNebuly:
    reject_all = False
    all_sent: ClassVar[list[dict[str, Any]]] = []

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if self.reject_all:
            raise PermanentRejectionError(400)
        self.sent.append(payload)
        RecordingNebuly.all_sent.append(payload)


def _make_transport(
    *,
    pages: dict[str, dict[str, Any]],
    files: dict[str, list[dict[str, Any]]],
    fail_download_for: set[str] | None = None,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"ok": True})
        if request.url.path.endswith("/logs"):
            event_type = request.url.params.get("event_type", "")
            page = pages.get(event_type, {"data": [], "has_more": False})
            return httpx.Response(200, json=page)
        file_id = request.url.path.rsplit("/", 1)[-1]
        if fail_download_for and file_id in fail_download_for:
            return httpx.Response(404, request=request)
        rows = files.get(file_id, [])
        body = "\n".join(json.dumps(row) for row in rows) + ("\n" if rows else "")
        return httpx.Response(200, text=body, request=request)

    return httpx.MockTransport(handler)


def _http_client_cm(transport: httpx.MockTransport) -> MagicMock:
    client = _REAL_HTTPX_CLIENT(
        base_url="https://api.chatgpt.com/v1/compliance",
        transport=transport,
    )
    cm = MagicMock()
    cm.__enter__.return_value = client
    cm.__exit__.return_value = None
    return cm


def _patch_run(monkeypatch: pytest.MonkeyPatch, transport: httpx.MockTransport) -> None:
    monkeypatch.setattr("openai_compliance_sync.sync.NebulyClient", RecordingNebuly)
    fake_httpx = types.SimpleNamespace(
        Client=lambda *_args, **_kwargs: _http_client_cm(transport),
        TransportError=httpx.TransportError,
    )
    monkeypatch.setattr("openai_compliance_sync.sync.httpx", fake_httpx)


def test_second_run_sends_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    user = _chat_row(
        "u1",
        author_type="user",
        text="hi",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=2)),
    )
    assistant = _chat_row(
        "a1",
        author_type="assistant",
        text="hey",
        previous_message_id="u1",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=55)),
    )
    closer = _chat_row(
        "u2",
        author_type="user",
        text="next",
        previous_message_id="a1",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=50)),
    )
    transport = _make_transport(
        pages={
            EVENT_CONVERSATION_MESSAGE: {
                "data": [{"id": "file-1"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=45)
                ),
            }
        },
        files={"file-1": [user, assistant, closer]},
    )
    _patch_run(monkeypatch, transport)
    config = _config(tmp_path)
    summary1 = run_sync(config)
    assert summary1.sources["chats"].sent == 1
    summary2 = run_sync(config)
    assert summary2.sources["chats"].sent == 0


def test_reply_in_later_file_emits_on_next_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    user = _chat_row(
        "u1",
        author_type="user",
        text="hi",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=2)),
    )
    assistant = _chat_row(
        "a1",
        author_type="assistant",
        text="hey",
        previous_message_id="u1",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=55)),
    )
    closer = _chat_row(
        "u2",
        author_type="user",
        text="next",
        previous_message_id="a1",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=50)),
    )

    schedule = [
        _ago(hours_ago=1, minutes_ago=58),
        _ago(minutes_ago=20),
        *([_ago(minutes_ago=20)] * 20),
    ]

    def now(_tz: tzinfo | None = None) -> datetime:
        return schedule.pop(0) if schedule else _ago(minutes_ago=20)

    monkeypatch.setattr(
        "openai_compliance_sync.sync.datetime",
        type("DT", (), {"now": staticmethod(now), "UTC": UTC}),
    )

    transport1 = _make_transport(
        pages={
            EVENT_CONVERSATION_MESSAGE: {
                "data": [{"id": "file-1"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=45)
                ),
            }
        },
        files={"file-1": [user]},
    )
    _patch_run(monkeypatch, transport1)
    config = _config(tmp_path)
    first = run_sync(config)
    assert first.sources["chats"].sent == 0
    assert first.sources["chats"].skipped_open == 1

    transport2 = _make_transport(
        pages={
            EVENT_CONVERSATION_MESSAGE: {
                "data": [{"id": "file-2"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=40)
                ),
            }
        },
        files={"file-2": [assistant, closer]},
    )
    _patch_run(monkeypatch, transport2)
    schedule[:] = [_ago(minutes_ago=20), *([_ago(minutes_ago=20)] * 20)]
    second = run_sync(config)
    assert second.sources["chats"].sent == 1


def test_transient_failure_keeps_cursor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    transport = _make_transport(
        pages={
            EVENT_CONVERSATION_MESSAGE: {
                "data": [{"id": "file-1"}, {"id": "file-2"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=45)
                ),
            }
        },
        files={"file-1": []},
        fail_download_for={"file-2"},
    )
    _patch_run(monkeypatch, transport)
    run_sync(_config(tmp_path))
    store = ComplianceStore(tmp_path / "compliance_store.db", "ws-demo", dry_run=False)
    assert store.get_cursor(EVENT_CONVERSATION_MESSAGE) is None


def test_permanent_rejection_is_recorded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    RecordingNebuly.reject_all = True
    try:
        prompt = _codex_event(
            "codex-p1",
            "PROMPT_SENT",
            prompt_text="p",
            timestamp=datetime_to_timestamp_str(_ago(hours_ago=2)),
        )
        response = _codex_event(
            "codex-r1",
            "PROMPT_RESPONSE_RECEIVED",
            response_text="r",
            timestamp=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=58)),
        )
        closer = _codex_event(
            "codex-p2",
            "PROMPT_SENT",
            prompt_text="close",
            timestamp=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=57)),
        )
        transport = _make_transport(
            pages={
                EVENT_CODEX_LOG: {
                    "data": [{"id": "codex-file"}],
                    "has_more": False,
                    "last_end_time": datetime_to_timestamp_str(
                        _ago(hours_ago=1, minutes_ago=55)
                    ),
                }
            },
            files={"codex-file": [prompt, response, closer]},
        )
        _patch_run(monkeypatch, transport)
        summary = run_sync(
            _config(tmp_path, sources=("codex",), codex_cost_wait_hours=0)
        )
        assert summary.sources["codex"].rejected == 1
        store = ComplianceStore(
            tmp_path / "compliance_store.db", "ws-demo", dry_run=False
        )
        assert store.is_emitted("codex", "codex-p1")
    finally:
        RecordingNebuly.reject_all = False


def test_codex_prompt_and_response_across_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prompt = _codex_event(
        "p1",
        "PROMPT_SENT",
        prompt_text="hi",
        timestamp=datetime_to_timestamp_str(_ago(hours_ago=2)),
    )
    response = _codex_event(
        "r1",
        "PROMPT_RESPONSE_RECEIVED",
        response_text="hey",
        timestamp=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=55)),
    )
    closer = _codex_event(
        "p2",
        "PROMPT_SENT",
        prompt_text="next",
        timestamp=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=50)),
    )

    schedule = [
        _ago(hours_ago=1, minutes_ago=58),
        *([_ago(hours_ago=1, minutes_ago=58)] * 10),
    ]

    def now_open(_tz: tzinfo | None = None) -> datetime:
        return schedule.pop(0) if schedule else _ago(hours_ago=1, minutes_ago=58)

    monkeypatch.setattr(
        "openai_compliance_sync.sync.datetime",
        type("DT", (), {"now": staticmethod(now_open), "UTC": UTC}),
    )

    transport1 = _make_transport(
        pages={
            EVENT_CODEX_LOG: {
                "data": [{"id": "codex-1"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=45)
                ),
            }
        },
        files={"codex-1": [prompt]},
    )
    _patch_run(monkeypatch, transport1)
    first = run_sync(_config(tmp_path, sources=("codex",), codex_cost_wait_hours=0))
    assert first.sources["codex"].sent == 0
    assert first.sources["codex"].skipped_open == 1

    transport2 = _make_transport(
        pages={
            EVENT_CODEX_LOG: {
                "data": [{"id": "codex-2"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=40)
                ),
            }
        },
        files={"codex-2": [response, closer]},
    )
    _patch_run(monkeypatch, transport2)
    schedule[:] = [
        _ago(hours_ago=1, minutes_ago=50),
        *([_ago(hours_ago=1, minutes_ago=50)] * 10),
    ]
    monkeypatch.setattr(
        "openai_compliance_sync.sync.datetime",
        type("DT", (), {"now": staticmethod(now_open), "UTC": UTC}),
    )
    second = run_sync(_config(tmp_path, sources=("codex",), codex_cost_wait_hours=0))
    assert second.sources["codex"].sent == 1


def test_codex_waits_for_turn_record_then_sends_with_cost(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prompt_ts = _ago(hours_ago=2)
    prompt = _codex_event(
        "p1",
        "PROMPT_SENT",
        prompt_text="q",
        timestamp=datetime_to_timestamp_str(prompt_ts),
    )
    response = _codex_event(
        "r1",
        "PROMPT_RESPONSE_RECEIVED",
        response_text="a",
        timestamp=datetime_to_timestamp_str(prompt_ts + timedelta(minutes=1)),
    )
    closer = _codex_event(
        "p2",
        "PROMPT_SENT",
        prompt_text="x",
        timestamp=datetime_to_timestamp_str(prompt_ts + timedelta(minutes=2)),
    )
    turn_row = _codex_turn_row("turn-1")

    transport1 = _make_transport(
        pages={
            EVENT_CODEX_TURN: {"data": [], "has_more": False},
            EVENT_CODEX_LOG: {
                "data": [{"id": "codex-a"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=45)
                ),
            },
        },
        files={"codex-a": [prompt, response, closer]},
    )
    _patch_run(monkeypatch, transport1)
    first = run_sync(_config(tmp_path, sources=("codex",), codex_cost_wait_hours=4))
    assert first.sources["codex"].sent == 0
    assert first.sources["codex"].skipped_waiting_cost >= 1

    transport2 = _make_transport(
        pages={
            EVENT_CODEX_TURN: {
                "data": [{"id": "turn-file-1"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(_ago(hours_ago=4)),
            },
            EVENT_CODEX_LOG: {"data": [], "has_more": False},
        },
        files={"turn-file-1": [turn_row]},
    )
    _patch_run(monkeypatch, transport2)
    RecordingNebuly.all_sent.clear()
    second = run_sync(_config(tmp_path, sources=("codex",), codex_cost_wait_hours=4))
    assert second.sources["codex"].sent == 1
    assert RecordingNebuly.all_sent[-1]["interaction"]["cost"] == 10_000


def test_codex_sent_without_cost_after_wait(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    prompt_ts = _ago(hours_ago=10)
    prompt = _codex_event(
        "p1",
        "PROMPT_SENT",
        prompt_text="q",
        timestamp=datetime_to_timestamp_str(prompt_ts),
    )
    response = _codex_event(
        "r1",
        "PROMPT_RESPONSE_RECEIVED",
        response_text="a",
        timestamp=datetime_to_timestamp_str(prompt_ts + timedelta(minutes=1)),
    )
    closer = _codex_event(
        "p2",
        "PROMPT_SENT",
        prompt_text="x",
        timestamp=datetime_to_timestamp_str(prompt_ts + timedelta(minutes=2)),
    )
    transport = _make_transport(
        pages={
            EVENT_CODEX_TURN: {"data": [], "has_more": False},
            EVENT_CODEX_LOG: {
                "data": [{"id": "codex-missing"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(_ago(hours_ago=8)),
            },
        },
        files={"codex-missing": [prompt, response, closer]},
    )
    _patch_run(monkeypatch, transport)
    summary = run_sync(_config(tmp_path, sources=("codex",), codex_cost_wait_hours=4))
    assert summary.sources["codex"].sent == 1
    assert summary.sources["codex"].sent_without_cost == 1


def test_codex_without_turn_id_sends_immediately(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prompt = _codex_event(
        "p1",
        "PROMPT_SENT",
        turn_id=None,
        prompt_text="q",
        timestamp=datetime_to_timestamp_str(_ago(hours_ago=2)),
    )
    response = _codex_event(
        "r1",
        "PROMPT_RESPONSE_RECEIVED",
        turn_id=None,
        response_text="a",
        timestamp=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=55)),
    )
    closer = _codex_event(
        "p2",
        "PROMPT_SENT",
        turn_id=None,
        prompt_text="x",
        timestamp=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=50)),
    )
    transport = _make_transport(
        pages={
            EVENT_CODEX_LOG: {
                "data": [{"id": "codex-nt"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(_ago(hours_ago=1)),
            }
        },
        files={"codex-nt": [prompt, response, closer]},
    )
    _patch_run(monkeypatch, transport)
    summary = run_sync(_config(tmp_path, sources=("codex",), codex_cost_wait_hours=4))
    assert summary.sources["codex"].sent == 1
    assert summary.sources["codex"].skipped_waiting_cost == 0


def test_codex_turn_403_disables_wait(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    prompt = _codex_event("p1", "PROMPT_SENT", prompt_text="q")
    response = _codex_event("r1", "PROMPT_RESPONSE_RECEIVED", response_text="a")
    closer = _codex_event("p2", "PROMPT_SENT", prompt_text="x")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"ok": True})
        if request.url.path.endswith("/logs"):
            event_type = request.url.params.get("event_type", "")
            if event_type == EVENT_CODEX_TURN:
                return httpx.Response(403, request=request)
            if event_type == EVENT_CODEX_LOG:
                return httpx.Response(
                    200,
                    json={
                        "data": [{"id": "codex-403"}],
                        "has_more": False,
                        "last_end_time": datetime_to_timestamp_str(_ago(hours_ago=1)),
                    },
                )
        body = "\n".join(json.dumps(row) for row in [prompt, response, closer]) + "\n"
        return httpx.Response(200, text=body, request=request)

    transport = httpx.MockTransport(handler)
    _patch_run(monkeypatch, transport)
    with caplog.at_level("WARNING"):
        summary = run_sync(
            _config(tmp_path, sources=("codex",), codex_cost_wait_hours=4)
        )
    assert summary.sources["codex"].sent == 1
    assert "CODEX_TURN listing returned 403" in caplog.text


def test_codex_cost_wait_zero_skips_turn_listing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/logs"):
            requested.append(request.url.params.get("event_type", ""))
            if request.url.params.get("event_type") == EVENT_CODEX_TURN:
                raise AssertionError("CODEX_TURN should not be listed")
            return httpx.Response(
                200,
                json={"data": [], "has_more": False},
            )
        return httpx.Response(200, text="", request=request)

    _patch_run(monkeypatch, httpx.MockTransport(handler))
    run_sync(_config(tmp_path, sources=("codex",), codex_cost_wait_hours=0))
    assert EVENT_CODEX_TURN not in requested


def test_invalid_row_skipped_run_continues(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    user = _chat_row(
        "u1",
        author_type="user",
        text="hi",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=2)),
    )
    assistant = _chat_row(
        "a1",
        author_type="assistant",
        text="hey",
        previous_message_id="u1",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=55)),
    )
    closer = _chat_row(
        "u2",
        author_type="user",
        text="next",
        previous_message_id="a1",
        created_at=datetime_to_timestamp_str(_ago(hours_ago=1, minutes_ago=50)),
    )
    transport = _make_transport(
        pages={
            EVENT_CONVERSATION_MESSAGE: {
                "data": [{"id": "file-bad"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=45)
                ),
            }
        },
        files={
            "file-bad": [{"not_a_message": True}, user, assistant, closer],
        },
    )
    _patch_run(monkeypatch, transport)
    with caplog.at_level("WARNING"):
        summary = run_sync(_config(tmp_path))
    assert "Skipping invalid row" in caplog.text
    assert summary.sources["chats"].sent == 1


def test_dry_run_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    transport = _make_transport(
        pages={
            EVENT_CONVERSATION_MESSAGE: {
                "data": [{"id": "file-1"}],
                "has_more": False,
                "last_end_time": datetime_to_timestamp_str(
                    _ago(hours_ago=1, minutes_ago=55)
                ),
            }
        },
        files={"file-1": []},
    )
    _patch_run(monkeypatch, transport)
    run_sync(_config(tmp_path, dry_run=True))
    assert not (tmp_path / "compliance_store.db").exists()
