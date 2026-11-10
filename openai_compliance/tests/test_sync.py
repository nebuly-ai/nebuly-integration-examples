from __future__ import annotations

import json
import types
from datetime import UTC, datetime, timedelta, tzinfo
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
from openai_compliance_sync.compliance_client import (
    EVENT_CODEX_LOG,
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
    tmp_path: Path, *, dry_run: bool = False, sources: tuple[str, ...] = ("chats",)
) -> Config:
    return Config(
        nebuly_api_key="nebuly-key",
        nebuly_endpoint="https://nebuly.example/trace",
        compliance_api_key="compliance-key",
        compliance_base_url="https://api.chatgpt.com/v1/compliance",
        compliance_principal_id="ws-demo",
        compliance_max_requests_per_minute=600,
        anonymize=False,
        from_date=_ago(hours_ago=72),
        to_date=None,
        cache_dir=tmp_path,
        dry_run=dry_run,
        verbose=False,
        sources=sources,
        interaction_idle_minutes=30,
        entra_directory=None,
    )


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

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if self.reject_all:
            raise PermanentRejectionError(400)
        self.sent.append(payload)


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
        codex_row = {
            "event_id": "codex-1",
            "client_id": "cli-1",
            "timestamp": datetime_to_timestamp_str(_ago(hours_ago=2)),
            "event_details": {
                "detail_type": "PROMPT_RESPONSE_RECEIVED",
                "session_id": "sess-1",
                "prompt_text": "p",
                "response_text": "r",
                "status": "ok",
                "model": "codex",
                "token_usage": {"input_tokens": 1, "output_tokens": 2},
            },
        }
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
            files={"codex-file": [codex_row]},
        )
        _patch_run(monkeypatch, transport)
        summary = run_sync(_config(tmp_path, sources=("codex",)))
        assert summary.sources["codex"].rejected == 1
        store = ComplianceStore(
            tmp_path / "compliance_store.db", "ws-demo", dry_run=False
        )
        assert store.is_emitted("codex", "codex-1")
    finally:
        RecordingNebuly.reject_all = False


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
