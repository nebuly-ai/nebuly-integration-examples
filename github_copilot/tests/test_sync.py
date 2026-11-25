from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import httpx
import pytest
from github_copilot_sync.config import Config
from github_copilot_sync.sources.rest import RestUsageSource
from github_copilot_sync.sync import run_sync

from tests.test_sources import _config


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


def _sync_config(tmp_path: Path, *, dry_run: bool = False) -> Config:
    base = _config(
        sources=("rest",),
        from_date=datetime(2026, 1, 1, tzinfo=UTC),
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

    def build_sources(config: Config) -> dict[str, RestUsageSource]:
        client = httpx.Client(
            base_url=config.github_api_url,
            transport=httpx.MockTransport(handler),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {config.github_token}",
                "X-GitHub-Api-Version": config.github_api_version,
            },
        )
        return {"rest": RestUsageSource(config, client=client)}

    monkeypatch.setattr("github_copilot_sync.sync.NebulyClient", RecordingNebuly)
    monkeypatch.setattr("github_copilot_sync.sync.build_sources", build_sources)


@pytest.fixture(autouse=True)
def _reset_recording() -> None:
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

    config = _sync_config(tmp_path)
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

    config = _sync_config(tmp_path, dry_run=True)
    summary = run_sync(config)
    assert summary.sources["rest"].sent == 1
    assert RecordingNebuly.all_sent == []
    assert not config.store_path.exists()

    real = _sync_config(tmp_path)
    real_summary = run_sync(real)
    assert real_summary.sources["rest"].sent == 1
    assert len(RecordingNebuly.all_sent) == 1
