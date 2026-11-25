from __future__ import annotations

import json

from github_copilot_sync.models import UsageRecord
from github_copilot_sync.pairing import PairingEngine
from github_copilot_sync.store import Store


def _record(
    record_type: str,
    github_request_id: str,
    timestamp_ms: int,
    event_id: str,
) -> UsageRecord:
    body = json.dumps(json.dumps({"messages": []}))
    return UsageRecord.model_validate(
        {
            "type": record_type,
            "user_id": 1,
            "enterprise_id": 99,
            "endpoint": "/chat/completions",
            "body": body,
            "@timestamp": timestamp_ms,
            "truncated": False,
            "event_id": event_id,
            "github_request_id": github_request_id,
        }
    )


def test_pairs_request_then_response(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    engine = PairingEngine(pair_wait_minutes=15, store=store)
    req = _record("request", "rid-1", 1_000, "evt-req")
    resp = _record("response", "rid-1", 2_000, "evt-resp")

    assert engine.ingest(req) == []
    calls = engine.ingest(resp)

    assert len(calls) == 1
    assert calls[0].github_request_id == "rid-1"
    store.close()


def test_expire_unpaired_counts_dropped(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    engine = PairingEngine(pair_wait_minutes=10, store=store)
    req = _record("request", "rid-late", 1_000, "evt-req")

    engine.ingest(req)
    expired = engine.expire_unpaired(watermark_ms=1_000 + 10 * 60_000)

    assert expired == 1
    assert engine.counters.unpaired_requests == 1
    store.close()
