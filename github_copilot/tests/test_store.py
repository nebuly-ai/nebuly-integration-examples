from __future__ import annotations

import json

from github_copilot_sync.models import UsageRecord
from github_copilot_sync.store import Store


def _usage_record(event_id: str, timestamp_ms: int) -> UsageRecord:
    return UsageRecord.model_validate(
        {
            "type": "request",
            "user_id": 7,
            "enterprise_id": 1,
            "endpoint": "/chat/completions",
            "body": json.dumps(json.dumps({"messages": []})),
            "@timestamp": timestamp_ms,
            "truncated": False,
            "event_id": event_id,
            "github_request_id": f"gh-{event_id}",
        }
    )


def test_event_id_dedupe(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    record = _usage_record("evt-1", 1000)

    assert store.insert_usage_record(record) is True
    assert store.insert_usage_record(record) is False
    store.close()


def test_dry_run_does_not_touch_the_cache_file(tmp_path) -> None:
    path = tmp_path / "nested" / "db.sqlite"
    store = Store(path, dry_run=True)
    store.insert_usage_record(_usage_record("evt-dry", 3000))
    store.record_emitted_turn("turn-1", "sent")
    store.set_source_cursor("rest", "123")
    store.close()

    assert not path.exists()
    assert not path.parent.exists()

    fresh = Store(path)
    assert fresh.is_turn_emitted("turn-1") is False
    assert fresh.get_source_cursor("rest") is None
    fresh.close()


def test_pending_request_round_trip(tmp_path) -> None:
    store = Store(tmp_path / "db.sqlite")
    record = _usage_record("evt-2", 2000)
    store.save_pending_request(record)
    pending = store.take_pending_request(record.github_request_id)

    assert pending is not None
    assert pending.event_id == "evt-2"
    assert store.take_pending_request(record.github_request_id) is None
    store.close()
