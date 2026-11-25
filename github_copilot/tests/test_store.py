from __future__ import annotations

import json
import sqlite3
from contextlib import suppress
from typing import TYPE_CHECKING

from github_copilot_sync.models import UsageRecord
from github_copilot_sync.store import Store

if TYPE_CHECKING:
    from pathlib import Path


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


def test_event_id_dedupe(tmp_path: Path) -> None:
    store = Store(tmp_path / "db.sqlite")
    record = _usage_record("evt-1", 1000)

    assert store.insert_usage_record(record) is True
    assert store.insert_usage_record(record) is False
    store.close()


def test_dry_run_does_not_touch_the_cache_file(tmp_path: Path) -> None:
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


def test_pending_request_round_trip(tmp_path: Path) -> None:
    store = Store(tmp_path / "db.sqlite")
    record = _usage_record("evt-2", 2000)
    store.save_pending_request(record)
    pending = store.take_pending_request(record.github_request_id)

    assert pending is not None
    assert pending.event_id == "evt-2"
    assert store.take_pending_request(record.github_request_id) is None
    store.close()


def test_usage_record_keeps_only_dedupe_columns(tmp_path: Path) -> None:
    store = Store(tmp_path / "db.sqlite")
    store.close()

    with sqlite3.connect(tmp_path / "db.sqlite") as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(usage_record)")]

    assert columns == ["event_id", "timestamp_ms"]


def test_prune_drops_dedupe_rows_older_than_retention_behind_newest(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "db.sqlite")
    store.insert_usage_record(_usage_record("old", 1_000))
    store.insert_usage_record(_usage_record("edge", 5_000))
    store.insert_usage_record(_usage_record("new", 10_000))

    assert store.prune_usage_records(retention_ms=5_000) == 1

    assert store.insert_usage_record(_usage_record("old", 1_000)) is True
    assert store.insert_usage_record(_usage_record("edge", 5_000)) is False
    assert store.insert_usage_record(_usage_record("new", 10_000)) is False
    store.close()


def test_prune_on_empty_table_is_a_noop(tmp_path: Path) -> None:
    store = Store(tmp_path / "db.sqlite")
    assert store.prune_usage_records(retention_ms=1) == 0
    store.close()


def test_store_prunes_on_open(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite"
    store = Store(path)
    store.insert_usage_record(_usage_record("ancient", 1_000))
    store.insert_usage_record(_usage_record("recent", 10 * 24 * 3_600_000))
    store.close()

    reopened = Store(path)

    assert reopened.insert_usage_record(_usage_record("ancient", 1_000)) is True
    assert (
        reopened.insert_usage_record(_usage_record("recent", 10 * 24 * 3_600_000))
        is False
    )
    reopened.close()


def _raise_boom() -> None:
    raise RuntimeError("boom")


def test_no_commit_per_row_inside_transaction(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite"
    store = Store(path)
    with suppress(RuntimeError), store.transaction():
        store.insert_usage_record(_usage_record("evt-tx", 1000))
        _raise_boom()

    assert store.insert_usage_record(_usage_record("evt-tx", 1000)) is True
    store.close()


def test_source_cursors_are_independent_per_source(tmp_path: Path) -> None:
    store = Store(tmp_path / "db.sqlite")
    store.set_source_cursor("rest", "111")
    store.set_source_cursor("s3", "222")
    store.close()

    reopened = Store(tmp_path / "db.sqlite")
    assert reopened.get_source_cursor("rest") == "111"
    assert reopened.get_source_cursor("s3") == "222"
    assert reopened.get_source_cursor("azure_blob") is None
    reopened.close()
