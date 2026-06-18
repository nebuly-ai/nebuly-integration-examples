from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from compliance_sync.checkpoint import Checkpoint


def test_checkpoint_resume(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"
    ts = datetime(2025, 6, 15, 12, 34, 56, tzinfo=timezone.utc)
    key = "chat1:msg1"

    cp = Checkpoint(path, "org_demo")
    assert not cp.is_sent("user_1", ts, key)
    cp.record_sent("user_1", ts, key)
    cp.save()

    cp2 = Checkpoint(path, "org_demo")
    assert cp2.is_sent("user_1", ts, key)
    assert not cp2.is_sent("user_1", ts, "chat1:msg2")

    later = datetime(2025, 6, 16, 0, 0, tzinfo=timezone.utc)
    assert not cp2.is_sent("user_1", later, "chat2:msg3")

    earlier = datetime(2025, 6, 14, 0, 0, tzinfo=timezone.utc)
    assert cp2.is_sent("user_1", earlier, "anything")


def test_view_snapshot_is_stable(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"
    cp = Checkpoint(path, "org_demo")
    snapshot = cp.view("user_1")

    later = datetime(2025, 6, 16, 0, 0, tzinfo=timezone.utc)
    cp.record_sent("user_1", later, "chat_later:msg_later")

    earlier = datetime(2025, 6, 14, 0, 0, tzinfo=timezone.utc)
    assert not snapshot.is_sent(earlier, "chat_earlier:msg_earlier")
    assert cp.is_sent("user_1", earlier, "chat_earlier:msg_earlier")


def test_updated_at_gte(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"
    from_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    watermark = datetime(2025, 6, 1, tzinfo=timezone.utc)

    cp = Checkpoint(path, "org_demo")
    cp.record_sent("user_1", watermark, "c:m")
    assert cp.updated_at_gte("user_1", from_date) == watermark
    assert cp.updated_at_gte("user_2", from_date) == from_date
