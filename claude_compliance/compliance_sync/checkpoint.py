from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

logger = logging.getLogger(__name__)


@dataclass
class UserCheckpoint:
    last_updated_at: datetime | None = None
    boundary_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CheckpointView:
    last_updated_at: datetime | None
    boundary_ids: frozenset[str]

    def is_sent(self, ts: datetime, key: str) -> bool:
        if self.last_updated_at is None:
            return False
        if ts < self.last_updated_at:
            return True
        if ts == self.last_updated_at:
            return key in self.boundary_ids
        return False


class Checkpoint:
    def __init__(self, path: Path, organization_uuid: str) -> None:
        self._path = path
        self._organization_uuid = organization_uuid
        self._users: dict[str, UserCheckpoint] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        with self._path.open() as f:
            data = json.load(f)
        if data.get("organization_uuid") != self._organization_uuid:
            logger.warning(
                "Checkpoint org uuid mismatch (%s vs %s), starting fresh",
                data.get("organization_uuid"),
                self._organization_uuid,
            )
            return
        users_raw = data.get("users", {})
        if not isinstance(users_raw, dict):
            return
        for user_id, cursor in users_raw.items():
            if not isinstance(user_id, str) or not isinstance(cursor, dict):
                continue
            ts_raw = cursor.get("last_updated_at")
            if not isinstance(ts_raw, str):
                continue
            boundary_ids: list[str] = []
            ids_raw = cursor.get("boundary_ids")
            if isinstance(ids_raw, list):
                boundary_ids = [item for item in ids_raw if isinstance(item, str)]
            self._users[user_id] = UserCheckpoint(
                last_updated_at=timestamp_str_to_datetime(ts_raw),
                boundary_ids=boundary_ids,
            )

    def updated_at_gte(self, user_id: str, from_date: datetime | None) -> datetime | None:
        cursor = self._users.get(user_id)
        watermark = cursor.last_updated_at if cursor else None
        if from_date is None:
            return watermark
        if watermark is None:
            return from_date
        return max(from_date, watermark)

    def view(self, user_id: str) -> CheckpointView:
        cursor = self._users.get(user_id)
        if cursor is None:
            return CheckpointView(None, frozenset())
        return CheckpointView(cursor.last_updated_at, frozenset(cursor.boundary_ids))

    def is_sent(self, user_id: str, ts: datetime, key: str) -> bool:
        return self.view(user_id).is_sent(ts, key)

    def record_sent(self, user_id: str, ts: datetime, key: str) -> None:
        cursor = self._users.setdefault(user_id, UserCheckpoint())
        if cursor.last_updated_at is None or ts > cursor.last_updated_at:
            cursor.last_updated_at = ts
            cursor.boundary_ids = [key]
        elif ts == cursor.last_updated_at and key not in cursor.boundary_ids:
            cursor.boundary_ids.append(key)

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "organization_uuid": self._organization_uuid,
            "users": {
                user_id: {
                    "last_updated_at": datetime_to_timestamp_str(cursor.last_updated_at),
                    "boundary_ids": cursor.boundary_ids,
                }
                for user_id, cursor in self._users.items()
                if cursor.last_updated_at is not None
            },
        }
        fd, tmp_path = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, indent=2)
                f.write("\n")
            os.replace(tmp_path, self._path)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise