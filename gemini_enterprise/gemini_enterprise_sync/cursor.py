from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

if TYPE_CHECKING:
    from pathlib import Path

_CURSOR_VERSION = 1


@dataclass(frozen=True)
class CursorState:
    last_timestamp: datetime | None
    last_insert_id: str | None


class Cursor:
    def __init__(self, path: Path, *, dry_run: bool = False) -> None:
        self._path = path
        self._dry_run = dry_run
        self._state = CursorState(last_timestamp=None, last_insert_id=None)

    @property
    def state(self) -> CursorState:
        return self._state

    def load(self) -> CursorState:
        if not self._path.exists():
            self._state = CursorState(last_timestamp=None, last_insert_id=None)
            return self._state

        data = json.loads(self._path.read_text(encoding="utf-8"))
        last_ts = data.get("last_timestamp")
        self._state = CursorState(
            last_timestamp=timestamp_str_to_datetime(last_ts) if last_ts else None,
            last_insert_id=data.get("last_insert_id"),
        )
        return self._state

    def advance(self, *, timestamp: datetime, insert_id: str) -> None:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        else:
            timestamp = timestamp.astimezone(UTC)

        if (
            self._state.last_timestamp is not None
            and timestamp < self._state.last_timestamp
        ):
            raise ValueError(
                f"Cursor cannot move backwards: {timestamp} < "
                f"{self._state.last_timestamp}"
            )

        self._state = CursorState(last_timestamp=timestamp, last_insert_id=insert_id)

        if self._dry_run:
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": _CURSOR_VERSION,
            "last_timestamp": datetime_to_timestamp_str(timestamp),
            "last_insert_id": insert_id,
            "updated_at": datetime_to_timestamp_str(datetime.now(UTC)),
        }
        tmp_path = self._path.with_suffix(".json.tmp")
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            os.close(fd)
            raise
        os.replace(tmp_path, self._path)
