from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from github_copilot_sync.models import UsageRecord

from .decode import decode_ndjson

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from github_copilot_sync.config import Config

    from . import SourceStats


@dataclass(frozen=True)
class StoredObject:
    key: str
    modified_ms: int


def select_objects(
    objects: Iterable[StoredObject], cursor: str | None, config: Config
) -> list[StoredObject]:
    """Objects to read, oldest write first.

    The cursor is a write time, not a key: an object that lands late has a
    newer write time even when its key sorts before objects already read. The
    cursor trails the newest object by `object_lag_seconds`, so objects still
    being written when the previous run listed are picked up; records already
    ingested are dropped by event_id dedupe.
    """
    lag_ms = config.object_lag_seconds * 1000
    floor_ms = _cursor_ms(cursor)
    if config.from_date is not None:
        # Records are written before their object, so an object last modified
        # before from_date holds nothing newer. The lag absorbs clock skew.
        floor_ms = max(floor_ms, int(config.from_date.timestamp() * 1000) - lag_ms)
    selected = [obj for obj in objects if obj.modified_ms >= floor_ms]
    return sorted(selected, key=lambda obj: (obj.modified_ms, obj.key))


def cursor_for(obj: StoredObject, config: Config) -> str:
    return str(max(0, obj.modified_ms - config.object_lag_seconds * 1000))


def records_from_object(
    raw: bytes, key: str, config: Config, stats: SourceStats
) -> Iterator[UsageRecord]:
    lines, problems = decode_ndjson(raw, source=key)
    stats.absorb_decode(
        files=problems.unreadable_files, lines=problems.unreadable_lines
    )
    to_ms = None if config.to_date is None else int(config.to_date.timestamp() * 1000)
    from_ms = (
        None if config.from_date is None else int(config.from_date.timestamp() * 1000)
    )
    for item in lines:
        record = UsageRecord.from_raw(item)
        if record is None:
            stats.non_usage_events += 1
            continue
        if to_ms is not None and record.timestamp_ms >= to_ms:
            continue
        if from_ms is not None and record.timestamp_ms < from_ms:
            continue
        yield record


def _cursor_ms(cursor: str | None) -> int:
    return 0 if cursor is None else int(cursor)
