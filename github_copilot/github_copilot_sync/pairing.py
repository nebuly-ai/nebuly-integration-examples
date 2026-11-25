"""Pair request and response usage records on github_request_id."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .models import Call, UsageRecord

if TYPE_CHECKING:
    from .store import Store


@dataclass
class PairingCounters:
    unpaired_requests: int = 0


class PairingEngine:
    def __init__(self, pair_wait_minutes: int, store: Store) -> None:
        self._pair_wait_ms = pair_wait_minutes * 60_000
        self._store = store
        self.counters = PairingCounters()

    def ingest(self, record: UsageRecord) -> list[Call]:
        if record.type == "request":
            self._store.save_pending_request(record)
            return []
        pending = self._store.take_pending_request(record.github_request_id)
        if pending is None:
            return []
        return [
            Call(
                github_request_id=record.github_request_id,
                user_id=pending.user_id,
                endpoint=pending.endpoint,
                request_body=pending.body,
                response_body=record.body,
                request_timestamp_ms=pending.timestamp_ms,
                response_timestamp_ms=record.timestamp_ms,
                request_event_id=pending.event_id,
                response_event_id=record.event_id,
                request_truncated=pending.truncated,
                response_truncated=record.truncated,
            )
        ]

    def expire_unpaired(self, watermark_ms: int) -> int:
        cutoff = watermark_ms - self._pair_wait_ms
        expired = self._store.expire_pending_requests_before(cutoff)
        self.counters.unpaired_requests += expired
        return expired
