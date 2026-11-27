"""Pair request and response usage records on github_request_id."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .models import Call, UsageRecord
from .store import PendingRecord

if TYPE_CHECKING:
    from .store import Store


@dataclass
class PairingCounters:
    unpaired_requests: int = 0
    unpaired_responses: int = 0


class PairingEngine:
    def __init__(self, pair_wait_minutes: int, store: Store) -> None:
        self._pair_wait_ms = pair_wait_minutes * 60_000
        self._store = store
        self.counters = PairingCounters()

    def ingest(self, record: UsageRecord) -> list[Call]:
        """Pair a record with its counterpart, or buffer it until that arrives.

        Object stores do not guarantee ordering, so a response can be read
        before its request.
        """
        if record.type == "request":
            response = self._store.take_pending_response(record.github_request_id)
            if response is None:
                self._store.save_pending_request(record)
                return []
            request = PendingRecord.from_usage_record(record)
        else:
            pending_request = self._store.take_pending_request(record.github_request_id)
            if pending_request is None:
                self._store.save_pending_response(record)
                return []
            request = pending_request
            response = PendingRecord.from_usage_record(record)
        return [
            Call(
                github_request_id=record.github_request_id,
                user_id=request.user_id,
                endpoint=request.endpoint,
                request_body=request.body,
                response_body=response.body,
                request_timestamp_ms=request.timestamp_ms,
                response_timestamp_ms=response.timestamp_ms,
                request_event_id=request.event_id,
                response_event_id=response.event_id,
                request_truncated=request.truncated,
                response_truncated=response.truncated,
            )
        ]

    def expire_unpaired(self, watermark_ms: int) -> int:
        cutoff = watermark_ms - self._pair_wait_ms
        expired_requests = self._store.expire_pending_requests_before(cutoff)
        expired_responses = self._store.expire_pending_responses_before(cutoff)
        self.counters.unpaired_requests += expired_requests
        self.counters.unpaired_responses += expired_responses
        return expired_requests + expired_responses
