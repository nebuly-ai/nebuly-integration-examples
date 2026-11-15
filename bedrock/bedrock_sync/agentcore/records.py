"""Source-scoped CloudWatch envelopes shared by ingestion and parsing."""

from __future__ import annotations

from dataclasses import dataclass, field

from bedrock_sync.shared.keys import canonical_key

__all__ = ["LogPage", "RawLogRecord", "SourceScope"]


@dataclass(frozen=True)
class SourceScope:
    account_id: str
    region: str
    source_id: str

    @property
    def key(self) -> str:
        return canonical_key(
            "agentcore", 1, self.account_id, self.region, self.source_id
        )

    def record_key(self, log_group: str, event_id: str) -> str:
        return canonical_key(self.key, log_group, event_id)

    def interaction_key(self, trace_id: str, root_span_id: str) -> str:
        return canonical_key(self.key, trace_id, root_span_id)

    def conversation_key(self, session_id: str) -> str:
        return canonical_key(self.key, session_id)


@dataclass(frozen=True)
class RawLogRecord:
    log_group: str
    log_stream: str
    event_id: str
    timestamp_ms: int
    ingestion_time_ms: int
    message: str = field(repr=False)


@dataclass(frozen=True)
class LogPage:
    log_group: str
    start_ms: int
    end_ms: int
    records: tuple[RawLogRecord, ...]
    complete: bool
    trace_id: str | None = None
