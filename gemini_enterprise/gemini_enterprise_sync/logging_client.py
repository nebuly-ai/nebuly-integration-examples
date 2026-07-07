from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from google.cloud import logging_v2

from . import grpc_init  # noqa: F401
from .config import datetime_to_timestamp_str

_LOG_NAME_SUFFIX = "discoveryengine.googleapis.com%2Fgemini_enterprise_user_activity"


@dataclass(frozen=True)
class LogRecord:
    timestamp: datetime
    insert_id: str
    trace_id: str | None
    payload: dict[str, Any]


class LoggingClient:
    def __init__(
        self,
        project_id: str,
        *,
        page_size: int = 1000,
        client: logging_v2.Client | None = None,
    ) -> None:
        self._project_id = project_id
        self._page_size = page_size
        self._client = client or logging_v2.Client(project=project_id)  # type: ignore[no-untyped-call]

    def _build_filter(self, *, since: datetime | None, until: datetime) -> str:
        log_name = (
            f'logName="projects/{self._project_id}/logs/{_LOG_NAME_SUFFIX}" '
            "AND (jsonPayload.serviceTextReply:* OR protoPayload.response.reply:*)"
        )
        parts = [log_name]
        if since is not None:
            parts.append(f'timestamp >= "{datetime_to_timestamp_str(since)}"')
        parts.append(f'timestamp <= "{datetime_to_timestamp_str(until)}"')
        return " AND ".join(parts)

    def fetch_batch(
        self, *, since: datetime | None, until: datetime, limit: int
    ) -> list[LogRecord]:
        entries = self._client.list_entries(  # type: ignore[no-untyped-call]
            resource_names=[f"projects/{self._project_id}"],
            filter_=self._build_filter(since=since, until=until),
            order_by="timestamp asc",
            page_size=min(self._page_size, limit),
            max_results=limit,
        )

        records: list[LogRecord] = []
        for entry in entries:
            payload = entry.payload
            if not isinstance(payload, dict):
                continue

            trace_id: str | None = None
            if entry.trace:
                trace_id = entry.trace.rsplit("/", 1)[-1]

            timestamp = entry.timestamp
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            else:
                timestamp = timestamp.astimezone(UTC)

            records.append(
                LogRecord(
                    timestamp=timestamp,
                    insert_id=entry.insert_id or "",
                    trace_id=trace_id,
                    payload=payload,
                )
            )
            if len(records) >= limit:
                break

        return records
