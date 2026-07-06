from __future__ import annotations

from datetime import UTC, datetime

from google.cloud import bigquery

from .logging_client import LogRecord


class BigQueryLoggingClient:
    def __init__(
        self,
        project_id: str,
        table: str,
        *,
        location: str | None = None,
        client: bigquery.Client | None = None,
    ) -> None:
        self._project_id = project_id
        self._table = table
        self._location = location
        self._client = client or bigquery.Client(project=project_id)
        self._wildcard = table.endswith("*")

    def _suffix_bounds(
        self, *, since: datetime | None, until: datetime
    ) -> tuple[str, str]:
        since_dt = since or until
        return (
            since_dt.astimezone(UTC).strftime("%Y%m%d"),
            until.astimezone(UTC).strftime("%Y%m%d"),
        )

    def _build_query(self, *, since: datetime | None) -> str:
        parts = [
            "SELECT timestamp, insertId, trace, jsonPayload",
            f"FROM `{self._table}`",
            "WHERE jsonPayload IS NOT NULL",
            "AND timestamp <= @until",
        ]
        if since is not None:
            parts.append("AND timestamp >= @since")
        if self._wildcard:
            parts.append("AND _TABLE_SUFFIX BETWEEN @sfx_start AND @sfx_end")
        parts.extend(["ORDER BY timestamp ASC", "LIMIT @limit"])
        return "\n".join(parts)

    def _build_params(
        self, *, since: datetime | None, until: datetime, limit: int
    ) -> list[bigquery.ScalarQueryParameter]:
        params: list[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("until", "TIMESTAMP", until),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
        if since is not None:
            params.append(bigquery.ScalarQueryParameter("since", "TIMESTAMP", since))
        if self._wildcard:
            sfx_start, sfx_end = self._suffix_bounds(since=since, until=until)
            params.extend(
                [
                    bigquery.ScalarQueryParameter("sfx_start", "STRING", sfx_start),
                    bigquery.ScalarQueryParameter("sfx_end", "STRING", sfx_end),
                ]
            )
        return params

    def fetch_batch(
        self, *, since: datetime | None, until: datetime, limit: int
    ) -> list[LogRecord]:
        job_config = bigquery.QueryJobConfig(
            query_parameters=self._build_params(since=since, until=until, limit=limit)
        )
        rows = self._client.query(
            self._build_query(since=since),
            job_config=job_config,
            location=self._location,
        ).result()

        records: list[LogRecord] = []
        for row in rows:
            payload = row["jsonPayload"]
            if not isinstance(payload, dict):
                continue

            trace_id: str | None = None
            trace = row["trace"]
            if trace:
                trace_id = trace.rsplit("/", 1)[-1]

            timestamp = row["timestamp"]
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            else:
                timestamp = timestamp.astimezone(UTC)

            records.append(
                LogRecord(
                    timestamp=timestamp,
                    insert_id=row["insertId"] or "",
                    trace_id=trace_id,
                    payload=payload,
                )
            )
            if len(records) >= limit:
                break

        return records
