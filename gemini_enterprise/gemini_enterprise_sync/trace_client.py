from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import google.auth
import google.auth.transport.requests
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud.trace_v1 import TraceServiceAsyncClient
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from google.auth.credentials import Credentials
    from google.cloud.trace_v1.types import Trace

logger = logging.getLogger(__name__)

_CLOUD_PLATFORM_SCOPE = ("https://www.googleapis.com/auth/cloud-platform",)

_INPUT_TOKENS_KEY = "gen_ai.usage.input_tokens"
_OUTPUT_TOKENS_KEY = "gen_ai.usage.output_tokens"
_MODEL_KEY = "gen_ai.request.model"


@dataclass(frozen=True)
class TraceData:
    input_tokens: int | None = None
    output_tokens: int | None = None
    time_start: datetime | None = None
    time_end: datetime | None = None
    model: str | None = None


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _coerce_token_label(
    labels: MutableMapping[str, str], key: str, trace_id: str
) -> int | None:
    raw = labels.get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning(
            "Ignoring malformed %s=%r on trace %s",
            key,
            raw,
            trace_id,
        )
        return None


def _parse_trace(trace: Trace, trace_id: str) -> TraceData | None:
    if not trace.spans:
        return None

    # Spans should already be sorted by start_time, but just in case.
    spans = sorted(trace.spans, key=lambda x: x.start_time)
    root = spans[0]
    time_start = _to_utc(root.start_time) if root.start_time else None
    time_end = _to_utc(root.end_time) if root.end_time else None

    input_tokens = None
    output_tokens = None
    model_name = None

    for span in spans:
        labels = span.labels
        if _INPUT_TOKENS_KEY in labels:
            parsed = _coerce_token_label(labels, _INPUT_TOKENS_KEY, trace_id)
            if parsed is not None:
                input_tokens = (input_tokens or 0) + parsed
        if _OUTPUT_TOKENS_KEY in labels:
            parsed = _coerce_token_label(labels, _OUTPUT_TOKENS_KEY, trace_id)
            if parsed is not None:
                output_tokens = (output_tokens or 0) + parsed
        if _MODEL_KEY in labels:
            model_name = labels[_MODEL_KEY]

    return TraceData(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        time_start=time_start,
        time_end=time_end,
        model=model_name,
    )


class TraceClient:
    def __init__(
        self,
        project_id: str,
        *,
        concurrency: int = 32,
        async_client: TraceServiceAsyncClient | None = None,
        credentials: Credentials | None = None,
    ) -> None:
        self._project_id = project_id
        self._concurrency = concurrency
        self._async_client = async_client
        self._credentials = credentials
        if async_client is None and credentials is None:
            creds, _ = google.auth.default(scopes=_CLOUD_PLATFORM_SCOPE)
            self._credentials = creds

    def fetch_traces(self, trace_ids: set[str]) -> dict[str, TraceData]:
        if not trace_ids:
            return {}
        return asyncio.run(self._fetch_all(trace_ids))

    async def _fetch_all(self, trace_ids: set[str]) -> dict[str, TraceData]:
        if self._async_client is not None:
            client = self._async_client
            owns_client = False
        else:
            if self._credentials is not None and not self._credentials.valid:
                self._credentials.refresh(google.auth.transport.requests.Request())  # type: ignore[no-untyped-call]
            client = TraceServiceAsyncClient(credentials=self._credentials)
            owns_client = True
        semaphore = asyncio.Semaphore(self._concurrency)

        async def fetch_one(trace_id: str) -> tuple[str, TraceData | None]:
            async with semaphore:
                try:
                    trace = await self._get_trace(client, trace_id)
                except NotFound:
                    return trace_id, None
                except GoogleAPICallError:
                    logger.exception("Failed to fetch trace %s", trace_id)
                    return trace_id, None
                return trace_id, _parse_trace(trace, trace_id)

        try:
            pairs = await asyncio.gather(*(fetch_one(tid) for tid in trace_ids))
        finally:
            if owns_client:
                await client.transport.close()  # type: ignore[no-untyped-call]

        return {tid: data for tid, data in pairs if data is not None}

    @retry(
        retry=retry_if_exception(
            lambda exc: (
                isinstance(exc, GoogleAPICallError) and not isinstance(exc, NotFound)
            )
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        reraise=True,
    )
    async def _get_trace(self, client: TraceServiceAsyncClient, trace_id: str) -> Trace:
        return await client.get_trace(
            project_id=self._project_id,
            trace_id=trace_id,
        )
