from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud.trace_v1 import TraceServiceClient
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from . import grpc_init  # noqa: F401

if TYPE_CHECKING:
    from google.cloud.trace_v1.types import Trace

logger = logging.getLogger(__name__)

_INPUT_TOKENS_KEY = "gen_ai.usage.input_tokens"
_OUTPUT_TOKENS_KEY = "gen_ai.usage.output_tokens"


@dataclass(frozen=True)
class TraceData:
    input_tokens: int | None = None
    output_tokens: int | None = None
    time_start: datetime | None = None
    time_end: datetime | None = None


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class TraceClient:
    def __init__(
        self,
        project_id: str,
        *,
        max_workers: int = 16,
        client: TraceServiceClient | None = None,
    ) -> None:
        self._project_id = project_id
        self._max_workers = max_workers
        self._client = client or TraceServiceClient()

    def fetch_traces(self, trace_ids: set[str]) -> dict[str, TraceData]:
        if not trace_ids:
            return {}

        results: dict[str, TraceData] = {}
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(self._get_one, tid): tid for tid in trace_ids}
            for fut in as_completed(futures):
                tid = futures[fut]
                try:
                    data = fut.result()
                    if data is not None:
                        results[tid] = data
                except NotFound:
                    continue
        return results

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
    def _get_trace(self, trace_id: str) -> Trace:
        return self._client.get_trace(
            project_id=self._project_id,
            trace_id=trace_id,
        )

    def _get_one(self, trace_id: str) -> TraceData | None:
        try:
            trace = self._get_trace(trace_id)
        except NotFound:
            raise
        except GoogleAPICallError:
            logger.exception("Failed to fetch trace %s", trace_id)
            return None

        if not trace.spans:
            return None

        root = trace.spans[0]
        time_start = _to_utc(root.start_time) if root.start_time else None
        time_end = _to_utc(root.end_time) if root.end_time else None

        input_tokens = None
        output_tokens = None

        for span in trace.spans:
            labels = span.labels
            if _INPUT_TOKENS_KEY in labels:
                if input_tokens is None:
                    input_tokens = 0
                input_tokens += int(labels[_INPUT_TOKENS_KEY])
            if _OUTPUT_TOKENS_KEY in labels:
                if output_tokens is None:
                    output_tokens = 0
                output_tokens += int(labels[_OUTPUT_TOKENS_KEY])

        return TraceData(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            time_start=time_start,
            time_end=time_end,
        )
