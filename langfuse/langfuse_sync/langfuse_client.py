from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx
from httpx import HTTPStatusError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

if TYPE_CHECKING:
    from langfuse_sync.config import Config
    from langfuse_sync.models import (
        LangfuseObservation,
        LangfuseObservationsResponse,
        LangfuseTrace,
        LangfuseTracesResponse,
    )

logger = logging.getLogger(__name__)

_PAGE_PAUSE_S = 0.5


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    return status in {429} or status >= 500


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


class LangfuseClient:
    def __init__(self, http: httpx.Client, config: Config) -> None:
        self._http = http
        self._config = config
        self._auth = (config.langfuse_public_key, config.langfuse_secret_key)
        self._base = config.langfuse_base_url

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    def _get(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> httpx.Response:
        response = self._http.get(
            f"{self._base}{path}",
            auth=self._auth,
            params=params,
            timeout=60.0,
        )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after else 2.0
            logger.warning("Langfuse rate limited (429), sleeping %.1fs", wait_s)
            time.sleep(wait_s)
            response.raise_for_status()
        if response.is_error:
            response.raise_for_status()
        return response

    def get_traces(
        self, start: datetime, end: datetime, limit: int = 100
    ) -> list[LangfuseTrace]:
        page = 1
        full_traces: list[LangfuseTrace] = []
        while True:
            if page > 1:
                time.sleep(_PAGE_PAUSE_S)
            response = self._get(
                "/api/public/traces",
                params={
                    "page": page,
                    "limit": limit,
                    "fromTimestamp": _iso(start),
                    "toTimestamp": _iso(end),
                    "orderBy": "timestamp.asc",
                },
            )
            payload: LangfuseTracesResponse = response.json()
            traces = payload.get("data") or []
            if not traces:
                break
            full_traces.extend(traces)
            meta = payload.get("meta") or {}
            total_pages = int(meta.get("totalPages") or page)
            logger.info(
                "Langfuse traces page %d/%d (%d rows)",
                page,
                total_pages,
                len(full_traces),
            )
            if page >= total_pages:
                break
            page += 1
        return full_traces

    def get_observations_by_trace_id(
        self, start: datetime, end: datetime, limit: int = 100
    ) -> dict[str, list[LangfuseObservation]]:
        page = 1
        by_trace: dict[str, list[LangfuseObservation]] = defaultdict(list)
        while True:
            if page > 1:
                time.sleep(_PAGE_PAUSE_S)
            response = self._get(
                "/api/public/observations",
                params={
                    "page": page,
                    "limit": limit,
                    "fromStartTime": _iso(start),
                    "toStartTime": _iso(end),
                },
            )
            payload: LangfuseObservationsResponse = response.json()
            observations = payload.get("data") or []
            if not observations:
                break
            for observation in observations:
                trace_id = observation.get("traceId")
                if trace_id:
                    by_trace[trace_id].append(observation)
            meta = payload.get("meta") or {}
            total_pages = int(meta.get("totalPages") or page)
            logger.info(
                "Langfuse observations page %d/%d (%d rows)",
                page,
                total_pages,
                sum(len(v) for v in by_trace.values()),
            )
            if page >= total_pages:
                break
            page += 1
        return dict(by_trace)
