from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from langfuse_sync.http_retry import http_retry, parse_retry_after

if TYPE_CHECKING:
    import httpx

    from langfuse_sync.config import Config
    from langfuse_sync.http_retry import RateLimiter
    from langfuse_sync.models import LangfuseObservation, LangfuseTrace

logger = logging.getLogger(__name__)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


class LangfuseClient:
    def __init__(
        self, http: httpx.AsyncClient, config: Config, limiter: RateLimiter
    ) -> None:
        self._http = http
        self._config = config
        self._limiter = limiter
        self._auth = (config.langfuse_public_key, config.langfuse_secret_key)
        self._base = config.langfuse_base_url

    @http_retry
    async def _get(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> httpx.Response:
        async with self._limiter.slot():
            response = await self._http.get(
                f"{self._base}{path}",
                auth=self._auth,
                params=params,
                timeout=60.0,
            )
        if response.status_code == 429:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                self._limiter.pause(retry_after)
        if response.is_error:
            response.raise_for_status()
        return response

    async def _get_page(
        self,
        path: str,
        params: dict[str, str | int],
        page: int,
        label: str,
    ) -> tuple[list[dict[str, Any]], int]:
        response = await self._get(path, params={**params, "page": page})
        payload: dict[str, Any] = response.json()
        rows: list[dict[str, Any]] = payload.get("data") or []
        meta = payload.get("meta") or {}
        total_pages = int(meta.get("totalPages") or page)
        logger.info(
            "Langfuse %s page %d/%d (%d rows)", label, page, total_pages, len(rows)
        )
        return rows, total_pages

    async def _paginate(
        self, path: str, params: dict[str, str | int], label: str
    ) -> list[dict[str, Any]]:
        """Fetch page 1 for the page count, then the rest concurrently in order."""
        rows, total_pages = await self._get_page(path, params, 1, label)
        if not rows or total_pages <= 1:
            return rows
        remaining = await asyncio.gather(
            *(
                self._get_page(path, params, page, label)
                for page in range(2, total_pages + 1)
            )
        )
        for page_rows, _ in remaining:
            rows.extend(page_rows)
        return rows

    async def get_traces(
        self, start: datetime, end: datetime, limit: int = 100
    ) -> list[LangfuseTrace]:
        rows = await self._paginate(
            "/api/public/traces",
            {
                "limit": limit,
                "fromTimestamp": _iso(start),
                "toTimestamp": _iso(end),
                "orderBy": "timestamp.asc",
            },
            "traces",
        )
        return cast("list[LangfuseTrace]", rows)

    async def get_observations_by_trace_id(
        self, start: datetime, end: datetime, limit: int = 100
    ) -> dict[str, list[LangfuseObservation]]:
        rows = await self._paginate(
            "/api/public/observations",
            {
                "limit": limit,
                "fromStartTime": _iso(start),
                "toStartTime": _iso(end),
            },
            "observations",
        )
        by_trace: dict[str, list[LangfuseObservation]] = defaultdict(list)
        for observation in rows:
            trace_id = observation.get("traceId")
            if trace_id:
                by_trace[trace_id].append(cast("LangfuseObservation", observation))
        return dict(by_trace)
