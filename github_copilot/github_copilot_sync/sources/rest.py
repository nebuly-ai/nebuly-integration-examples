from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
from httpx import HTTPStatusError
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from github_copilot_sync.config import datetime_to_timestamp_str
from github_copilot_sync.models import UsageRecord

from . import SourceStats

if TYPE_CHECKING:
    from collections.abc import Iterator

    from github_copilot_sync.config import Config

logger = logging.getLogger(__name__)

REST_WINDOW_HOURS = 48
_LINK_RE = re.compile(r'<([^>]+)>;\s*rel="([^"]+)"')


def parse_link_header(header: str | None) -> dict[str, str]:
    if not header:
        return {}
    links: dict[str, str] = {}
    for match in _LINK_RE.finditer(header):
        links[match.group(2)] = match.group(1)
    return links


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    if status == 429:
        return True
    if status == 403:
        remaining = exc.response.headers.get("X-RateLimit-Remaining")
        if remaining == "0":
            return True
    return status >= 500


def _retry_after_seconds(retry_state: RetryCallState) -> float:
    if retry_state.outcome is None:
        return 60.0
    exc = retry_state.outcome.exception()
    if isinstance(exc, HTTPStatusError) and exc.response.status_code == 429:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                return float(retry_after)
            except ValueError:
                pass
        logger.warning("GitHub REST rate limited (429), will retry")
    return 60.0


def _records_from_payload(payload: object) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("usage_records", "records", "data"):
            raw = payload.get(key)
            if isinstance(raw, list):
                return [item for item in raw if isinstance(item, dict)]
        logger.error(
            "Unknown usage-records envelope, keys %s: no records read. "
            "The API schema may have changed.",
            sorted(payload),
        )
    return []


def _ms_to_phrase(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=UTC)
    return f"created:>={datetime_to_timestamp_str(dt)}"


class RestUsageSource:
    def __init__(self, config: Config, client: httpx.Client) -> None:
        self._config = config
        self._overlap_ms = config.rest_overlap_seconds * 1000
        self._client = client
        self.stats = SourceStats()

    def _initial_cursor_ms(self, cursor: str | None) -> int:
        from_ms = (
            None
            if self._config.from_date is None
            else int(self._config.from_date.timestamp() * 1000)
        )
        if cursor is not None:
            return int(cursor) if from_ms is None else max(int(cursor), from_ms)
        if from_ms is not None:
            return from_ms
        return int(
            (datetime.now(UTC) - timedelta(hours=REST_WINDOW_HOURS - 1)).timestamp()
            * 1000
        )

    def _warn_if_outside_rest_window(self, cursor_ms: int) -> None:
        cutoff = datetime.now(UTC) - timedelta(hours=REST_WINDOW_HOURS)
        if datetime.fromtimestamp(cursor_ms / 1000, tz=UTC) < cutoff:
            logger.warning(
                "REST usage-records cursor is older than %s h; "
                "REST only covers the last %s h — use S3 or Azure Blob for backfill",
                REST_WINDOW_HOURS,
                REST_WINDOW_HOURS,
            )

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=_retry_after_seconds,
        reraise=True,
    )
    def _request(
        self, method: str, url: str, *, params: dict[str, str] | None = None
    ) -> httpx.Response:
        response = self._client.request(method, url, params=params)
        response.raise_for_status()
        return response

    def _list_path(self) -> str:
        enterprise = self._config.github_enterprise
        return f"/enterprises/{enterprise}/copilot/usage-records"

    def _fetch_page(
        self,
        *,
        params: dict[str, str] | None = None,
        url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        if url:
            response = self._request("GET", url)
        else:
            response = self._request("GET", self._list_path(), params=params)
        next_url = parse_link_header(response.headers.get("Link")).get("next")
        return _records_from_payload(response.json()), next_url

    def iter_records(self, cursor: str | None) -> Iterator[tuple[UsageRecord, str]]:
        start_ms = self._initial_cursor_ms(cursor)
        self._warn_if_outside_rest_window(start_ms)
        params: dict[str, str] | None = {
            "per_page": "25",
            "order": "asc",
            "phrase": _ms_to_phrase(start_ms),
        }
        next_url: str | None = None
        latest_ms = start_ms

        while True:
            if next_url:
                page, next_url = self._fetch_page(url=next_url)
            else:
                page, next_url = self._fetch_page(params=params)
                params = None
            if not page and not next_url:
                break
            for raw in page:
                record = UsageRecord.from_raw(raw)
                if record is None:
                    self.stats.non_usage_events += 1
                    continue
                if self._config.to_date is not None and record.timestamp_ms >= int(
                    self._config.to_date.timestamp() * 1000
                ):
                    return
                latest_ms = max(latest_ms, record.timestamp_ms)
                new_cursor = str(max(0, latest_ms - self._overlap_ms))
                yield record, new_cursor
            if not next_url:
                break
