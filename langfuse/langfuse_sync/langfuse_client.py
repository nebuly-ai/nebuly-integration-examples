"""Langfuse public API client."""

from __future__ import annotations

import datetime
import logging
import time
from collections import defaultdict
from typing import TYPE_CHECKING

import requests
from langfuse_sync import config
from requests.auth import HTTPBasicAuth

if TYPE_CHECKING:
    from langfuse_sync.models import (
        LangfuseObservation,
        LangfuseObservationsResponse,
        LangfuseTrace,
        LangfuseTracesResponse,
    )

logger = logging.getLogger(__name__)

_MAX_RETRIES = 10
_PAGE_PAUSE_S = 0.5


def _auth() -> HTTPBasicAuth:
    return HTTPBasicAuth(config.public_key or "", config.secret_key or "")


def _iso(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.UTC)
    else:
        dt = dt.astimezone(datetime.UTC)
    return dt.isoformat().replace("+00:00", "Z")


def _get(url: str, *, params: dict[str, str | int] | None = None) -> requests.Response:
    for attempt in range(_MAX_RETRIES):
        response = requests.get(url, auth=_auth(), params=params, timeout=30)
        if response.status_code != 429:
            response.raise_for_status()
            return response

        retry_after = response.headers.get("Retry-After")
        wait_s = float(retry_after) if retry_after else min(2**attempt, 60)
        logger.warning(
            "Langfuse rate limited (429), retry %s/%s in %.1fs",
            attempt + 1,
            _MAX_RETRIES,
            wait_s,
        )
        time.sleep(wait_s)

    response.raise_for_status()
    return response


def get_traces(
    start: datetime.datetime, end: datetime.datetime, limit: int = 100
) -> list[LangfuseTrace]:
    page = 1
    full_traces: list[LangfuseTrace] = []
    while True:
        if page > 1:
            time.sleep(_PAGE_PAUSE_S)
        response = _get(
            f"{config.langfuse_base_url}/api/public/traces",
            params={
                "page": page,
                "limit": limit,
                "fromTimestamp": _iso(start),
                "toTimestamp": _iso(end),
            },
        )
        payload: LangfuseTracesResponse = response.json()
        traces = payload.get("data") or []
        if not traces:
            break
        full_traces.extend(traces)
        meta = payload.get("meta") or {}
        total_pages = int(meta.get("totalPages") or page)
        if page >= total_pages:
            break
        page += 1
    return full_traces


def get_observations_by_trace_id(
    start: datetime.datetime, end: datetime.datetime, limit: int = 100
) -> dict[str, list[LangfuseObservation]]:
    """Fetch all observations in the date range once, grouped by trace id."""
    page = 1
    by_trace: dict[str, list[LangfuseObservation]] = defaultdict(list)
    while True:
        if page > 1:
            time.sleep(_PAGE_PAUSE_S)
        response = _get(
            f"{config.langfuse_base_url}/api/public/observations",
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
        if page >= total_pages:
            break
        page += 1
    return dict(by_trace)
