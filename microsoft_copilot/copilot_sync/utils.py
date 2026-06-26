from __future__ import annotations

import httpx
from httpx import HTTPStatusError


def should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    return status == 429 or status >= 500
