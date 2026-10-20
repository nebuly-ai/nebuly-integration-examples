from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx

from .graph import fetch_token, list_users

if TYPE_CHECKING:
    from pathlib import Path

    from .config import Config


def write_extract(path: Path, users: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"users": users}
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def run_export(config: Config, client: httpx.Client | None = None) -> int:
    owns_client = client is None
    http = client or httpx.Client(timeout=60.0)
    try:
        token = fetch_token(
            http,
            tenant_id=config.azure_tenant_id,
            client_id=config.azure_client_id,
            client_secret=config.azure_client_secret,
            login_base_url=config.login_base_url,
        )
        users = list_users(http, token, graph_base_url=config.graph_base_url)
    finally:
        if owns_client:
            http.close()

    if config.dry_run:
        return len(users)

    write_extract(config.output, users)
    return len(users)
