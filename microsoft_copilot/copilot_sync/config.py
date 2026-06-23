from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from dotenv import load_dotenv


def timestamp_str_to_datetime(timestamp: str) -> datetime:
    if not timestamp:
        raise ValueError("timestamp is required")
    ts = timestamp.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt


def datetime_to_timestamp_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    azure_tenant_id: str
    azure_client_id: str
    azure_client_secret: str
    copilot_sku: str
    graph_max_requests_per_minute: int
    ingestion_lag_minutes: int
    nebuly_api_key: str
    nebuly_endpoint: str
    anonymize: bool
    from_date: datetime | None
    to_date: datetime | None
    cache_dir: Path
    dry_run: bool
    verbose: bool

    @classmethod
    def from_env_and_args(cls, argv: list[str] | None = None) -> Config:
        load_dotenv()
        parser = argparse.ArgumentParser(
            description="Sync Microsoft Copilot Enterprise interactions to Nebuly",
        )
        parser.add_argument(
            "--from-date",
            type=str,
            default=None,
            help="ISO backfill start date",
        )
        parser.add_argument(
            "--to-date",
            type=str,
            default=None,
            help="ISO end date filter",
        )
        parser.add_argument("--cache-dir", type=Path, default=Path("./.cache"))
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Build payloads without POSTing",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable debug logging (includes HTTP request traces)",
        )
        args = parser.parse_args(argv)

        azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
        azure_client_id = os.environ.get("AZURE_CLIENT_ID")
        azure_client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        nebuly_api_key = os.environ.get("NEBULY_API_KEY")

        missing = [
            name
            for name, val in [
                ("AZURE_TENANT_ID", azure_tenant_id),
                ("AZURE_CLIENT_ID", azure_client_id),
                ("AZURE_CLIENT_SECRET", azure_client_secret),
                ("NEBULY_API_KEY", nebuly_api_key),
            ]
            if not val
        ]
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

        from_date = (
            timestamp_str_to_datetime(args.from_date) if args.from_date else None
        )
        to_date = timestamp_str_to_datetime(args.to_date) if args.to_date else None

        return cls(
            azure_tenant_id=cast("str", azure_tenant_id),
            azure_client_id=cast("str", azure_client_id),
            azure_client_secret=cast("str", azure_client_secret),
            copilot_sku=os.environ.get(
                "COPILOT_SKU",
                "639dec6b-bb19-468b-871c-c5c441c4b0cb",
            ),
            graph_max_requests_per_minute=int(
                os.environ.get("GRAPH_MAX_REQUESTS_PER_MINUTE", "600"),
            ),
            ingestion_lag_minutes=int(os.environ.get("INGESTION_LAG_MINUTES", "15")),
            nebuly_api_key=cast("str", nebuly_api_key),
            nebuly_endpoint=os.environ.get(
                "NEBULY_ENDPOINT",
                "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction",
            ).rstrip("/"),
            anonymize=_parse_bool(os.environ.get("ANONYMIZE", "false")),
            from_date=from_date,
            to_date=to_date,
            cache_dir=args.cache_dir,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

    def run_until(self) -> datetime:
        if self.to_date is not None:
            return self.to_date
        lag = timedelta(minutes=self.ingestion_lag_minutes)
        return datetime.now(UTC) - lag
