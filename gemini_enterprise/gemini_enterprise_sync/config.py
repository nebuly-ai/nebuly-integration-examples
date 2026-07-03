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
    nebuly_api_key: str
    nebuly_endpoint: str
    gcp_project_id: str
    gcp_location: str
    gcp_collection: str
    gcp_engine_id: str
    settle_lag_seconds: int
    log_batch_size: int
    trace_max_workers: int
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
            description="Sync Gemini Enterprise logs and traces to Nebuly"
        )
        parser.add_argument(
            "--from-date", type=str, default=None, help="ISO backfill start date"
        )
        parser.add_argument(
            "--to-date", type=str, default=None, help="ISO end date filter"
        )
        parser.add_argument("--cache-dir", type=Path, default=Path("./.cache"))
        parser.add_argument(
            "--dry-run", action="store_true", help="Build payloads without POSTing"
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable debug logging (includes HTTP request traces)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=None,
            help="Max log records per batch (overrides GCP_LOG_BATCH_SIZE)",
        )
        parser.add_argument(
            "--trace-workers",
            type=int,
            default=None,
            help="Parallel get_trace workers (overrides GCP_TRACE_MAX_WORKERS)",
        )
        args = parser.parse_args(argv)

        nebuly_api_key = os.environ.get("NEBULY_API_KEY")
        gcp_project_id = os.environ.get("GCP_PROJECT_ID")
        gcp_location = os.environ.get("GCP_LOCATION")
        gcp_engine_id = os.environ.get("GCP_ENGINE_ID")

        missing = [
            name
            for name, val in [
                ("NEBULY_API_KEY", nebuly_api_key),
                ("GCP_PROJECT_ID", gcp_project_id),
                ("GCP_LOCATION", gcp_location),
                ("GCP_ENGINE_ID", gcp_engine_id),
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
            nebuly_api_key=cast(str, nebuly_api_key),
            nebuly_endpoint=os.environ.get(
                "NEBULY_ENDPOINT",
                "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction",
            ).rstrip("/"),
            gcp_project_id=cast(str, gcp_project_id),
            gcp_location=cast(str, gcp_location),
            gcp_collection=os.environ.get("GCP_COLLECTION", "default_collection"),
            gcp_engine_id=cast(str, gcp_engine_id),
            settle_lag_seconds=int(os.environ.get("GCP_SETTLE_LAG_SECONDS", "60")),
            log_batch_size=args.batch_size
            or int(os.environ.get("GCP_LOG_BATCH_SIZE", "500")),
            trace_max_workers=args.trace_workers
            or int(os.environ.get("GCP_TRACE_MAX_WORKERS", "16")),
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
        return datetime.now(UTC) - timedelta(seconds=self.settle_lag_seconds)
