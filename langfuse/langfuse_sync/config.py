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
    langfuse_public_key: str
    langfuse_secret_key: str
    langfuse_base_url: str
    nebuly_api_key: str
    nebuly_endpoint: str
    anonymize: bool
    settle_lag_seconds: int
    from_date: datetime | None
    to_date: datetime | None
    cache_dir: Path
    dry_run: bool
    verbose: bool
    force: bool
    max_concurrency: int = 5

    @classmethod
    def from_env_and_args(cls, argv: list[str] | None = None) -> Config:
        load_dotenv()
        parser = argparse.ArgumentParser(
            description="Incrementally sync Langfuse traces to Nebuly"
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
            "--yes",
            "--force",
            action="store_true",
            dest="force",
            help="Confirm gap-creating runs without prompting",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=None,
            help="Max in-flight requests per service (env MAX_CONCURRENCY, default 5)",
        )
        args = parser.parse_args(argv)

        public_key = os.environ.get("LANGFUSE_PUBLIC_KEY")
        secret_key = os.environ.get("LANGFUSE_SECRET_KEY")
        nebuly_api_key = os.environ.get("NEBULY_API_KEY")

        missing = [
            name
            for name, val in [
                ("LANGFUSE_PUBLIC_KEY", public_key),
                ("LANGFUSE_SECRET_KEY", secret_key),
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

        max_concurrency = (
            args.concurrency
            if args.concurrency is not None
            else int(os.environ.get("MAX_CONCURRENCY", "5"))
        )
        if max_concurrency < 1:
            raise RuntimeError("MAX_CONCURRENCY / --concurrency must be at least 1")

        return cls(
            langfuse_public_key=cast(str, public_key),
            langfuse_secret_key=cast(str, secret_key),
            langfuse_base_url=os.environ.get(
                "LANGFUSE_BASE_URL", "https://cloud.langfuse.com"
            ).rstrip("/"),
            nebuly_api_key=cast(str, nebuly_api_key),
            nebuly_endpoint=os.environ.get(
                "NEBULY_ENDPOINT",
                "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction",
            ).rstrip("/"),
            anonymize=_parse_bool(os.environ.get("ANONYMIZE", "false")),
            settle_lag_seconds=int(
                os.environ.get("LANGFUSE_SETTLE_LAG_SECONDS", "900")
            ),
            from_date=from_date,
            to_date=to_date,
            cache_dir=args.cache_dir,
            dry_run=args.dry_run,
            verbose=args.verbose,
            force=args.force,
            max_concurrency=max_concurrency,
        )

    def run_until(self) -> datetime:
        if self.to_date is not None:
            return self.to_date
        return datetime.now(UTC) - timedelta(seconds=self.settle_lag_seconds)
