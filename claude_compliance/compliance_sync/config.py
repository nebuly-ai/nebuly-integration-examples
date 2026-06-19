from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from dotenv import load_dotenv


def timestamp_str_to_datetime(timestamp: str) -> datetime:
    if not timestamp:
        raise ValueError("timestamp is required")
    ts = timestamp.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def datetime_to_timestamp_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    nebuly_api_key: str
    nebuly_endpoint: str
    compliance_api_key: str
    compliance_base_url: str
    organization_uuid: str
    compliance_max_requests_per_minute: int
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
            description="Sync Claude Compliance data to Nebuly"
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
        args = parser.parse_args(argv)

        nebuly_api_key = os.environ.get("NEBULY_API_KEY")
        compliance_api_key = os.environ.get("COMPLIANCE_API_KEY")
        compliance_base_url = os.environ.get("COMPLIANCE_BASE_URL")
        organization_uuid = os.environ.get("ORGANIZATION_UUID")

        missing = [
            name
            for name, val in [
                ("NEBULY_API_KEY", nebuly_api_key),
                ("COMPLIANCE_API_KEY", compliance_api_key),
                ("COMPLIANCE_BASE_URL", compliance_base_url),
                ("ORGANIZATION_UUID", organization_uuid),
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
            ),
            compliance_api_key=cast(str, compliance_api_key),
            compliance_base_url=cast(str, compliance_base_url).rstrip("/"),
            organization_uuid=cast(str, organization_uuid),
            compliance_max_requests_per_minute=int(
                os.environ.get("COMPLIANCE_MAX_REQUESTS_PER_MINUTE", "600")
            ),
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
        return datetime.now(timezone.utc)
