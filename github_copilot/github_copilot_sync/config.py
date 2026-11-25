from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from dotenv import load_dotenv

from .entra import EntraDirectory, fetch_entra_directory, load_entra_extract


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


DEFAULT_SOURCES = ("rest", "s3", "azure_blob")


def _parse_sources(value: str) -> tuple[str, ...]:
    parts = tuple(part.strip() for part in value.split(",") if part.strip())
    if not parts:
        raise RuntimeError("At least one source is required")
    unknown = [part for part in parts if part not in DEFAULT_SOURCES]
    if unknown:
        raise RuntimeError(
            "Unknown sources: " + ", ".join(unknown) + ". Expected rest, s3, azure_blob"
        )
    return parts


@dataclass(frozen=True)
class Config:
    github_token: str
    github_enterprise: str
    github_api_url: str
    github_api_version: str
    nebuly_api_key: str
    nebuly_endpoint: str
    sources: tuple[str, ...]
    pair_wait_minutes: int
    conversation_gap_hours: int
    interaction_idle_minutes: int
    rest_overlap_seconds: int
    resolve_users: bool
    s3_bucket: str
    s3_prefix: str
    s3_endpoint_url: str | None
    azure_storage_connection_string: str | None
    azure_blob_account_url: str | None
    azure_blob_container: str
    azure_blob_prefix: str
    azure_blob_sas_token: str | None
    anonymize: bool
    from_date: datetime | None
    to_date: datetime | None
    cache_dir: Path
    dry_run: bool
    verbose: bool
    entra_directory: EntraDirectory | None = None
    csv_export: bool = False
    csv_export_path: Path = Path("./interactions.csv")

    @property
    def cache_key(self) -> str:
        destination = (
            f"csv|{self.csv_export_path.resolve()}"
            if self.csv_export
            else f"nebuly|{self.nebuly_endpoint}|{self.nebuly_api_key}"
        )
        raw = (
            f"{self.github_api_url}|{self.github_enterprise}|"
            f"{','.join(self.sources)}|{destination}"
        )
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @property
    def store_path(self) -> Path:
        return self.cache_dir / self.cache_key / "copilot_store.db"

    @classmethod
    def from_env_and_args(cls, argv: list[str] | None = None) -> Config:
        load_dotenv()
        parser = argparse.ArgumentParser(
            description="Sync GitHub Copilot usage records to Nebuly"
        )
        parser.add_argument(
            "--from-date", type=str, default=None, help="ISO backfill start date"
        )
        parser.add_argument(
            "--to-date",
            type=str,
            default=None,
            help="ISO end date filter (exclusive upper bound)",
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
            "--sources",
            default=",".join(DEFAULT_SOURCES),
            help="Comma-separated sources: rest, s3, azure_blob",
        )
        args = parser.parse_args(argv)

        github_token = os.environ.get("GITHUB_TOKEN")
        github_enterprise = os.environ.get("GITHUB_ENTERPRISE")
        nebuly_api_key = os.environ.get("NEBULY_API_KEY")
        csv_export = _parse_bool(os.environ.get("CSV_EXPORT", "false"))

        required = [
            ("GITHUB_TOKEN", github_token),
            ("GITHUB_ENTERPRISE", github_enterprise),
        ]
        if not csv_export:
            required.insert(0, ("NEBULY_API_KEY", nebuly_api_key))
        missing = [name for name, val in required if not val]
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

        from_date = (
            timestamp_str_to_datetime(args.from_date) if args.from_date else None
        )
        to_date = timestamp_str_to_datetime(args.to_date) if args.to_date else None
        entra_extract_path = os.environ.get("ENTRA_EXTRACT_PATH", "").strip()
        entra_fetch = _parse_bool(os.environ.get("ENTRA_FETCH", "false"))
        if entra_fetch and entra_extract_path:
            raise RuntimeError("Set ENTRA_FETCH or ENTRA_EXTRACT_PATH, not both")
        if entra_fetch:
            entra_directory = fetch_entra_directory()
        elif entra_extract_path:
            entra_directory = load_entra_extract(Path(entra_extract_path))
        else:
            entra_directory = None

        return cls(
            github_token=cast(str, github_token),
            github_enterprise=cast(str, github_enterprise),
            github_api_url=os.environ.get(
                "GITHUB_API_URL", "https://api.github.com"
            ).rstrip("/"),
            github_api_version=os.environ.get("GITHUB_API_VERSION", "2026-03-10"),
            nebuly_api_key=nebuly_api_key or "",
            nebuly_endpoint=os.environ.get(
                "NEBULY_ENDPOINT",
                "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction",
            ).rstrip("/"),
            sources=_parse_sources(args.sources),
            pair_wait_minutes=int(os.environ.get("PAIR_WAIT_MINUTES", "15")),
            conversation_gap_hours=int(os.environ.get("CONVERSATION_GAP_HOURS", "8")),
            interaction_idle_minutes=int(
                os.environ.get("INTERACTION_IDLE_MINUTES", "30")
            ),
            rest_overlap_seconds=int(os.environ.get("REST_OVERLAP_SECONDS", "60")),
            resolve_users=_parse_bool(os.environ.get("RESOLVE_USERS", "true")),
            s3_bucket=os.environ.get("S3_BUCKET", ""),
            s3_prefix=os.environ.get("S3_PREFIX", ""),
            s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
            azure_storage_connection_string=os.environ.get(
                "AZURE_STORAGE_CONNECTION_STRING"
            )
            or None,
            azure_blob_account_url=os.environ.get("AZURE_BLOB_ACCOUNT_URL") or None,
            azure_blob_container=os.environ.get("AZURE_BLOB_CONTAINER", ""),
            azure_blob_prefix=os.environ.get("AZURE_BLOB_PREFIX", ""),
            azure_blob_sas_token=os.environ.get("AZURE_BLOB_SAS_TOKEN") or None,
            anonymize=_parse_bool(os.environ.get("ANONYMIZE", "false")),
            from_date=from_date,
            to_date=to_date,
            cache_dir=args.cache_dir,
            dry_run=args.dry_run,
            verbose=args.verbose,
            entra_directory=entra_directory,
            csv_export=csv_export,
            csv_export_path=Path(
                os.environ.get("CSV_EXPORT_PATH", "./interactions.csv")
            ),
        )
