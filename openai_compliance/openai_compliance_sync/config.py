from __future__ import annotations

import argparse
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


DEFAULT_SOURCES = ("chats", "codex")


def _parse_sources(value: str) -> tuple[str, ...]:
    parts = tuple(part.strip() for part in value.split(",") if part.strip())
    if not parts:
        raise RuntimeError("At least one source is required")
    unknown = [part for part in parts if part not in DEFAULT_SOURCES]
    if unknown:
        raise RuntimeError(
            "Unknown sources: " + ", ".join(unknown) + ". Expected chats, codex"
        )
    return parts


@dataclass(frozen=True)
class Config:
    nebuly_api_key: str
    nebuly_endpoint: str
    compliance_api_key: str
    compliance_base_url: str
    compliance_workspace_id: str
    compliance_max_requests_per_minute: int
    anonymize: bool
    from_date: datetime | None
    to_date: datetime | None
    cache_dir: Path
    dry_run: bool
    verbose: bool
    sources: tuple[str, ...] = DEFAULT_SOURCES
    interaction_idle_minutes: int = 30
    codex_cost_wait_hours: int = 4
    entra_directory: EntraDirectory | None = None

    @classmethod
    def from_env_and_args(cls, argv: list[str] | None = None) -> Config:
        load_dotenv()
        parser = argparse.ArgumentParser(
            description="Sync OpenAI Compliance Logs Platform data to Nebuly"
        )
        parser.add_argument(
            "--from-date", type=str, default=None, help="ISO backfill start date"
        )
        parser.add_argument(
            "--to-date",
            type=str,
            default=None,
            help="ISO end date filter (maps to listing before=)",
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
            help="Comma-separated sources: chats, codex",
        )
        args = parser.parse_args(argv)

        nebuly_api_key = os.environ.get("NEBULY_API_KEY")
        compliance_api_key = os.environ.get("COMPLIANCE_API_KEY")
        compliance_workspace_id = os.environ.get("COMPLIANCE_WORKSPACE_ID")

        missing = [
            name
            for name, val in [
                ("NEBULY_API_KEY", nebuly_api_key),
                ("COMPLIANCE_API_KEY", compliance_api_key),
                ("COMPLIANCE_WORKSPACE_ID", compliance_workspace_id),
            ]
            if not val
        ]
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
            nebuly_api_key=cast(str, nebuly_api_key),
            nebuly_endpoint=os.environ.get(
                "NEBULY_ENDPOINT",
                "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction",
            ).rstrip("/"),
            compliance_api_key=cast(str, compliance_api_key),
            compliance_base_url=os.environ.get(
                "COMPLIANCE_BASE_URL", "https://api.chatgpt.com/v1/compliance"
            ).rstrip("/"),
            compliance_workspace_id=cast(str, compliance_workspace_id),
            compliance_max_requests_per_minute=int(
                os.environ.get("COMPLIANCE_MAX_REQUESTS_PER_MINUTE", "100")
            ),
            anonymize=_parse_bool(os.environ.get("ANONYMIZE", "false")),
            from_date=from_date,
            to_date=to_date,
            cache_dir=args.cache_dir,
            dry_run=args.dry_run,
            verbose=args.verbose,
            sources=_parse_sources(args.sources),
            interaction_idle_minutes=int(
                os.environ.get("INTERACTION_IDLE_MINUTES", "30")
            ),
            codex_cost_wait_hours=int(os.environ.get("CODEX_COST_WAIT_HOURS", "4")),
            entra_directory=entra_directory,
        )
