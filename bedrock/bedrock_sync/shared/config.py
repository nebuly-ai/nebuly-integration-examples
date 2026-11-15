"""Settings both connectors read the same way.

Environment variables hold where the data is and the secrets: AWS account and
region, AWS role, Nebuly key and endpoint, end user handling. Command-line
flags hold how one run behaves: dates, state file, retention, dry run.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from dotenv import load_dotenv

from .errors import ConfigError
from .time_utils import timestamp_str_to_datetime

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

DEFAULT_NEBULY_ENDPOINT = (
    "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction"
)
DEFAULT_CONTENT_RETENTION_SECONDS = 7 * 86400
_TRUE = {"true", "1", "yes", "on"}
_FALSE = {"false", "0", "no", "off", ""}


# Command-line value types ---------------------------------------------------------


def date_arg(value: str) -> datetime:
    """An ISO date or timestamp, read as UTC when it has no offset."""
    try:
        return timestamp_str_to_datetime(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected an ISO date or timestamp") from exc


def nonnegative_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected a nonnegative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("Expected a nonnegative integer")
    return parsed


def positive_int_arg(value: str) -> int:
    parsed = nonnegative_int_arg(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("Expected a positive integer")
    return parsed


def add_common_arguments(
    parser: argparse.ArgumentParser, *, default_state_db: Path
) -> None:
    parser.add_argument(
        "--from-date",
        type=date_arg,
        help="Start of the first run or of a backfill (ISO date or timestamp)",
    )
    parser.add_argument(
        "--to-date", type=date_arg, help="Stop at this time (ISO date or timestamp)"
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=default_state_db,
        help=f"SQLite file with the sync state (default {default_state_db})",
    )
    parser.add_argument(
        "--content-retention-seconds",
        type=nonnegative_int_arg,
        default=DEFAULT_CONTENT_RETENTION_SECONDS,
        help="Keep sent or rejected payloads this long (default 7 days)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the payloads without sending them or saving the state",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Debug logs (never HTTP bodies)"
    )
    parser.add_argument(
        "--no-retry-http-429",
        action="store_false",
        dest="retry_http_429",
        help="Treat a 429 as uncertain (never sent again) instead of retrying it",
    )


# Environment ----------------------------------------------------------------------


def load_env(env: Mapping[str, str] | None) -> Mapping[str, str]:
    """The injected environment, or the process one after loading ``.env``."""
    if env is not None:
        return env
    load_dotenv()
    return os.environ


def env_str(env: Mapping[str, str], name: str, default: str = "") -> str:
    return env.get(name, default).strip()


def env_required(env: Mapping[str, str], name: str) -> str:
    value = env_str(env, name)
    if not value:
        raise ConfigError(f"{name} is required")
    return value


def env_bool(env: Mapping[str, str], name: str) -> bool:
    value = env_str(env, name).lower()
    if value not in _TRUE | _FALSE:
        raise ConfigError(f"{name} must be a boolean (true or false)")
    return value in _TRUE


def _account_id(env: Mapping[str, str]) -> str:
    value = env_required(env, "BEDROCK_ACCOUNT_ID")
    if len(value) != 12 or not value.isascii() or not value.isdigit():
        raise ConfigError("BEDROCK_ACCOUNT_ID must contain 12 ASCII digits")
    return value


def _endpoint(env: Mapping[str, str]) -> str:
    endpoint = env_str(env, "NEBULY_ENDPOINT", DEFAULT_NEBULY_ENDPOINT).rstrip("/")
    try:
        parts = urlsplit(endpoint)
        _ = parts.port  # validates the port too
    except ValueError as exc:
        raise ConfigError("NEBULY_ENDPOINT must be a valid HTTP(S) URL") from exc
    if (
        parts.scheme not in {"http", "https"}
        or any(character.isspace() for character in endpoint)
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.fragment
    ):
        raise ConfigError(
            "NEBULY_ENDPOINT must be an HTTP(S) URL without credentials or fragment"
        )
    return endpoint


def check_date_range(start: datetime | None, end: datetime | None) -> None:
    if start is not None and end is not None and start >= end:
        raise ConfigError("--from-date must be earlier than --to-date")


@dataclass(frozen=True, kw_only=True)
class CommonConfig:
    """Settings of both connectors. Secrets stay out of ``repr``."""

    account_id: str
    region: str
    aws_role_arn: str | None
    aws_external_id: str | None = field(repr=False)
    nebuly_api_key: str = field(repr=False)
    nebuly_endpoint: str
    user_hash_secret: str = field(repr=False)
    send_plain_end_user: bool
    anonymize: bool
    from_date: datetime | None
    to_date: datetime | None
    state_db: Path
    content_retention_seconds: int
    dry_run: bool
    verbose: bool
    retry_http_429: bool

    @staticmethod
    def common_values(
        env: Mapping[str, str], args: argparse.Namespace
    ) -> dict[str, Any]:
        """Validated values for the fields above, from the environment and the
        flags added by ``add_common_arguments``."""
        account_id = _account_id(env)
        region = env_required(env, "BEDROCK_REGION")
        role = env_str(env, "AWS_ROLE_ARN") or None
        external_id = env_str(env, "AWS_EXTERNAL_ID") or None
        if external_id and not role:
            raise ConfigError("AWS_EXTERNAL_ID requires AWS_ROLE_ARN")
        send_plain_end_user = env_bool(env, "SEND_PLAIN_END_USER")
        user_hash_secret = env.get("USER_HASH_SECRET", "")
        if not send_plain_end_user and not user_hash_secret.strip():
            raise ConfigError(
                "USER_HASH_SECRET is required unless SEND_PLAIN_END_USER is true"
            )
        api_key = env_str(env, "NEBULY_API_KEY")
        if not args.dry_run and not api_key:
            raise ConfigError("NEBULY_API_KEY is required outside dry-run")
        check_date_range(args.from_date, args.to_date)
        return {
            "account_id": account_id,
            "region": region,
            "aws_role_arn": role,
            "aws_external_id": external_id,
            "nebuly_api_key": api_key,
            "nebuly_endpoint": _endpoint(env),
            "user_hash_secret": user_hash_secret,
            "send_plain_end_user": send_plain_end_user,
            "anonymize": env_bool(env, "ANONYMIZE"),
            "from_date": args.from_date,
            "to_date": args.to_date,
            "state_db": args.state_db,
            "content_retention_seconds": args.content_retention_seconds,
            "dry_run": args.dry_run,
            "verbose": args.verbose,
            "retry_http_429": args.retry_http_429,
        }
