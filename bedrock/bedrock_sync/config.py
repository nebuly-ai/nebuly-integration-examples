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


def _normalize_prefix(value: str) -> str:
    """Strip slashes so the prefix can be joined with "/" safely."""
    return value.strip().strip("/")


@dataclass(frozen=True)
class Config:
    nebuly_api_key: str
    nebuly_endpoint: str
    s3_bucket: str
    s3_prefix: str
    aws_account_id: str
    bedrock_region: str
    aws_role_arn: str | None
    aws_external_id: str | None
    settle_lag_seconds: int
    conversation_chain_ttl_days: int
    anonymize: bool
    send_plain_end_user: bool
    user_hash_secret: str
    from_date: datetime | None
    to_date: datetime | None
    cache_dir: Path
    dry_run: bool
    verbose: bool
    force: bool

    @classmethod
    def from_env_and_args(cls, argv: list[str] | None = None) -> Config:
        load_dotenv()
        parser = argparse.ArgumentParser(
            description="Sync AWS Bedrock model invocation logs (S3) to Nebuly"
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
        args = parser.parse_args(argv)

        nebuly_api_key = os.environ.get("NEBULY_API_KEY")
        s3_bucket = os.environ.get("BEDROCK_S3_BUCKET")
        aws_account_id = os.environ.get("BEDROCK_ACCOUNT_ID")
        bedrock_region = os.environ.get("BEDROCK_REGION")

        missing = [
            name
            for name, val in [
                ("NEBULY_API_KEY", nebuly_api_key),
                ("BEDROCK_S3_BUCKET", s3_bucket),
                ("BEDROCK_ACCOUNT_ID", aws_account_id),
                ("BEDROCK_REGION", bedrock_region),
            ]
            if not val
        ]
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

        aws_role_arn = os.environ.get("AWS_ROLE_ARN") or None
        aws_external_id = os.environ.get("AWS_EXTERNAL_ID") or None
        if aws_external_id and not aws_role_arn:
            raise RuntimeError("AWS_EXTERNAL_ID is set but AWS_ROLE_ARN is missing")

        send_plain_end_user = _parse_bool(
            os.environ.get("SEND_PLAIN_END_USER", "false")
        )
        user_hash_secret = os.environ.get("USER_HASH_SECRET", "")
        if not send_plain_end_user and not user_hash_secret:
            raise RuntimeError(
                "USER_HASH_SECRET is required when SEND_PLAIN_END_USER is false"
            )

        from_date = (
            timestamp_str_to_datetime(args.from_date) if args.from_date else None
        )
        to_date = timestamp_str_to_datetime(args.to_date) if args.to_date else None
        if from_date is not None and to_date is not None and from_date >= to_date:
            raise RuntimeError("--from-date must be earlier than --to-date")

        return cls(
            nebuly_api_key=cast(str, nebuly_api_key),
            nebuly_endpoint=os.environ.get(
                "NEBULY_ENDPOINT",
                "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction",
            ).rstrip("/"),
            s3_bucket=cast(str, s3_bucket),
            s3_prefix=_normalize_prefix(os.environ.get("BEDROCK_S3_PREFIX", "")),
            aws_account_id=cast(str, aws_account_id),
            bedrock_region=cast(str, bedrock_region),
            aws_role_arn=aws_role_arn,
            aws_external_id=aws_external_id,
            settle_lag_seconds=int(os.environ.get("BEDROCK_SETTLE_LAG_SECONDS", "300")),
            conversation_chain_ttl_days=int(
                os.environ.get("CONVERSATION_CHAIN_TTL_DAYS", "30")
            ),
            anonymize=_parse_bool(os.environ.get("ANONYMIZE", "false")),
            send_plain_end_user=send_plain_end_user,
            user_hash_secret=user_hash_secret,
            from_date=from_date,
            to_date=to_date,
            cache_dir=args.cache_dir,
            dry_run=args.dry_run,
            verbose=args.verbose,
            force=args.force,
        )

    def logs_root_prefix(self) -> str:
        """S3 key prefix under which Bedrock writes the hourly log folders:
        [<prefix>/]AWSLogs/<account>/BedrockModelInvocationLogs/<region>"""
        parts = [
            self.s3_prefix,
            "AWSLogs",
            self.aws_account_id,
            "BedrockModelInvocationLogs",
            self.bedrock_region,
        ]
        return "/".join(p for p in parts if p)

    def run_until(self, now: datetime | None = None) -> datetime:
        """End of the window: ``--to-date``, but never later than the settle
        lag, so files still being delivered are left to the next run."""
        settled = (now or datetime.now(UTC)) - timedelta(
            seconds=self.settle_lag_seconds
        )
        return settled if self.to_date is None else min(self.to_date, settled)
