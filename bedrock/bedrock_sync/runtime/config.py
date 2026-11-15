"""Settings of the runtime connector (Bedrock model invocation logs on S3).

The settings both connectors share are in ``shared/config.py``. This adds
where the logs are (``BEDROCK_S3_BUCKET``, ``BEDROCK_S3_PREFIX``) and how
interactions close.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from bedrock_sync.shared.config import (
    CommonConfig,
    add_common_arguments,
    env_required,
    env_str,
    load_env,
    nonnegative_int_arg,
    positive_int_arg,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

DEFAULT_STATE_DB = Path(".cache/runtime/state.db")


@dataclass(frozen=True, kw_only=True)
class Config(CommonConfig):
    s3_bucket: str
    s3_prefix: str
    settle_lag_seconds: int = 300
    interaction_idle_minutes: int = 30
    delivery_margin_seconds: int = 300
    force: bool = False

    @classmethod
    def from_env_and_args(
        cls,
        argv: list[str] | None = None,
        *,
        env: Mapping[str, str] | None = None,
    ) -> Config:
        parser = argparse.ArgumentParser(
            description="Send Bedrock model invocation logs (S3) to Nebuly"
        )
        add_common_arguments(parser, default_state_db=DEFAULT_STATE_DB)
        parser.add_argument(
            "--settle-lag-seconds",
            type=nonnegative_int_arg,
            default=300,
            help="Leave files newer than this to the next run (default 300)",
        )
        parser.add_argument(
            "--interaction-idle-minutes",
            type=positive_int_arg,
            default=30,
            help="Close an interaction after this long without calls (default 30)",
        )
        parser.add_argument(
            "--delivery-margin-seconds",
            type=nonnegative_int_arg,
            default=300,
            help="Wait this long for late S3 files before closing (default 300)",
        )
        parser.add_argument(
            "--yes",
            "--force",
            action="store_true",
            dest="force",
            help="Confirm a run that leaves a gap in the synced range",
        )
        args = parser.parse_args(argv)
        values = load_env(env)
        return cls(
            **CommonConfig.common_values(values, args),
            s3_bucket=env_required(values, "BEDROCK_S3_BUCKET"),
            s3_prefix=env_str(values, "BEDROCK_S3_PREFIX").strip("/"),
            settle_lag_seconds=args.settle_lag_seconds,
            interaction_idle_minutes=args.interaction_idle_minutes,
            delivery_margin_seconds=args.delivery_margin_seconds,
            force=args.force,
        )

    @property
    def log_source(self) -> str:
        """Where the logs are read from; the state file is bound to it."""
        return f"s3://{self.s3_bucket}/{self.logs_root_prefix()}"

    def logs_root_prefix(self) -> str:
        """S3 key prefix under which Bedrock writes the hourly log folders:
        [<prefix>/]AWSLogs/<account>/BedrockModelInvocationLogs/<region>"""
        parts = [
            self.s3_prefix,
            "AWSLogs",
            self.account_id,
            "BedrockModelInvocationLogs",
            self.region,
        ]
        return "/".join(p for p in parts if p)

    def run_until(self, now: datetime | None = None) -> datetime:
        """End of the window: ``--to-date``, but never later than the settle
        lag, so files still being delivered are left to the next run."""
        settled = (now or datetime.now(UTC)) - timedelta(
            seconds=self.settle_lag_seconds
        )
        return settled if self.to_date is None else min(self.to_date, settled)
