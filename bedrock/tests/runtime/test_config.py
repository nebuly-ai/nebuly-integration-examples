"""Runtime settings; the shared ones are tested in tests/shared/test_config.py."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from bedrock_sync.runtime.config import Config
from bedrock_sync.shared.errors import ConfigError


@pytest.fixture
def env() -> dict[str, str]:
    return {
        "NEBULY_API_KEY": "key",
        "BEDROCK_S3_BUCKET": "logs-bucket",
        "BEDROCK_ACCOUNT_ID": "123456789012",
        "BEDROCK_REGION": "eu-north-1",
        "USER_HASH_SECRET": "secret",
    }


def test_defaults(env: dict[str, str]) -> None:
    config = Config.from_env_and_args([], env=env)
    assert config.state_db == Path(".cache/runtime/state.db")
    assert (
        config.settle_lag_seconds,
        config.interaction_idle_minutes,
        config.delivery_margin_seconds,
        config.content_retention_seconds,
    ) == (300, 30, 300, 604800)
    assert config.log_source == (
        "s3://logs-bucket/AWSLogs/123456789012/BedrockModelInvocationLogs/eu-north-1"
    )


def test_bucket_is_required(env: dict[str, str]) -> None:
    del env["BEDROCK_S3_BUCKET"]
    with pytest.raises(ConfigError, match="BEDROCK_S3_BUCKET"):
        Config.from_env_and_args([], env=env)


def test_prefix_slashes_are_stripped(env: dict[str, str]) -> None:
    env["BEDROCK_S3_PREFIX"] = "/bedrock/logs/"
    assert Config.from_env_and_args([], env=env).logs_root_prefix() == (
        "bedrock/logs/AWSLogs/123456789012/BedrockModelInvocationLogs/eu-north-1"
    )


def test_interaction_flags(env: dict[str, str]) -> None:
    config = Config.from_env_and_args(
        [
            "--interaction-idle-minutes",
            "10",
            "--delivery-margin-seconds",
            "0",
            "--settle-lag-seconds",
            "60",
            "--yes",
        ],
        env=env,
    )
    assert (config.interaction_idle_minutes, config.delivery_margin_seconds) == (10, 0)
    assert config.settle_lag_seconds == 60
    assert config.force
    with pytest.raises(SystemExit):
        Config.from_env_and_args(["--interaction-idle-minutes", "0"], env=env)


def test_run_until_uses_to_date(env: dict[str, str]) -> None:
    config = Config.from_env_and_args(["--to-date", "2026-11-01T10:00:00Z"], env=env)
    assert config.run_until().isoformat() == "2026-11-01T10:00:00+00:00"


def test_run_until_never_goes_past_the_settle_lag(env: dict[str, str]) -> None:
    config = Config.from_env_and_args(["--to-date", "2030-01-01T00:00:00Z"], env=env)
    now = datetime(2026, 11, 12, 10, tzinfo=UTC)
    assert config.run_until(now) == now - timedelta(seconds=300)
