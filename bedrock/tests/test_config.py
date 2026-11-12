from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from bedrock_sync.config import Config


@pytest.fixture
def base_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("BEDROCK_S3_BUCKET", "logs-bucket")
    monkeypatch.setenv("BEDROCK_ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("BEDROCK_REGION", "eu-north-1")
    monkeypatch.setenv("USER_HASH_SECRET", "secret")
    for name in (
        "BEDROCK_S3_PREFIX",
        "AWS_ROLE_ARN",
        "AWS_EXTERNAL_ID",
        "SEND_PLAIN_END_USER",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


@patch("bedrock_sync.config.load_dotenv")
def test_missing_required_vars_raises(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    base_env.delenv("BEDROCK_S3_BUCKET")
    base_env.delenv("BEDROCK_REGION")

    with pytest.raises(RuntimeError, match="BEDROCK_S3_BUCKET, BEDROCK_REGION"):
        Config.from_env_and_args([])


@patch("bedrock_sync.config.load_dotenv")
def test_missing_user_hash_secret_raises(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    base_env.delenv("USER_HASH_SECRET")

    with pytest.raises(RuntimeError, match="USER_HASH_SECRET is required"):
        Config.from_env_and_args([])


@patch("bedrock_sync.config.load_dotenv")
def test_external_id_without_role_raises(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    base_env.setenv("AWS_EXTERNAL_ID", "ext")

    with pytest.raises(RuntimeError, match="AWS_ROLE_ARN is missing"):
        Config.from_env_and_args([])


@patch("bedrock_sync.config.load_dotenv")
def test_from_date_after_to_date_raises(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    with pytest.raises(RuntimeError, match="earlier than"):
        Config.from_env_and_args(
            ["--from-date", "2026-11-02", "--to-date", "2026-11-01"]
        )


@patch("bedrock_sync.config.load_dotenv")
def test_logs_root_prefix_without_custom_prefix(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    config = Config.from_env_and_args([])

    assert config.logs_root_prefix() == (
        "AWSLogs/123456789012/BedrockModelInvocationLogs/eu-north-1"
    )


@patch("bedrock_sync.config.load_dotenv")
def test_logs_root_prefix_strips_slashes(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    base_env.setenv("BEDROCK_S3_PREFIX", "/bedrock/logs/")

    config = Config.from_env_and_args([])

    assert config.logs_root_prefix() == (
        "bedrock/logs/AWSLogs/123456789012/BedrockModelInvocationLogs/eu-north-1"
    )


@patch("bedrock_sync.config.load_dotenv")
def test_run_until_uses_to_date(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    config = Config.from_env_and_args(["--to-date", "2026-11-01T10:00:00Z"])

    assert config.run_until().isoformat() == "2026-11-01T10:00:00+00:00"


@patch("bedrock_sync.config.load_dotenv")
def test_run_until_never_goes_past_the_settle_lag(
    load_dotenv: object, base_env: pytest.MonkeyPatch
) -> None:
    config = Config.from_env_and_args(["--to-date", "2030-01-01T00:00:00Z"])
    now = datetime(2026, 11, 12, 10, tzinfo=UTC)

    assert config.run_until(now) == now - timedelta(seconds=300)
