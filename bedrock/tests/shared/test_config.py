"""Settings both connectors read the same way."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from bedrock_sync.shared.config import CommonConfig, add_common_arguments, load_env
from bedrock_sync.shared.errors import ConfigError


@dataclass(frozen=True, kw_only=True)
class _Config(CommonConfig):
    pass


def read(argv: list[str], env: dict[str, str]) -> _Config:
    parser = argparse.ArgumentParser()
    add_common_arguments(parser, default_state_db=Path("state.db"))
    return _Config(**CommonConfig.common_values(env, parser.parse_args(argv)))


@pytest.fixture
def env() -> dict[str, str]:
    return {
        "BEDROCK_ACCOUNT_ID": "123456789012",
        "BEDROCK_REGION": "eu-north-1",
        "USER_HASH_SECRET": "test-hash-secret",
        "NEBULY_API_KEY": "test-api-key",
    }


def test_defaults(env: dict[str, str]) -> None:
    config = read([], env)
    assert config.state_db == Path("state.db")
    assert config.content_retention_seconds == 7 * 86400
    assert config.history_retention_seconds == 30 * 86400
    assert not (config.dry_run)
    assert not (config.anonymize)
    assert not (config.pseudonymize_end_user)
    assert config.nebuly_endpoint.startswith("https://backend.nebuly.com/")


def test_history_retention_is_not_shorter_than_content_retention(
    env: dict[str, str],
) -> None:
    config = read(["--history-retention-seconds", "864000"], env)
    assert config.history_retention_seconds == 864000
    with pytest.raises(ConfigError, match="history-retention"):
        read(["--history-retention-seconds", "3600"], env)


def test_injected_environment_does_not_read_dotenv(env: dict[str, str]) -> None:
    with patch("bedrock_sync.shared.config.load_dotenv") as dotenv:
        assert load_env(env) is env
    dotenv.assert_not_called()


def test_dates_are_utc_and_ordered(env: dict[str, str]) -> None:
    config = read(
        ["--from-date", "2026-11-13T01:00:00+01:00", "--to-date", "2026-11-14"], env
    )
    assert config.from_date == datetime(2026, 11, 13, tzinfo=UTC)
    assert config.to_date == datetime(2026, 11, 14, tzinfo=UTC)
    with pytest.raises(ConfigError, match="earlier than"):
        read(["--from-date", "2026-11-14", "--to-date", "2026-11-14"], env)


def test_retry_given_up_takes_one_key_instead_of_a_read_window(
    env: dict[str, str],
) -> None:
    option = ["--retry-given-up", "opaque-key"]
    assert read(option, env).retry_given_up == "opaque-key"
    for args in (
        ["--from-date", "2026-11-13"],
        ["--to-date", "2026-11-14"],
        ["--dry-run"],
    ):
        with pytest.raises(ConfigError, match="cannot be combined"):
            read([*option, *args], env)


@pytest.mark.parametrize(
    "argv",
    [
        ["--from-date", "yesterday"],
        ["--content-retention-seconds", "-1"],
        ["--content-retention-seconds", "no"],
    ],
)
def test_invalid_flags_exit_with_usage(env: dict[str, str], argv: list[str]) -> None:
    with pytest.raises(SystemExit) as error:
        read(argv, env)
    assert error.value.code == 2


@pytest.mark.parametrize("name", ["BEDROCK_ACCOUNT_ID", "BEDROCK_REGION"])
def test_source_is_required(env: dict[str, str], name: str) -> None:
    del env[name]
    with pytest.raises(ConfigError, match=name):
        read([], env)


@pytest.mark.parametrize("value", ["123", chr(0xFF11) * 12, "abcdefghijkl"])
def test_account_has_twelve_digits(env: dict[str, str], value: str) -> None:
    env["BEDROCK_ACCOUNT_ID"] = value
    with pytest.raises(ConfigError, match="12 ASCII digits"):
        read([], env)


def test_api_key_is_optional_only_in_dry_run(env: dict[str, str]) -> None:
    del env["NEBULY_API_KEY"]
    assert read(["--dry-run"], env).nebuly_api_key == ""
    with pytest.raises(ConfigError, match="NEBULY_API_KEY"):
        read([], env)


def test_hash_secret_is_required_only_to_pseudonymize_users(
    env: dict[str, str],
) -> None:
    del env["USER_HASH_SECRET"]
    assert not read(["--dry-run"], env).pseudonymize_end_user
    env["PSEUDONYMIZE_END_USER"] = "true"
    with pytest.raises(ConfigError, match="USER_HASH_SECRET"):
        read(["--dry-run"], env)
    env["USER_HASH_SECRET"] = "secret"
    assert read(["--dry-run"], env).pseudonymize_end_user


def test_role_booleans_and_secrets(env: dict[str, str]) -> None:
    env["AWS_EXTERNAL_ID"] = "test-external-id"
    with pytest.raises(ConfigError, match="requires AWS_ROLE_ARN"):
        read([], env)
    env["AWS_ROLE_ARN"] = "synthetic-role"
    env["ANONYMIZE"] = "typo"
    with pytest.raises(ConfigError, match="ANONYMIZE must be a boolean"):
        read([], env)
    env["ANONYMIZE"] = "yes"
    config = read([], env)
    assert config.anonymize
    for name in ("USER_HASH_SECRET", "NEBULY_API_KEY", "AWS_EXTERNAL_ID"):
        assert env[name] not in repr(config)


@pytest.mark.parametrize(
    "endpoint",
    [
        "backend.example/events",
        "ftp://example.com/events",
        "https:///events",
        "https://user:password@example.com/events",
        "https://example.com/events#part",
        "https://example.com:99999/events",
        "https://[broken/events",
        "https://exam ple.com/events",
        "https://example.com/ev\nents",
    ],
)
def test_invalid_endpoint(env: dict[str, str], endpoint: str) -> None:
    env["NEBULY_ENDPOINT"] = endpoint
    with pytest.raises(ConfigError, match="NEBULY_ENDPOINT"):
        read([], env)
