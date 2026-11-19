"""AgentCore settings; the shared ones are tested in tests/shared/test_config.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from bedrock_sync.agentcore.config import AgentCoreConfig
from bedrock_sync.agentcore.identity import DEFAULT_ACTOR_NAMESPACES
from bedrock_sync.shared.errors import ConfigError


@pytest.fixture
def env() -> dict[str, str]:
    return {
        "BEDROCK_ACCOUNT_ID": "123456789012",
        "BEDROCK_REGION": "eu-north-1",
        "USER_HASH_SECRET": "test-hash-secret",
        "NEBULY_API_KEY": "test-api-key",
    }


def args(*extra: str) -> list[str]:
    return ["--source-id", "synthetic-endpoint", "--log-group", "/test/runtime", *extra]


def test_defaults(env: dict[str, str]) -> None:
    config = AgentCoreConfig.from_env_and_args(args(), env=env)
    assert config.from_date is None  # the state checks first-run coverage later
    assert config.state_db == Path(".cache/agentcore/state.db")
    assert config.actor_id_attribute == "actor.id"
    assert config.actor_namespace_templates == DEFAULT_ACTOR_NAMESPACES
    assert (config.overlap_seconds, config.reconciliation_seconds) == (300, 86400)
    assert config.root_wait_seconds == 1200


def test_groups_and_overrides(env: dict[str, str]) -> None:
    config = AgentCoreConfig.from_env_and_args(
        args(
            "--log-group",
            "/test/runtime",
            "--log-group",
            "aws/spans",
            "--overlap-seconds",
            "0",
            "--actor-id-attribute",
            "customer.actor_id",
            "--actor-namespace-template",
            "/facts/{actorId}/",
            "--actor-namespace-template",
            "/users/{actorId}/",
            "--reconciliation-interval-seconds",
            "3600",
            "--root-wait-seconds",
            "0",
        ),
        env=env,
    )
    assert config.log_groups == ("/test/runtime", "aws/spans")
    assert config.overlap_seconds == 0
    assert config.actor_id_attribute == "customer.actor_id"
    assert config.actor_namespace_templates == (
        "/facts/{actorId}/",
        "/users/{actorId}/",
    )
    assert config.reconciliation_interval_seconds == 3600
    assert config.root_wait_seconds == 0


@pytest.mark.parametrize(
    "extra",
    [("--source-id", " "), ("--actor-id-attribute", " "), ("--log-group", " ")],
)
def test_blank_names_are_rejected(env: dict[str, str], extra: tuple[str, ...]) -> None:
    with pytest.raises(ConfigError, match="must not be blank"):
        AgentCoreConfig.from_env_and_args(args(*extra), env=env)


def test_help_does_not_require_credentials(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        AgentCoreConfig.from_env_and_args(["--help"], env={})
    assert error.value.code == 0
    assert "--log-group" in capsys.readouterr().out


@pytest.mark.parametrize(
    "template",
    ["/facts/", "/{actorId}/{actorId}/", "/prefix{actorId}/", "/{actorId}/{bad-name}/"],
)
def test_invalid_actor_namespace_is_rejected(
    env: dict[str, str], template: str
) -> None:
    with pytest.raises(ConfigError, match=r"namespace|Namespace"):
        AgentCoreConfig.from_env_and_args(
            args("--actor-namespace-template", template), env=env
        )
