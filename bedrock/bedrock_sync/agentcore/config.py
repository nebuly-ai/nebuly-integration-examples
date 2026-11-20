"""Settings of the AgentCore connector (observability logs on CloudWatch).

The settings both connectors share are in ``shared/config.py``. This adds
which runtime endpoint and log groups to read, how far back to read again,
and where the actor id comes from.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from bedrock_sync.shared.config import (
    CommonConfig,
    add_common_arguments,
    load_env,
    nonnegative_int_arg,
)
from bedrock_sync.shared.errors import ConfigError

from .identity import DEFAULT_ACTOR_NAMESPACES, namespace_pattern

if TYPE_CHECKING:
    from collections.abc import Mapping

DEFAULT_STATE_DB = Path(".cache/agentcore/state.db")


@dataclass(frozen=True, kw_only=True)
class AgentCoreConfig(CommonConfig):
    source_id: str
    log_groups: tuple[str, ...]
    overlap_seconds: int = 300
    reconciliation_seconds: int = 86400
    reconciliation_interval_seconds: int = 86400
    root_wait_seconds: int = 1200
    actor_id_attribute: str = "actor.id"
    actor_namespace_templates: tuple[str, ...] = DEFAULT_ACTOR_NAMESPACES

    @classmethod
    def from_env_and_args(
        cls,
        argv: list[str] | None = None,
        *,
        env: Mapping[str, str] | None = None,
    ) -> AgentCoreConfig:
        parser = argparse.ArgumentParser(
            description="Send AgentCore invocations (CloudWatch) to Nebuly"
        )
        parser.add_argument(
            "--log-group",
            action="append",
            required=True,
            help="CloudWatch log group to read; repeat for each group",
        )
        parser.add_argument(
            "--source-id",
            required=True,
            help="Runtime endpoint ARN (cloud.resource_id of its spans); "
            "the state is bound to it",
        )
        add_common_arguments(parser, default_state_db=DEFAULT_STATE_DB)
        parser.add_argument(
            "--overlap-seconds",
            type=nonnegative_int_arg,
            default=300,
            help="Read again this much before the last run's end (default 300)",
        )
        parser.add_argument(
            "--reconciliation-seconds",
            type=nonnegative_int_arg,
            default=86400,
            help="Read this far before the previous reconciliation's end "
            "(default 1 day; 0 disables reconciliation reads)",
        )
        parser.add_argument(
            "--reconciliation-interval-seconds",
            type=nonnegative_int_arg,
            default=86400,
            help="Run a reconciliation at most this often (default 1 day)",
        )
        parser.add_argument(
            "--root-wait-seconds",
            type=nonnegative_int_arg,
            default=1200,
            help="Wait this long after an agent span ends for the spans above "
            "it (up to POST /invocations) before sending it as a turn; a "
            "sub-agent seen early would otherwise be sent (default 20 minutes)",
        )
        parser.add_argument(
            "--actor-id-attribute",
            default="actor.id",
            help="Span attribute holding the actor id (default actor.id)",
        )
        parser.add_argument(
            "--actor-namespace-template",
            action="append",
            help="Memory namespace with the actor id, as a fallback; repeat "
            "for each layout. Replaces the defaults: "
            + ", ".join(DEFAULT_ACTOR_NAMESPACES),
        )
        args = parser.parse_args(argv)
        values = load_env(env)
        source_id = args.source_id.strip()
        actor_id_attribute = args.actor_id_attribute.strip()
        log_groups = tuple(dict.fromkeys(group.strip() for group in args.log_group))
        if not all((source_id, actor_id_attribute, *log_groups)):
            raise ConfigError(
                "Source ID, actor attribute and log groups must not be blank"
            )
        templates = tuple(
            dict.fromkeys(args.actor_namespace_template or DEFAULT_ACTOR_NAMESPACES)
        )
        try:
            for template in templates:
                namespace_pattern(template)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        return cls(
            **CommonConfig.common_values(values, args),
            source_id=source_id,
            log_groups=log_groups,
            overlap_seconds=args.overlap_seconds,
            reconciliation_seconds=args.reconciliation_seconds,
            reconciliation_interval_seconds=args.reconciliation_interval_seconds,
            root_wait_seconds=args.root_wait_seconds,
            actor_id_attribute=actor_id_attribute,
            actor_namespace_templates=templates,
        )
