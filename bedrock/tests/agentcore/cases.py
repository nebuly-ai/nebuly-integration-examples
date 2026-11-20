"""Helpers over the AgentCore cases built by ``spans``."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

from bedrock_sync.agentcore.assembler import assemble
from bedrock_sync.agentcore.config import AgentCoreConfig
from bedrock_sync.agentcore.parser import parse_record
from bedrock_sync.agentcore.records import RawLogRecord, SourceScope

if TYPE_CHECKING:
    from bedrock_sync.agentcore.models import (
        IdentityLog,
        InteractionSnapshot,
        ParseResult,
        Span,
    )

from tests.agentcore.spans import SCENARIOS, load

__all__ = ["REAL_CASES", "Logs", "load", "parse_case", "raw_record", "snapshot"]

REAL_CASES = sorted(SCENARIOS)


def raw_record(event: dict[str, Any]) -> RawLogRecord:
    """A CloudWatch event of a case as the state stores it."""
    return RawLogRecord(
        event["logGroupName"],
        event["logStreamName"],
        event["eventId"],
        event["timestamp"],
        event["ingestionTime"],
        event["message"],
    )


def parse_case(case: dict[str, Any]) -> tuple[SourceScope, list[Span | IdentityLog]]:
    """Every span and identity log of a case; fails on any diagnostic."""
    source = SourceScope(**case["source"])
    records: list[Span | IdentityLog] = []
    for event in case["records"]:
        parsed = parse_record(raw_record(event), source)
        assert not parsed.diagnostics
        if parsed.span is not None:
            records.append(parsed.span)
        if parsed.identity is not None:
            records.append(parsed.identity)
    return source, records


def snapshot(case: dict[str, Any]) -> tuple[SourceScope, InteractionSnapshot]:
    """The interaction a case assembles to."""
    source = SourceScope(**case["source"])
    parsed = [parse_record(raw_record(event), source) for event in case["records"]]
    return source, assemble(source, records_of(parsed)).interactions[0]


def records_of(parsed: list[ParseResult]) -> list[Span | IdentityLog]:
    """The spans and identity logs among parse results."""
    return [
        record for item in parsed if (record := item.span or item.identity) is not None
    ]


def config(**env: str) -> AgentCoreConfig:
    """A dry-run configuration for the synthetic source."""
    return AgentCoreConfig.from_env_and_args(
        ["--source-id", "synthetic", "--log-group", "runtime", "--dry-run"],
        env={
            "BEDROCK_ACCOUNT_ID": "123456789012",
            "BEDROCK_REGION": "eu-north-1",
            "USER_HASH_SECRET": "test-secret",
            **env,
        },
    )


class Logs:
    """A CloudWatch Logs client over a list of events.

    ``empty_first_page`` answers each first request with an empty page and a
    token, as CloudWatch may do.
    """

    def __init__(
        self, events: list[dict[str, Any]], *, empty_first_page: bool = False
    ) -> None:
        self.events = events
        self.empty_first_page = empty_first_page
        self.requests: list[dict[str, Any]] = []

    def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        self.requests.append(kwargs)
        if self.empty_first_page and "nextToken" not in kwargs:
            return {"events": [], "nextToken": "next-page"}
        trace = re.search(r'"([a-f0-9]{32})"', str(kwargs.get("filterPattern", "")))
        return {
            "events": [
                {key: value for key, value in event.items() if key != "logGroupName"}
                for event in self.events
                if event["logGroupName"] == kwargs["logGroupName"]
                and kwargs["startTime"] <= event["timestamp"] <= kwargs["endTime"]
                and (
                    trace is None
                    or json.loads(event["message"]).get("traceId") == trace[1]
                )
            ]
        }
