"""Read-only pagination and event-time boundaries."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from bedrock_sync.agentcore.cloudwatch_client import (
    CloudWatchLogReader,
    InvalidLogResponseError,
    LogsAccessError,
    PaginationError,
    create_logs_client,
)
from bedrock_sync.agentcore.config import AgentCoreConfig


class FakeLogs:
    def __init__(self, *responses: dict[str, Any] | Exception) -> None:
        self.responses = iter(responses)
        self.requests: list[dict[str, Any]] = []

    def filter_log_events(self, **kwargs: object) -> dict[str, Any]:
        self.requests.append(kwargs)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def event(timestamp: int = 10, message: str = "{}") -> dict[str, Any]:
    return {
        "eventId": "synthetic-event",
        "logStreamName": "otel-rt-logs",
        "timestamp": timestamp,
        "ingestionTime": timestamp + 5000,
        "message": message,
    }


def test_empty_pages_tokens_and_all_groups() -> None:
    client = FakeLogs(
        {"events": [], "nextToken": "first"}, {"events": [event()]}, {"events": []}
    )
    pages = list(
        CloudWatchLogReader(client, ["runtime", "aws/spans"]).iter_pages(10, 20)
    )
    assert [page.complete for page in pages] == [False, True, True]
    assert [page.log_group for page in pages] == ["runtime", "runtime", "aws/spans"]
    assert client.requests[0]["endTime"] == 19
    assert client.requests[1]["nextToken"] == "first"
    assert "nextToken" not in client.requests[2]
    assert "message=" not in repr(pages[1].records[0])


@pytest.mark.parametrize("tokens", [["same", "same"], ["one", "two", "one"]])
def test_repeated_tokens_fail(tokens: list[str]) -> None:
    client = FakeLogs(*({"nextToken": token} for token in tokens))
    with pytest.raises(PaginationError, match="repeated"):
        list(CloudWatchLogReader(client, ["runtime"]).iter_pages(10, 20))


def test_page_budget_does_not_complete_window() -> None:
    client = FakeLogs({"nextToken": "one"})
    pages = CloudWatchLogReader(client, ["runtime"], max_pages_per_group=1).iter_pages(
        10, 20
    )
    assert not next(pages).complete
    with pytest.raises(PaginationError, match="budget"):
        next(pages)


def test_access_error_propagates() -> None:
    client = FakeLogs(PermissionError("Denied"))
    with pytest.raises(PermissionError):
        list(CloudWatchLogReader(client, ["runtime"]).iter_pages(10, 20))


@pytest.mark.parametrize(
    ("code", "guidance"),
    [
        ("AccessDeniedException", "logs:FilterLogEvents"),
        ("ResourceNotFoundException", "--log-group"),
        ("ExpiredTokenException", "refresh the AWS login"),
    ],
)
def test_read_access_error_has_safe_guidance(code: str, guidance: str) -> None:
    error = ClientError(
        {"Error": {"Code": code, "Message": "private resource ARN"}},
        "FilterLogEvents",
    )
    with pytest.raises(LogsAccessError, match=guidance) as shown:
        list(CloudWatchLogReader(FakeLogs(error), ["runtime"]).iter_pages(10, 20))
    assert "private resource ARN" not in str(shown.value)


@pytest.mark.parametrize(
    "bad",
    [
        {},
        event(20),
        event(9),
        {**event(), "timestamp": True},
        {**event(), "message": {}},
        {**event(), "eventId": ""},
    ],
)
def test_invalid_record_does_not_complete_window(bad: dict[str, Any]) -> None:
    with pytest.raises(InvalidLogResponseError):
        list(
            CloudWatchLogReader(FakeLogs({"events": [bad]}), ["runtime"]).iter_pages(
                10, 20
            )
        )


def test_long_span_selected_by_end_and_trace_filter() -> None:
    trace = "a" * 32
    message = json.dumps(
        {
            "traceId": trace,
            "startTimeUnixNano": 1_000_000,
            "endTimeUnixNano": 11_000_000,
        }
    )
    client = FakeLogs({"events": [event(11, message)]})
    page = next(
        CloudWatchLogReader(client, ["runtime"]).iter_pages(10, 20, trace_id=trace)
    )
    assert json.loads(page.records[0].message)["startTimeUnixNano"] < 10_000_000
    assert page.complete
    assert page.trace_id == trace
    assert client.requests[0]["filterPattern"] == '{ $.traceId = "' + trace + '" }'
    with pytest.raises(ValueError, match="trace ID"):
        list(
            CloudWatchLogReader(client, ["runtime"]).iter_pages(10, 20, trace_id='"bad')
        )


def test_factory_uses_bounded_read_retries_and_refreshable_role() -> None:
    config = AgentCoreConfig.from_env_and_args(
        ["--source-id", "synthetic", "--log-group", "runtime", "--dry-run"],
        env={
            "BEDROCK_ACCOUNT_ID": "123456789012",
            "BEDROCK_REGION": "eu-north-1",
            "AWS_ROLE_ARN": "synthetic-role",
            "AWS_EXTERNAL_ID": "synthetic-external",
        },
    )
    base, assumed, sts, logs = Mock(), Mock(), Mock(), Mock()
    base.client.return_value = sts
    assumed.client.return_value = logs
    sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "synthetic-key",
            "SecretAccessKey": "synthetic-secret",
            "SessionToken": "synthetic-token",
            "Expiration": datetime(2099, 1, 1, tzinfo=UTC),
        }
    }
    with (
        patch(
            "bedrock_sync.shared.aws.boto3.Session",
            side_effect=[base, assumed],
        ),
        patch("bedrock_sync.shared.aws.get_session") as core_session,
        patch(
            "bedrock_sync.shared.aws.RefreshableCredentials.create_from_metadata"
        ) as renewable,
    ):
        assert create_logs_client(config) is logs
        callback = renewable.call_args.kwargs["refresh_using"]
        assert callback()["access_key"] == "synthetic-key"
        assert sts.assume_role.call_count == 2
        assert sts.assume_role.call_args.kwargs["ExternalId"] == "synthetic-external"
        assert core_session.return_value._credentials is renewable.return_value
        sdk_config = assumed.client.call_args.kwargs["config"]
        assert sdk_config.retries["total_max_attempts"] == 5
        assert sdk_config.connect_timeout == 10
        assert sdk_config.read_timeout == 30
