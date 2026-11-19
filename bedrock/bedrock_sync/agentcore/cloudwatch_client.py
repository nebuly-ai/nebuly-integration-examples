"""Read complete, bounded CloudWatch windows without trace-search sampling.

FilterLogEvents uses event time, not ingestion time. Pagination tokens are ephemeral
and must never be stored as durable coverage. SDK retries are read-only and bounded.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Protocol, cast

from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
    UnauthorizedSSOTokenError,
)

from bedrock_sync.shared.aws import aws_session
from bedrock_sync.shared.errors import ConnectorError

from .records import LogPage, RawLogRecord

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from .config import AgentCoreConfig


class LogsAPI(Protocol):
    def filter_log_events(self, **kwargs: object) -> dict[str, Any]: ...


class PaginationError(ConnectorError):
    """CloudWatch did not provide a safely exhaustible window."""


class InvalidLogResponseError(ConnectorError, ValueError):
    """A response cannot be persisted without losing its record identity."""


class LogsAccessError(ConnectorError):
    """A CloudWatch read needs different credentials or source settings."""


_CREDENTIAL_ERRORS = (
    NoCredentialsError,
    PartialCredentialsError,
    UnauthorizedSSOTokenError,
)
_CREDENTIAL_GUIDANCE = (
    "AWS credentials are unavailable or expired; check the configured profile "
    "or role and refresh the AWS login"
)


def _access_error(error: ClientError) -> LogsAccessError | None:
    code = error.response.get("Error", {}).get("Code")
    if code in {"ExpiredToken", "ExpiredTokenException", "UnrecognizedClientException"}:
        return LogsAccessError(_CREDENTIAL_GUIDANCE)
    if code in {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation"}:
        if error.operation_name == "AssumeRole":
            return LogsAccessError(
                "AWS denied sts:AssumeRole; check the source credentials and "
                "the configured role's trust policy"
            )
        return LogsAccessError(
            "AWS denied logs:FilterLogEvents; check the role's permissions "
            "for each configured --log-group"
        )
    if code == "ResourceNotFoundException":
        return LogsAccessError(
            "CloudWatch log group not found; check --log-group and BEDROCK_REGION"
        )
    return None


@contextmanager
def _explain_access_errors() -> Iterator[None]:
    try:
        yield
    except _CREDENTIAL_ERRORS:
        raise LogsAccessError(_CREDENTIAL_GUIDANCE) from None
    except ClientError as exc:
        if access_error := _access_error(exc):
            raise access_error from None
        raise


def create_logs_client(config: AgentCoreConfig) -> LogsAPI:
    """CloudWatch Logs client on the shared AWS session (``shared/aws.py``)."""
    sdk_config = BotoConfig(
        connect_timeout=10,
        read_timeout=30,
        retries={"mode": "standard", "total_max_attempts": 5},
    )
    with _explain_access_errors():
        session = aws_session(
            config, session_name="nebuly-agentcore-reader", boto_config=sdk_config
        )
        return cast("LogsAPI", session.client("logs", config=sdk_config))


class CloudWatchLogReader:
    """Yield one persisted-page unit at a time; coverage belongs to the store."""

    def __init__(
        self,
        client: LogsAPI,
        log_groups: Sequence[str],
        *,
        page_size: int = 10000,
        max_pages_per_group: int = 100000,
    ) -> None:
        if not log_groups or any(not group.strip() for group in log_groups):
            raise ValueError("At least one nonblank log group is required")
        if not 1 <= page_size <= 10000 or max_pages_per_group < 1:
            raise ValueError("Invalid page size or pagination budget")
        self._client = client
        self.log_groups = tuple(dict.fromkeys(log_groups))
        self._page_size = page_size
        self._max_pages = max_pages_per_group

    def iter_pages(
        self, start_ms: int, end_ms: int, *, trace_id: str | None = None
    ) -> Iterator[LogPage]:
        """Read [start_ms, end_ms), including empty nonterminal pages.

        Trace filtering is an optimization only. The parser still validates exact
        trace and resource identity. No untrusted filter expression is accepted.
        """
        if start_ms < 0 or end_ms <= start_ms:
            raise ValueError("Expected a nonnegative, nonempty event-time window")
        if trace_id is not None and (
            len(trace_id) != 32
            or any(char not in "0123456789abcdefABCDEF" for char in trace_id)
        ):
            raise ValueError("Expected a 32-character hexadecimal trace ID")
        for group in self.log_groups:
            yield from self._group_pages(group, start_ms, end_ms, trace_id)

    def _group_pages(
        self, group: str, start_ms: int, end_ms: int, trace_id: str | None
    ) -> Iterator[LogPage]:
        request: dict[str, Any] = {
            "logGroupName": group,
            "startTime": start_ms,
            # AWS endTime includes that millisecond; our upper bound excludes it.
            "endTime": end_ms - 1,
            "limit": self._page_size,
        }
        if trace_id is not None:
            request["filterPattern"] = '{ $.traceId = "' + trace_id + '" }'
        seen_tokens: set[str] = set()
        for _ in range(self._max_pages):
            with _explain_access_errors():
                response = self._client.filter_log_events(**request)
            events = response.get("events", [])
            if not isinstance(events, list):
                raise InvalidLogResponseError("Expected a list of CloudWatch events")
            records = tuple(_record(group, event) for event in events)
            if any(not start_ms <= item.timestamp_ms < end_ms for item in records):
                raise InvalidLogResponseError(
                    "CloudWatch returned an event outside the requested window"
                )
            token = response.get("nextToken")
            if token is not None and (not isinstance(token, str) or not token):
                raise InvalidLogResponseError("Invalid CloudWatch pagination token")
            if token in seen_tokens:
                raise PaginationError("CloudWatch pagination token repeated")
            yield LogPage(
                group,
                start_ms,
                end_ms,
                records,
                complete=token is None,
                trace_id=trace_id,
            )
            if token is None:
                return
            seen_tokens.add(token)
            request["nextToken"] = token
        raise PaginationError("CloudWatch pagination budget exhausted")


def _record(group: str, event: object) -> RawLogRecord:
    if not isinstance(event, dict):
        raise InvalidLogResponseError("Expected a CloudWatch event object")
    for name in ("eventId", "logStreamName", "message"):
        value = event.get(name)
        if not isinstance(value, str) or (name != "message" and not value):
            raise InvalidLogResponseError("Missing or invalid CloudWatch record field")
    for name in ("timestamp", "ingestionTime"):
        value = event.get(name)
        if type(value) is not int or value < 0:
            raise InvalidLogResponseError(
                "Missing or invalid CloudWatch record timestamp"
            )
    return RawLogRecord(
        log_group=group,
        log_stream=event["logStreamName"],
        event_id=event["eventId"],
        timestamp_ms=event["timestamp"],
        ingestion_time_ms=event["ingestionTime"],
        message=event["message"],
    )
