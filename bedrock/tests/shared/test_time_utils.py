"""Timestamp conversions retain the runtime's UTC behavior after relocation."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from bedrock_sync.shared.time_utils import (
    datetime_to_timestamp_str,
    timestamp_str_to_datetime,
)


@pytest.mark.parametrize(
    "value",
    ["2026-11-13T12:00:00Z", "2026-11-13T13:00:00+01:00", "2026-11-13T12:00:00"],
)
def test_parse_normalizes_to_utc(value: str) -> None:
    assert timestamp_str_to_datetime(value) == datetime(2026, 11, 13, 12, tzinfo=UTC)


def test_serialize_normalizes_offsets_without_losing_fractional_seconds() -> None:
    value = datetime(
        2026, 11, 13, 13, 0, 0, 123456, tzinfo=timezone(timedelta(hours=1))
    )
    assert datetime_to_timestamp_str(value) == "2026-11-13T12:00:00.123456Z"
    assert timestamp_str_to_datetime(datetime_to_timestamp_str(value)) == value


def test_serialize_naive_time_as_utc() -> None:
    naive = datetime(2026, 11, 13, tzinfo=UTC).replace(tzinfo=None)
    assert datetime_to_timestamp_str(naive) == "2026-11-13T00:00:00Z"


def test_parse_missing_time_fails() -> None:
    with pytest.raises(ValueError, match="timestamp is required"):
        timestamp_str_to_datetime("")
