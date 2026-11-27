from __future__ import annotations

import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import boto3
import httpx
from botocore.stub import Stubber
from github_copilot_sync.config import Config
from github_copilot_sync.sources.azure_blob import AzureBlobUsageSource
from github_copilot_sync.sources.decode import decode_ndjson
from github_copilot_sync.sources.rest import (
    RestUsageSource,
    _ms_to_phrase,
    parse_link_header,
)
from github_copilot_sync.sources.s3 import S3UsageSource

if TYPE_CHECKING:
    import pytest


def _config(**kwargs: object) -> Config:
    base = Config(
        github_token="token",
        github_enterprise="acme",
        github_api_url="https://api.github.com",
        github_api_version="2026-03-10",
        nebuly_api_key="n",
        nebuly_endpoint="https://nebuly.example/events",
        sources=("rest", "s3", "azure_blob"),
        pair_wait_minutes=15,
        conversation_gap_hours=8,
        interaction_idle_minutes=30,
        rest_overlap_seconds=60,
        resolve_users=True,
        s3_bucket="copilot-logs",
        s3_prefix="exports/",
        s3_endpoint_url=None,
        azure_storage_connection_string="DefaultEndpointsProtocol=https;AccountName=dev;AccountKey=x==;EndpointSuffix=core.windows.net",
        azure_blob_account_url=None,
        azure_blob_container="logs",
        azure_blob_prefix="copilot/",
        azure_blob_sas_token=None,
        anonymize=False,
        from_date=None,
        to_date=None,
        cache_dir=Path(),
        dry_run=True,
        verbose=False,
    )
    return replace(base, **kwargs)  # type: ignore[arg-type]


def _record_dict(
    event_id: str,
    *,
    timestamp_ms: int = 1_700_000_000_000,
    github_request_id: str = "gh-req",
) -> dict[str, Any]:
    return {
        "type": "request",
        "user_id": 1,
        "enterprise_id": 9,
        "endpoint": "/chat/completions",
        "body": json.dumps({}),
        "@timestamp": timestamp_ms,
        "truncated": False,
        "event_id": event_id,
        "github_request_id": github_request_id,
    }


def test_parse_link_header() -> None:
    header = (
        '<https://api.github.com/page=2>; rel="next", '
        '<https://api.github.com/page=1>; rel="first"'
    )
    links = parse_link_header(header)
    assert links["next"] == "https://api.github.com/page=2"


def test_rest_link_pagination() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "&page=2" in str(request.url) or "?page=2" in str(request.url):
            return httpx.Response(200, json=[_record_dict("e2")])
        return httpx.Response(
            200,
            json=[_record_dict("e1")],
            headers={
                "Link": (
                    "<https://api.github.com/enterprises/acme/copilot/"
                    'usage-records?page=2>; rel="next"'
                )
            },
        )

    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    source = RestUsageSource(_config(sources=("rest",)), client=client)
    records = list(source.iter_records(None))

    assert [record.event_id for record, _ in records] == ["e1", "e2"]
    assert len(calls) == 2


def test_rest_retries_429(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, json=[_record_dict("e1")])

    monkeypatch.setattr(
        "github_copilot_sync.sources.rest._retry_after_seconds", lambda *_: 0
    )
    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    source = RestUsageSource(_config(sources=("rest",)), client=client)
    rows = list(source.iter_records(None))

    assert attempts == 2
    assert rows[0][0].event_id == "e1"


def test_rest_warns_when_cursor_older_than_48h(
    caplog: pytest.LogCaptureFixture,
) -> None:
    old_ms = int((datetime.now(UTC) - timedelta(hours=72)).timestamp() * 1000)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    with caplog.at_level("WARNING"):
        source = RestUsageSource(_config(sources=("rest",)), client=client)
        list(source.iter_records(str(old_ms)))

    assert "REST only covers" in caplog.text


def test_rest_skips_non_usage_events() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[{"action": "repo.create"}, _record_dict("good")],
        )

    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    source = RestUsageSource(_config(sources=("rest",)), client=client)
    rows = list(source.iter_records(None))

    assert len(rows) == 1
    assert source.stats.non_usage_events == 1


def test_rest_counts_malformed_records_as_non_usage_events() -> None:
    missing_enterprise = _record_dict("bad-1")
    del missing_enterprise["enterprise_id"]
    null_user = {**_record_dict("bad-2"), "user_id": None}
    dict_body = {**_record_dict("bad-3"), "body": {"nested": "value"}}

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                _record_dict("good-1"),
                missing_enterprise,
                null_user,
                dict_body,
                _record_dict("good-2"),
            ],
        )

    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    source = RestUsageSource(_config(sources=("rest",)), client=client)
    rows = list(source.iter_records(None))

    assert [record.event_id for record, _ in rows] == ["good-1", "good-2"]
    assert source.stats.non_usage_events == 3


def test_decode_gzip_plain_and_corrupt() -> None:
    lines = [
        {
            "type": "request",
            "github_request_id": "a",
            "body": "{}",
            "event_id": "1",
            "@timestamp": 1,
        }
    ]
    plain = ("\n".join(json.dumps(line) for line in lines)).encode()
    gz = gzip.compress(plain)
    decoded_plain, _ = decode_ndjson(plain, source="plain")
    decoded_gz, _ = decode_ndjson(gz, source="gz")
    assert decoded_plain == decoded_gz

    bad_line = b'{"a":1}\nnot-json\n{"b":2}'
    decoded, problems = decode_ndjson(bad_line, source="mix")
    assert len(decoded) == 2
    assert problems.unreadable_lines == 1

    corrupt, problems = decode_ndjson(
        b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff",
        source="bad.gz",
    )
    assert corrupt == []
    assert problems.unreadable_files == 1


def _ms(hour: int) -> int:
    return int(datetime(2026, 1, 1, hour, tzinfo=UTC).timestamp() * 1000)


def _s3_client() -> Any:  # noqa: ANN401 - boto3 client has no precise public type
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _ndjson(*lines: dict[str, Any]) -> bytes:
    return "\n".join(json.dumps(line) for line in lines).encode()


def _stub_s3_object(stubber: Stubber, key: str, payload: bytes) -> None:
    stubber.add_response(
        "get_object",
        {"Body": MagicMock(read=MagicMock(return_value=payload))},
        {"Bucket": "copilot-logs", "Key": key},
    )


def _stub_s3_listing(stubber: Stubber, objects: list[tuple[str, int]]) -> None:
    stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {
                    "Key": key,
                    "LastModified": datetime.fromtimestamp(ms / 1000, tz=UTC),
                }
                for key, ms in objects
            ],
            "IsTruncated": False,
        },
        {"Bucket": "copilot-logs", "Prefix": "exports/"},
    )


def test_s3_paginates_and_reports_non_usage_events() -> None:
    client = _s3_client()
    key1 = "exports/part-001.ndjson"
    key2 = "exports/part-002.ndjson"
    payload = _ndjson(_record_dict("s3-1"), {"action": "ignored"})
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {
                "Contents": [
                    {"Key": key1, "LastModified": datetime(2026, 1, 1, 1, tzinfo=UTC)}
                ],
                "IsTruncated": True,
                "NextContinuationToken": "token-1",
            },
            {"Bucket": "copilot-logs", "Prefix": "exports/"},
        )
        stubber.add_response(
            "list_objects_v2",
            {
                "Contents": [
                    {"Key": key2, "LastModified": datetime(2026, 1, 1, 2, tzinfo=UTC)}
                ],
                "IsTruncated": False,
            },
            {
                "Bucket": "copilot-logs",
                "Prefix": "exports/",
                "ContinuationToken": "token-1",
            },
        )
        _stub_s3_object(stubber, key1, payload)
        _stub_s3_object(stubber, key2, payload)

        source = S3UsageSource(_config(sources=("s3",)), client=client)
        rows = list(source.iter_records(cursor=None))

    assert [record.event_id for record, _ in rows] == ["s3-1", "s3-1"]
    assert source.stats.non_usage_events == 2


def test_s3_counts_malformed_record_and_keeps_valid_ones() -> None:
    client = _s3_client()
    key = "exports/part-001.ndjson"
    malformed = _record_dict("bad")
    del malformed["enterprise_id"]
    payload = _ndjson(_record_dict("good-1"), malformed, _record_dict("good-2"))
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {
                "Contents": [
                    {"Key": key, "LastModified": datetime(2026, 1, 1, 1, tzinfo=UTC)}
                ],
                "IsTruncated": False,
            },
            {"Bucket": "copilot-logs", "Prefix": "exports/"},
        )
        _stub_s3_object(stubber, key, payload)

        source = S3UsageSource(_config(sources=("s3",)), client=client)
        rows = list(source.iter_records(cursor=None))

    assert [record.event_id for record, _ in rows] == ["good-1", "good-2"]
    assert source.stats.non_usage_events == 1


def test_s3_reads_late_object_whose_key_sorts_before_the_cursor() -> None:
    client = _s3_client()
    late_key = "exports/a-late.ndjson"
    old_key = "exports/z-old.ndjson"
    with Stubber(client) as stubber:
        _stub_s3_listing(stubber, [(old_key, _ms(1)), (late_key, _ms(5))])
        _stub_s3_object(stubber, late_key, _ndjson(_record_dict("late")))

        source = S3UsageSource(
            _config(sources=("s3",), object_lag_seconds=60), client=client
        )
        rows = list(source.iter_records(cursor=str(_ms(4))))

    assert [record.event_id for record, _ in rows] == ["late"]


def test_s3_orders_objects_by_modified_time_and_cursor_is_monotonic() -> None:
    client = _s3_client()
    with Stubber(client) as stubber:
        _stub_s3_listing(
            stubber, [("exports/a.ndjson", _ms(3)), ("exports/b.ndjson", _ms(2))]
        )
        _stub_s3_object(stubber, "exports/b.ndjson", _ndjson(_record_dict("b")))
        _stub_s3_object(stubber, "exports/a.ndjson", _ndjson(_record_dict("a")))

        source = S3UsageSource(
            _config(sources=("s3",), object_lag_seconds=60), client=client
        )
        rows = list(source.iter_records(cursor=None))

    assert [record.event_id for record, _ in rows] == ["b", "a"]
    cursors = [int(cursor) for _, cursor in rows]
    assert cursors == [_ms(2) - 60_000, _ms(3) - 60_000]


def test_s3_to_date_skips_newer_records_without_stopping() -> None:
    client = _s3_client()
    to_date = datetime.fromtimestamp(1_700_000_100, tz=UTC)
    newer = _record_dict("newer", timestamp_ms=1_700_000_200_000)
    older = _record_dict("older", timestamp_ms=1_700_000_000_000)
    with Stubber(client) as stubber:
        _stub_s3_listing(
            stubber, [("exports/a.ndjson", _ms(1)), ("exports/b.ndjson", _ms(2))]
        )
        _stub_s3_object(stubber, "exports/a.ndjson", _ndjson(newer, older))
        _stub_s3_object(stubber, "exports/b.ndjson", _ndjson(_record_dict("b-old")))

        source = S3UsageSource(_config(sources=("s3",), to_date=to_date), client=client)
        rows = list(source.iter_records(cursor=None))

    assert [record.event_id for record, _ in rows] == ["older", "b-old"]


def test_s3_skips_objects_modified_before_from_date() -> None:
    client = _s3_client()
    from_date = datetime(2026, 1, 1, 12, tzinfo=UTC)
    with Stubber(client) as stubber:
        _stub_s3_listing(
            stubber, [("exports/old.ndjson", _ms(1)), ("exports/new.ndjson", _ms(13))]
        )
        _stub_s3_object(
            stubber,
            "exports/new.ndjson",
            _ndjson(_record_dict("new", timestamp_ms=_ms(13))),
        )

        source = S3UsageSource(
            _config(sources=("s3",), from_date=from_date, object_lag_seconds=60),
            client=client,
        )
        rows = list(source.iter_records(cursor=None))

    assert [record.event_id for record, _ in rows] == ["new"]


def _blob(name: str, modified_ms: int) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    blob.last_modified = datetime.fromtimestamp(modified_ms / 1000, tz=UTC)
    return blob


def test_azure_blob_reads_late_blob_and_skips_processed_ones() -> None:
    container = MagicMock()
    container.list_blobs.return_value = [
        _blob("copilot/z-old.ndjson", _ms(1)),
        _blob("copilot/a-late.ndjson", _ms(5)),
    ]
    container.download_blob.return_value.readall.return_value = _ndjson(
        _record_dict("az-1")
    )

    source = AzureBlobUsageSource(
        _config(sources=("azure_blob",), object_lag_seconds=60),
        container=container,
    )
    rows = list(source.iter_records(str(_ms(4))))

    container.list_blobs.assert_called_once_with(name_starts_with="copilot/")
    container.download_blob.assert_called_once_with("copilot/a-late.ndjson")
    assert rows[0][0].event_id == "az-1"
    assert int(rows[0][1]) == _ms(5) - 60_000


def test_azure_blob_to_date_skips_newer_records_without_stopping() -> None:
    container = MagicMock()
    container.list_blobs.return_value = [_blob("copilot/a.ndjson", _ms(1))]
    container.download_blob.return_value.readall.return_value = _ndjson(
        _record_dict("newer", timestamp_ms=1_700_000_200_000),
        _record_dict("older", timestamp_ms=1_700_000_000_000),
    )

    source = AzureBlobUsageSource(
        _config(
            sources=("azure_blob",),
            to_date=datetime.fromtimestamp(1_700_000_100, tz=UTC),
        ),
        container=container,
    )
    rows = list(source.iter_records(None))

    assert [record.event_id for record, _ in rows] == ["older"]


def test_rest_logs_error_on_unknown_envelope(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [_record_dict("e1")]})

    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    source = RestUsageSource(_config(sources=("rest",)), client=client)
    with caplog.at_level("ERROR"):
        rows = list(source.iter_records(None))

    assert rows == []
    assert "items" in caplog.text
    assert "unknown" in caplog.text.lower()


def _first_phrase(config: Config, cursor: str | None) -> str:
    phrases: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        phrases.append(request.url.params["phrase"])
        return httpx.Response(200, json=[])

    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    list(RestUsageSource(config, client=client).iter_records(cursor))
    return phrases[0]


def test_rest_from_date_overrides_an_older_cursor() -> None:
    from_date = datetime.now(UTC) - timedelta(hours=1)
    old_cursor = int((from_date - timedelta(hours=5)).timestamp() * 1000)
    config = _config(sources=("rest",), from_date=from_date)

    assert _first_phrase(config, str(old_cursor)) == _ms_to_phrase(
        int(from_date.timestamp() * 1000)
    )


def test_rest_newer_cursor_wins_over_from_date() -> None:
    from_date = datetime.now(UTC) - timedelta(hours=5)
    new_cursor = int((from_date + timedelta(hours=4)).timestamp() * 1000)
    config = _config(sources=("rest",), from_date=from_date)

    assert _first_phrase(config, str(new_cursor)) == _ms_to_phrase(new_cursor)
