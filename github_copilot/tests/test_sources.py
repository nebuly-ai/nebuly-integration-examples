from __future__ import annotations

import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import boto3
import httpx
import pytest
from botocore.stub import Stubber
from github_copilot_sync.config import Config
from github_copilot_sync.sources.decode import decode_ndjson
from github_copilot_sync.sources.rest import RestUsageSource, parse_link_header


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
        cache_dir=Path("/tmp"),
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
                "Link": '<https://api.github.com/enterprises/acme/copilot/usage-records?page=2>; rel="next"'
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
    source.close()


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
    source.close()


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
        source.close()

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
    source.close()


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


def test_s3_start_after_pagination_and_records() -> None:
    from github_copilot_sync.sources.s3 import S3UsageSource

    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    key1 = "exports/part-001.ndjson"
    key2 = "exports/part-002.ndjson"
    payload = (
        json.dumps(_record_dict("s3-1"))
        + "\n"
        + json.dumps({"action": "ignored"})
        + "\n"
    ).encode()
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {
                "Contents": [{"Key": key1}],
                "IsTruncated": True,
                "NextContinuationToken": "token-1",
            },
            {
                "Bucket": "copilot-logs",
                "Prefix": "exports/",
                "StartAfter": "exports/part-000.ndjson",
            },
        )
        stubber.add_response(
            "get_object",
            {"Body": MagicMock(read=MagicMock(return_value=payload))},
            {"Bucket": "copilot-logs", "Key": key1},
        )
        stubber.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": key2}], "IsTruncated": False},
            {
                "Bucket": "copilot-logs",
                "Prefix": "exports/",
                "ContinuationToken": "token-1",
            },
        )
        stubber.add_response(
            "get_object",
            {"Body": MagicMock(read=MagicMock(return_value=payload))},
            {"Bucket": "copilot-logs", "Key": key2},
        )

        source = S3UsageSource(_config(sources=("s3",)), client=client)
        rows = list(source.iter_records(cursor="exports/part-000.ndjson"))

    assert [record.event_id for record, cursor in rows] == ["s3-1", "s3-1"]
    assert rows[-1][1] == key2
    assert source.stats.non_usage_events == 2


def test_azure_blob_skips_cursor_and_yields() -> None:
    from github_copilot_sync.sources.azure_blob import AzureBlobUsageSource

    container = MagicMock()
    blob_old = MagicMock()
    blob_old.name = "copilot/day1.ndjson"
    blob_new = MagicMock()
    blob_new.name = "copilot/day2.ndjson"
    container.list_blobs.return_value = [blob_old, blob_new]
    ndjson = json.dumps(_record_dict("az-1")).encode()
    container.download_blob.return_value.readall.return_value = ndjson

    source = AzureBlobUsageSource(
        _config(sources=("azure_blob",)),
        container=container,
    )
    rows = list(source.iter_records("copilot/day1.ndjson"))

    container.download_blob.assert_called_once_with("copilot/day2.ndjson")
    assert rows[0][0].event_id == "az-1"
    assert rows[0][1] == "copilot/day2.ndjson"
