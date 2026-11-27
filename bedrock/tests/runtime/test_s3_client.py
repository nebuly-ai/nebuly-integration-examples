from __future__ import annotations

import gzip
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from bedrock_sync.runtime.grouping import CallRef
from bedrock_sync.runtime.s3_client import (
    LocalObjectStore,
    LogObject,
    LogRecord,
    LogSource,
    S3ObjectStore,
    _is_missing,
    parse_delivered_at,
)
from botocore.exceptions import ClientError
from botocore.stub import Stubber

if TYPE_CHECKING:
    from pathlib import Path


ROOT = "logs/AWSLogs/123456789012/BedrockModelInvocationLogs/eu-north-1"


def _key(hour: str, stamp: str, suffix: str = "abc123") -> str:
    return f"{ROOT}/2026/11/11/{hour}/20261111T{stamp}Z_{suffix}.json.gz"


def _write(root: Path, key: str, lines: list[Any] | bytes) -> None:
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(lines, bytes):
        path.write_bytes(lines)
    else:
        payload = "\n".join(json.dumps(x) for x in lines)
        path.write_bytes(gzip.compress(payload.encode()))


def _dt(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def _source(root: Path, root_prefix: str = ROOT) -> LogSource:
    return LogSource(
        LocalObjectStore(root), root_prefix=root_prefix, max_workers=2, read_ahead=4
    )


def test_parse_delivered_at() -> None:
    assert parse_delivered_at(_key("16", "164458346")) == _dt("2026-11-11T16:44:58.346")
    assert (
        parse_delivered_at(f"{ROOT}/2026/11/11/16/amazon-bedrock-logs-permission-check")
        is None
    )


def test_list_objects_uses_delivery_time_window(tmp_path: Path) -> None:
    keys = [
        _key("09", "095959999"),
        _key("10", "100000000"),
        _key("10", "103000000"),
        _key("11", "110500000"),
        _key("12", "120000001"),
    ]
    for key in keys:
        _write(tmp_path, key, [{"requestId": key}])
    _write(tmp_path, f"{ROOT}/2026/11/11/10/data/req_input.json.gz", [{}])
    _write(tmp_path, f"{ROOT}/2026/11/11/10/amazon-bedrock-logs-permission-check", b"")

    objects = _source(tmp_path).list_objects(
        since=_dt("2026-11-11T10:00:00"), until=_dt("2026-11-11T12:00:00")
    )

    assert [o.key for o in objects] == keys[1:4]


def test_bucket_prefix_named_data_is_not_mistaken_for_bodies(tmp_path: Path) -> None:
    root = "data/AWSLogs/123456789012/BedrockModelInvocationLogs/eu-north-1"
    key = f"{root}/2026/11/11/10/20261111T100100000Z_abc123.json.gz"
    _write(tmp_path, key, [{}])
    _write(tmp_path, f"{root}/2026/11/11/10/data/req_input.json.gz", [{}])

    objects = _source(tmp_path, root).list_objects(
        since=_dt("2026-11-11T10:00:00"), until=_dt("2026-11-11T11:00:00")
    )

    assert [o.key for o in objects] == [key]


def test_list_objects_resumes_after_cursor(tmp_path: Path) -> None:
    keys = [_key("10", "101000000"), _key("10", "102000000"), _key("11", "110000000")]
    for key in keys:
        _write(tmp_path, key, [{}])

    objects = _source(tmp_path).list_objects(
        since=_dt("2026-11-11T10:00:00"),
        until=_dt("2026-11-11T12:00:00"),
        after_key=keys[0],
    )

    assert [o.key for o in objects] == keys[1:]


def test_read_object_decodes_lines_and_inlines_large_bodies(tmp_path: Path) -> None:
    body_key = f"{ROOT}/2026/11/11/10/data/req-2_input.json.gz"
    _write(tmp_path, body_key, [{"messages": [{"role": "user", "content": "big"}]}])
    key = _key("10", "100100000")
    path = tmp_path / key
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {"requestId": "req-1", "input": {"inputBodyJson": {"messages": []}}}
        ),
        "",
        "{not json",
        json.dumps(
            {
                "requestId": "req-2",
                "input": {"inputBodyS3Path": f"s3://bucket/{body_key}"},
            }
        ),
        json.dumps(
            {
                "requestId": "req-3",
                "input": {"inputBodyS3Path": "s3://bucket/missing.json.gz"},
            }
        ),
    ]
    path.write_bytes(gzip.compress("\n".join(lines).encode()))
    obj = LogObject(key=key, delivered_at=_dt("2026-11-11T10:01:00"))

    records = _source(tmp_path).read_object(obj)

    assert [r.request_id for r in records] == ["req-1", "req-2", "req-3"]
    assert (
        records[1].record["input"]["inputBodyJson"]["messages"][0]["content"] == "big"
    )
    assert "inputBodyJson" not in records[2].record["input"]


def test_read_object_preserves_unicode_separators_inside_json_strings(
    tmp_path: Path,
) -> None:
    key = _key("10", "100100000")
    expected = [
        {"requestId": "req-1", "input": "first\u2028second\u2029third\u0085fourth"},
        {"requestId": "req-2", "input": "next record"},
    ]
    payload = "\r\n".join(json.dumps(record, ensure_ascii=False) for record in expected)
    _write(tmp_path, key, gzip.compress(payload.encode()))
    obj = LogObject(key=key, delivered_at=_dt("2026-11-11T10:01:00"))
    source = _source(tmp_path)

    assert [record.record for record in source.read_object(obj)] == expected
    assert source.problems.unreadable_lines == 0


def test_unreadable_body_stops_the_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only a body that is gone costs its record: any other error is raised,
    so the cursor does not move past the record."""
    body_key = f"{ROOT}/2026/11/11/10/data/req-1_input.json.gz"
    key = _key("10", "100100000")
    _write(
        tmp_path,
        key,
        [{"requestId": "req-1", "input": {"inputBodyS3Path": f"s3://b/{body_key}"}}],
    )
    real_get = LocalObjectStore.get_bytes

    def denied(self: LocalObjectStore, wanted: str) -> bytes:
        if wanted == body_key:
            raise PermissionError(wanted)
        return real_get(self, wanted)

    monkeypatch.setattr(LocalObjectStore, "get_bytes", denied)
    obj = LogObject(key=key, delivered_at=_dt("2026-11-11T10:01:00"))

    with pytest.raises(PermissionError):
        _source(tmp_path).read_object(obj)


def test_only_a_missing_object_counts_as_missing() -> None:
    def client_error(code: str) -> ClientError:
        return ClientError({"Error": {"Code": code}}, "GetObject")

    assert _is_missing(FileNotFoundError())
    assert _is_missing(client_error("NoSuchKey"))
    assert not _is_missing(client_error("AccessDenied"))
    assert not _is_missing(PermissionError())


@pytest.mark.parametrize(
    ("content", "files", "lines", "kept"),
    [
        (gzip.compress(b'{"requestId": "x"}\n')[:-6], 1, 0, 0),
        (gzip.compress(b'{"requestId": "\xff"}\n'), 1, 0, 0),
        (gzip.compress(b'{"requestId": "a"}\nnot json\n{"requestId": "b"}\n'), 0, 1, 2),
    ],
    ids=["broken-gzip", "not-utf8", "bad-line"],
)
def test_unreadable_content_is_skipped_counted_and_logged(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    content: bytes,
    files: int,
    lines: int,
    kept: int,
) -> None:
    key = _key("10", "100100000")
    _write(tmp_path, key, content)
    obj = LogObject(key=key, delivered_at=_dt("2026-11-11T10:01:00"))
    source = _source(tmp_path)

    with caplog.at_level(logging.ERROR):
        assert len(source.read_object(obj)) == kept

    assert (source.problems.unreadable_files, source.problems.unreadable_lines) == (
        files,
        lines,
    )
    assert key in caplog.text


def test_iter_records_keeps_object_order(tmp_path: Path) -> None:
    # More objects than read_ahead, so several batches are read.
    keys = [_key("10", f"10{i:02d}00000") for i in range(10)]
    for i, key in enumerate(keys):
        _write(tmp_path, key, [{"requestId": str(i)}])
    source = _source(tmp_path)
    objects = source.list_objects(
        since=_dt("2026-11-11T10:00:00"), until=_dt("2026-11-11T11:00:00")
    )

    seen = [records[0].request_id for _, records in source.iter_records(objects)]

    assert seen == [str(i) for i in range(10)]


def test_s3_store_passes_prefix_and_cursor() -> None:
    client = boto3.client(
        "s3",
        region_name="eu-north-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    key = _key("10", "100100000")
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": key}], "IsTruncated": False},
            {
                "Bucket": "bucket",
                "Prefix": f"{ROOT}/2026/11/11/10/",
                "StartAfter": "cursor",
            },
        )
        store = S3ObjectStore("bucket", client)

        keys = list(store.list_keys(f"{ROOT}/2026/11/11/10/", start_after="cursor"))

    assert keys == [key]


def test_read_requests_reads_each_file_once_and_skips_files_gone(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    kept, gone = _key("10", "100000000", "a"), _key("10", "100500000", "b")
    _write(
        tmp_path, kept, [{"requestId": "r1"}, {"requestId": "r2"}, {"requestId": "r3"}]
    )
    source = _source(tmp_path)
    reads: list[str] = []
    original = source.read_object

    def counting(obj: LogObject) -> list[LogRecord]:
        reads.append(obj.key)
        return original(obj)

    source.read_object = counting  # type: ignore[method-assign]
    records = source.read_requests(
        [CallRef("r1", kept, "a"), CallRef("r3", kept, "a"), CallRef("r9", gone, "b")]
    )

    assert [r.request_id for r in records] == ["r1", "r3"]
    assert reads == [kept, gone]
    assert "could not restore 1 call(s) of open interactions" in caplog.text
