"""Read Bedrock model invocation logs from S3.

Layout written by Bedrock::

    [<prefix>/]AWSLogs/<account>/BedrockModelInvocationLogs/<region>/
        YYYY/MM/DD/HH/<YYYYMMDDTHHMMSSmmm>Z_<hash>.json.gz   one record per line
        YYYY/MM/DD/HH/data/<requestId>_input.json.gz         bodies too large to inline

The hour folder and the timestamp in the file name are the *delivery* time,
30-90 s after the record ``timestamp`` (the start of the call) and later still
for long streamed calls. Files are not ordered by call time either: a call can
land in a file delivered before the file of an earlier call. So the sync
window and the resume cursor work on the object key, never on the record
``timestamp``, and the grouper reorders calls itself.
"""

from __future__ import annotations

import contextlib
import gzip
import itertools
import json
import logging
import re
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from bedrock_sync.shared.aws import aws_session

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path

    from mypy_boto3_s3 import S3Client

    from .config import Config
    from .grouping import CallRef
    from .models import JsonDict

logger = logging.getLogger(__name__)

_KEY_TIMESTAMP_RE = re.compile(r"/(\d{8}T\d{6})(\d{3})Z_[0-9A-Za-z]+\.json\.gz$")
_BODY_FIELDS = (
    ("input", "inputBodyJson", "inputBodyS3Path"),
    ("output", "outputBodyJson", "outputBodyS3Path"),
)


class ObjectStore(Protocol):
    def list_keys(self, prefix: str, *, start_after: str | None) -> Iterator[str]:
        """Keys under ``prefix``, sorted, strictly after ``start_after``."""
        ...

    def get_bytes(self, key: str) -> bytes: ...


@dataclass(frozen=True)
class LogObject:
    key: str
    delivered_at: datetime


@dataclass(frozen=True)
class LogRecord:
    key: str
    record: JsonDict

    @property
    def request_id(self) -> str:
        return str(self.record.get("requestId") or "")


class S3ObjectStore:
    def __init__(self, bucket: str, client: S3Client) -> None:
        self._bucket = bucket
        self._client = client

    def list_keys(self, prefix: str, *, start_after: str | None) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        kwargs: dict[str, str] = {"Bucket": self._bucket, "Prefix": prefix}
        if start_after:
            kwargs["StartAfter"] = start_after
        for page in paginator.paginate(**kwargs):  # type: ignore[arg-type]
            for obj in page.get("Contents") or []:
                yield obj["Key"]

    def get_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        return response["Body"].read()


class LocalObjectStore:
    """Directory laid out like the bucket. Used in tests and local dry runs."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def list_keys(self, prefix: str, *, start_after: str | None) -> Iterator[str]:
        base = self._root / prefix
        if not base.is_dir():
            return
        keys = sorted(
            p.relative_to(self._root).as_posix() for p in base.rglob("*") if p.is_file()
        )
        for key in keys:
            if start_after is None or key > start_after:
                yield key

    def get_bytes(self, key: str) -> bytes:
        return (self._root / key).read_bytes()


def build_s3_store(config: Config) -> S3ObjectStore:
    """S3 store on the shared AWS session (``shared/aws.py``)."""
    boto_config = BotoConfig(retries={"max_attempts": 10, "mode": "adaptive"})
    session = aws_session(
        config, session_name="nebuly-bedrock-runtime", boto_config=boto_config
    )
    return S3ObjectStore(config.s3_bucket, session.client("s3", config=boto_config))


def parse_delivered_at(key: str) -> datetime | None:
    match = _KEY_TIMESTAMP_RE.search(key)
    if match is None:
        return None
    seconds, millis = match.groups()
    dt = datetime.strptime(seconds, "%Y%m%dT%H%M%S").replace(tzinfo=UTC)
    return dt + timedelta(milliseconds=int(millis))


def _hour_floor(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _s3_uri_to_key(uri: str) -> str:
    if uri.startswith("s3://"):
        return uri.removeprefix("s3://").split("/", 1)[1]
    return uri


def _is_missing(exc: Exception) -> bool:
    """The object no longer exists (deleted, or expired by a lifecycle rule)."""
    if isinstance(exc, FileNotFoundError):
        return True
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code")
        return code in {"NoSuchKey", "404", "NotFound"}
    return False


@dataclass
class ReadProblems:
    """Log content that could not be read, for the run summary."""

    unreadable_files: int = 0
    unreadable_lines: int = 0


def _decode_lines(raw: bytes, *, source: str) -> tuple[list[JsonDict], ReadProblems]:
    """A corrupt file is logged and skipped rather than blocking every later
    run at the same cursor: S3 objects are written whole, so reading it again
    would not help. The calls in it are lost, so it is an error."""
    problems = ReadProblems()
    try:
        payload = gzip.decompress(raw)
    except gzip.BadGzipFile:
        payload = raw
    except (EOFError, zlib.error) as exc:
        logger.error(  # noqa: TRY400
            "Corrupt log file %s skipped, its calls are lost: %s", source, exc
        )
        problems.unreadable_files += 1
        return [], problems
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        logger.error(  # noqa: TRY400
            "Log file %s is not UTF-8 and was skipped, its calls are lost: %s",
            source,
            exc,
        )
        problems.unreadable_files += 1
        return [], problems
    records: list[JsonDict] = []
    # JSON Lines uses LF; Unicode separators can occur inside JSON strings.
    for line_no, line in enumerate(text.split("\n"), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            logger.error(  # noqa: TRY400
                "Line %s of %s is not JSON and was skipped, its call is lost",
                line_no,
                source,
            )
            problems.unreadable_lines += 1
            continue
        if isinstance(value, dict):
            records.append(value)
    return records, problems


class LogSource:
    def __init__(
        self,
        store: ObjectStore,
        *,
        root_prefix: str,
        max_workers: int = 8,
        read_ahead: int = 32,
    ) -> None:
        self._store = store
        self._root = root_prefix.strip("/")
        self._max_workers = max_workers
        self._read_ahead = max(read_ahead, 1)
        self.problems = ReadProblems()
        self._problems_lock = threading.Lock()

    def _hour_prefix(self, hour: datetime) -> str:
        return f"{self._root}/{hour:%Y/%m/%d/%H}/"

    def list_objects(
        self, *, since: datetime, until: datetime, after_key: str | None = None
    ) -> list[LogObject]:
        """Log files delivered in ``[since, until]``, oldest first.

        ``after_key`` is the resume cursor: the last key already processed.
        """
        objects: list[LogObject] = []
        hour = _hour_floor(since)
        while hour <= until:
            prefix = self._hour_prefix(hour)
            start_after = after_key if after_key and after_key >= prefix else None
            for key in self._store.list_keys(prefix, start_after=start_after):
                if key.removeprefix(prefix).startswith("data/"):
                    continue
                delivered_at = parse_delivered_at(key)
                if delivered_at is None or not since <= delivered_at <= until:
                    continue
                objects.append(LogObject(key=key, delivered_at=delivered_at))
            hour += timedelta(hours=1)
        objects.sort(key=lambda o: o.key)
        return objects

    def read_object(self, obj: LogObject) -> list[LogRecord]:
        records, problems = _decode_lines(
            self._store.get_bytes(obj.key), source=obj.key
        )
        with self._problems_lock:
            self.problems.unreadable_files += problems.unreadable_files
            self.problems.unreadable_lines += problems.unreadable_lines
        return [
            LogRecord(key=obj.key, record=self._inline_bodies(record))
            for record in records
        ]

    def iter_records(
        self, objects: list[LogObject]
    ) -> Iterator[tuple[LogObject, list[LogRecord]]]:
        """Read objects in parallel, yielding them in input order so the caller
        can advance its cursor object by object.

        At most ``read_ahead`` objects are held in memory at once, so a long
        backfill does not load the whole window before the first is used.
        """
        if not objects:
            return
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            for batch in itertools.batched(objects, self._read_ahead):
                yield from zip(batch, pool.map(self.read_object, batch), strict=True)

    def read_requests(self, calls: Iterable[CallRef]) -> list[LogRecord]:
        """Read again the calls saved at the last checkpoint. Each file is read
        once, however many of its records are wanted. Missing files are logged
        so the caller can report the calls that could not be restored."""
        wanted: dict[str, set[str]] = {}
        for call in calls:
            wanted.setdefault(call.source_key, set()).add(call.request_id)
        found: list[LogRecord] = []
        for key in sorted(wanted):
            obj = LogObject(
                key=key,
                delivered_at=parse_delivered_at(key)
                or datetime.min.replace(tzinfo=UTC),
            )
            try:
                records = self.read_object(obj)
            except Exception as exc:
                # A missing object only affects its turns. Other errors, such
                # as permissions or network failures, stop the run.
                if not _is_missing(exc):
                    raise
                logger.warning(
                    "Log file %s is missing: could not restore %d call(s) "
                    "of open interactions",
                    key,
                    len(wanted[key]),
                )
                continue
            found.extend(r for r in records if r.request_id in wanted[key])
        return found

    def _inline_bodies(self, record: JsonDict) -> JsonDict:
        """Replace ``*BodyS3Path`` references with the body they point to.

        On failure the reference is left in place and the parser skips the
        record as MISSING_BODY.
        """
        for section_name, inline_field, path_field in _BODY_FIELDS:
            section = record.get(section_name)
            if not isinstance(section, dict) or inline_field in section:
                continue
            uri = section.get(path_field)
            if not isinstance(uri, str):
                continue
            body = self._read_body(uri)
            if body is not None:
                record[section_name] = {**section, inline_field: body}
        return record

    def _read_body(self, uri: str) -> object:
        """``None`` if the body is gone or unreadable. Any other error
        (permissions, network) is raised: skipping the record would lose it
        for good once the cursor or a checkpoint moves past it."""
        key = _s3_uri_to_key(uri)
        try:
            payload = self._store.get_bytes(key)
            with contextlib.suppress(gzip.BadGzipFile):
                payload = gzip.decompress(payload)
            return json.loads(payload)
        except (ValueError, EOFError, zlib.error) as exc:
            logger.warning("Could not decode body %s: %s", key, exc)
            return None
        except Exception as exc:
            if not _is_missing(exc):
                raise
            logger.warning("Body %s not found: %s", key, exc)
            return None
