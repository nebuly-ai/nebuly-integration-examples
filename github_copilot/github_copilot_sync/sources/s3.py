from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3
from botocore.client import BaseClient

from github_copilot_sync.models import UsageRecord

from . import SourceStats
from .decode import decode_ndjson

if TYPE_CHECKING:
    from collections.abc import Iterator

    from github_copilot_sync.config import Config


class S3UsageSource:
    def __init__(
        self,
        config: Config,
        *,
        client: BaseClient | None = None,
    ) -> None:
        if not config.s3_bucket:
            raise RuntimeError("S3_BUCKET is required when source s3 is enabled")
        self._config = config
        self._bucket = config.s3_bucket
        self._prefix = config.s3_prefix
        session_kwargs: dict[str, Any] = {}
        if config.s3_endpoint_url:
            session_kwargs["endpoint_url"] = config.s3_endpoint_url
        self._client = client or boto3.client("s3", **session_kwargs)
        self.stats = SourceStats()

    def _list_keys(self, start_after: str) -> Iterator[str]:
        kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": self._prefix}
        if start_after:
            kwargs["StartAfter"] = start_after
        pending: list[str] = []
        has_more = True
        while has_more or pending:
            if not pending:
                response = self._client.list_objects_v2(**kwargs)
                pending = [
                    str(item.get("Key") or "")
                    for item in (response.get("Contents") or [])
                    if item.get("Key")
                ]
                has_more = bool(response.get("IsTruncated"))
                if has_more:
                    token = response.get("NextContinuationToken")
                    if not token:
                        has_more = False
                    else:
                        kwargs = {
                            "Bucket": self._bucket,
                            "Prefix": self._prefix,
                            "ContinuationToken": token,
                        }
                continue
            yield pending.pop(0)

    def iter_records(self, cursor: str | None) -> Iterator[tuple[UsageRecord, str]]:
        start_after = cursor or ""
        for key in self._list_keys(start_after):
            if cursor and key <= cursor:
                continue
            raw = self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()
            lines, problems = decode_ndjson(raw, source=key)
            self.stats.absorb_decode(
                files=problems.unreadable_files, lines=problems.unreadable_lines
            )
            for item in lines:
                record = UsageRecord.from_raw(item)
                if record is None:
                    self.stats.non_usage_events += 1
                    continue
                if self._config.to_date is not None:
                    if record.timestamp_ms >= int(
                        self._config.to_date.timestamp() * 1000
                    ):
                        return
                if self._config.from_date is not None:
                    if record.timestamp_ms < int(
                        self._config.from_date.timestamp() * 1000
                    ):
                        continue
                yield record, key
