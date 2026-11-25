from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3

from . import SourceStats
from .objects import StoredObject, cursor_for, records_from_object, select_objects

if TYPE_CHECKING:
    from collections.abc import Iterator

    from botocore.client import BaseClient

    from github_copilot_sync.config import Config
    from github_copilot_sync.models import UsageRecord


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
        self._client: Any = client or boto3.client("s3", **session_kwargs)
        self.stats = SourceStats()

    def _list_objects(self) -> Iterator[StoredObject]:
        pages = self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=self._prefix
        )
        for page in pages:
            for item in page.get("Contents") or []:
                if item.get("Key"):
                    yield StoredObject(
                        key=str(item["Key"]),
                        modified_ms=int(item["LastModified"].timestamp() * 1000),
                    )

    def iter_records(self, cursor: str | None) -> Iterator[tuple[UsageRecord, str]]:
        for obj in select_objects(self._list_objects(), cursor, self._config):
            raw = self._client.get_object(Bucket=self._bucket, Key=obj.key)[
                "Body"
            ].read()
            position = cursor_for(obj, self._config)
            for record in records_from_object(raw, obj.key, self._config, self.stats):
                yield record, position
