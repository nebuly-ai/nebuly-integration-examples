from __future__ import annotations

from typing import TYPE_CHECKING

from azure.storage.blob import BlobServiceClient, ContainerClient

from github_copilot_sync.models import UsageRecord

from . import SourceStats
from .decode import decode_ndjson

if TYPE_CHECKING:
    from collections.abc import Iterator

    from github_copilot_sync.config import Config


def _container_client(config: Config) -> ContainerClient:
    if not config.azure_blob_container:
        raise RuntimeError(
            "AZURE_BLOB_CONTAINER is required when source azure_blob is enabled"
        )
    if config.azure_storage_connection_string:
        service = BlobServiceClient.from_connection_string(
            config.azure_storage_connection_string
        )
        return service.get_container_client(config.azure_blob_container)
    if config.azure_blob_account_url:
        sas = config.azure_blob_sas_token or ""
        account_url = config.azure_blob_account_url.rstrip("/")
        if sas and not sas.startswith("?"):
            sas = f"?{sas}"
        service = BlobServiceClient(account_url=f"{account_url}{sas}")
        return service.get_container_client(config.azure_blob_container)
    raise RuntimeError(
        "Set AZURE_STORAGE_CONNECTION_STRING or AZURE_BLOB_ACCOUNT_URL for azure_blob"
    )


class AzureBlobUsageSource:
    def __init__(
        self,
        config: Config,
        *,
        container: ContainerClient | None = None,
    ) -> None:
        self._config = config
        self._prefix = config.azure_blob_prefix
        self._container = container or _container_client(config)
        self.stats = SourceStats()

    def iter_records(self, cursor: str | None) -> Iterator[tuple[UsageRecord, str]]:
        last_seen = cursor or ""
        for blob in self._container.list_blobs(name_starts_with=self._prefix):
            name = blob.name
            if last_seen and name <= last_seen:
                continue
            downloader = self._container.download_blob(name)
            raw = downloader.readall()
            lines, problems = decode_ndjson(raw, source=name)
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
                yield record, name
