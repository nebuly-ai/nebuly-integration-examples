from __future__ import annotations

from typing import TYPE_CHECKING

from azure.storage.blob import BlobServiceClient, ContainerClient

from . import SourceStats
from .objects import StoredObject, cursor_for, records_from_object, select_objects

if TYPE_CHECKING:
    from collections.abc import Iterator

    from github_copilot_sync.config import Config
    from github_copilot_sync.models import UsageRecord


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

    def _list_objects(self) -> Iterator[StoredObject]:
        for blob in self._container.list_blobs(name_starts_with=self._prefix):
            yield StoredObject(
                key=blob.name,
                modified_ms=int(blob.last_modified.timestamp() * 1000),
            )

    def iter_records(self, cursor: str | None) -> Iterator[tuple[UsageRecord, str]]:
        for obj in select_objects(self._list_objects(), cursor, self._config):
            raw = self._container.download_blob(obj.key).readall()
            position = cursor_for(obj, self._config)
            for record in records_from_object(raw, obj.key, self._config, self.stats):
                yield record, position
