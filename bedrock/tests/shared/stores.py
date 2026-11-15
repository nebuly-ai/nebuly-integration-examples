"""A minimal store over the shared outbox, for the shared tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bedrock_sync.shared import outbox
from bedrock_sync.shared.store import Store

if TYPE_CHECKING:
    from collections.abc import Mapping

    from bedrock_sync.shared.delivery import Delivery

PAYLOAD = {"interaction": {"time_start": "2026-11-11T10:00:00Z"}, "traces": []}


class OutboxStore(Store):
    LABEL = "test"
    APPLICATION_ID = 0x54455354
    SCHEMA_VERSION = 1

    def _initialize(self) -> None:
        if not self._schema_exists():
            self._create_schema("CREATE TABLE marker(value TEXT);")

    def put(self, key: str, payload: Mapping[str, object] = PAYLOAD) -> str:
        with self._transaction():
            outbox.put(self._db, key, payload)
        return key

    def get(self, key: str) -> Delivery:
        delivery = self.find_delivery(key)
        assert delivery is not None
        return delivery

    def prune(self, retention_seconds: int) -> int:
        with self._transaction():
            return outbox.compact(self._db, self._retention_cutoff(retention_seconds))
