from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from .config import datetime_to_timestamp_str, timestamp_str_to_datetime

if TYPE_CHECKING:
    from pathlib import Path

# NULL coverage_from means the conversation is covered from the beginning.
# A failed row that covered nothing stores this sentinel instead of NULL.
_UNCOVERED = datetime(9999, 1, 1, tzinfo=UTC)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_source_state (
  organization_uuid TEXT NOT NULL,
  source            TEXT NOT NULL,
  watermark         TEXT,
  updated_at        TEXT NOT NULL,
  PRIMARY KEY (organization_uuid, source)
);

CREATE TABLE IF NOT EXISTS sync_conversation_state (
  organization_uuid TEXT NOT NULL,
  source            TEXT NOT NULL,
  conversation_id   TEXT NOT NULL,
  updated_at_seen   TEXT NOT NULL,
  coverage_from     TEXT,
  status            TEXT NOT NULL,
  last_error        TEXT,
  metadata_json     TEXT,
  updated_at        TEXT NOT NULL,
  PRIMARY KEY (organization_uuid, source, conversation_id)
);

CREATE TABLE IF NOT EXISTS sync_emitted_interaction (
  organization_uuid TEXT NOT NULL,
  source            TEXT NOT NULL,
  conversation_id   TEXT NOT NULL,
  interaction_key   TEXT NOT NULL,
  status            TEXT NOT NULL,
  updated_at        TEXT NOT NULL,
  PRIMARY KEY (organization_uuid, source, conversation_id, interaction_key)
);
"""


@dataclass(frozen=True)
class ConversationState:
    source: str
    conversation_id: str
    updated_at_seen: datetime
    coverage_from: datetime | None
    status: str
    last_error: str | None
    metadata_json: str | None


def merge_coverage_from(
    existing: datetime | None,
    new: datetime | None,
) -> datetime | None:
    """Combine two coverage starts. None means covered from the beginning."""
    if existing is None or new is None:
        return None
    return min(existing, new)


def _row_ts(value: str | None) -> datetime | None:
    if value is None:
        return None
    return timestamp_str_to_datetime(value)


def _now_ts() -> str:
    return datetime_to_timestamp_str(datetime.now(tz=UTC))


class SyncCache:
    def __init__(self, db_path: Path, organization_uuid: str, *, dry_run: bool) -> None:
        self._organization_uuid = organization_uuid
        self._dry_run = dry_run
        if dry_run:
            self._conn = sqlite3.connect(":memory:")
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    def get_conversation(
        self, source: str, conversation_id: str
    ) -> ConversationState | None:
        row = self._conn.execute(
            """
            SELECT source, conversation_id, updated_at_seen, coverage_from,
                   status, last_error, metadata_json
            FROM sync_conversation_state
            WHERE organization_uuid = ? AND source = ? AND conversation_id = ?
            """,
            (self._organization_uuid, source, conversation_id),
        ).fetchone()
        if row is None:
            return None
        return ConversationState(
            source=row[0],
            conversation_id=row[1],
            updated_at_seen=timestamp_str_to_datetime(row[2]),
            coverage_from=_row_ts(row[3]),
            status=row[4],
            last_error=row[5],
            metadata_json=row[6],
        )

    def should_process(
        self,
        source: str,
        conversation_id: str,
        updated_at: datetime,
        requested_from: datetime | None,
    ) -> bool:
        """True when the conversation needs a fetch.

        Gone conversations are never processed. Pending and failed ones always
        are. A completed conversation is processed when its source `updated_at`
        changed, or when this run asks for history earlier than `coverage_from`.
        `coverage_from is None` means the stored row already covers the start
        of the conversation, so a later `--from-date` does not backfill it.
        """
        state = self.get_conversation(source, conversation_id)
        if state is None:
            return True
        if state.status == "gone":
            return False
        if state.status in {"pending", "failed"}:
            return True
        if updated_at != state.updated_at_seen:
            return True
        return _needs_backfill(requested_from, state.coverage_from)

    def upsert_conversation(
        self,
        source: str,
        conversation_id: str,
        *,
        updated_at_seen: datetime,
        coverage_from: datetime | None,
        status: str,
        last_error: str | None = None,
        metadata_json: str | None = None,
        claim_coverage: bool = True,
    ) -> None:
        existing = self.get_conversation(source, conversation_id)
        if metadata_json is None and existing is not None:
            metadata_json = existing.metadata_json
        if not claim_coverage:
            if existing is not None:
                coverage_from = existing.coverage_from
            elif coverage_from is None:
                coverage_from = _UNCOVERED
        elif existing is not None:
            coverage_from = merge_coverage_from(existing.coverage_from, coverage_from)

        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO sync_conversation_state (
              organization_uuid, source, conversation_id, updated_at_seen,
              coverage_from, status, last_error, metadata_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (organization_uuid, source, conversation_id) DO UPDATE SET
              updated_at_seen = excluded.updated_at_seen,
              coverage_from = excluded.coverage_from,
              status = excluded.status,
              last_error = excluded.last_error,
              metadata_json = excluded.metadata_json,
              updated_at = excluded.updated_at
            """,
            (
                self._organization_uuid,
                source,
                conversation_id,
                datetime_to_timestamp_str(updated_at_seen),
                datetime_to_timestamp_str(coverage_from) if coverage_from else None,
                status,
                last_error,
                metadata_json,
                now,
            ),
        )

    def mark_gone(
        self,
        source: str,
        conversation_id: str,
        *,
        updated_at_seen: datetime,
        reason: str,
    ) -> None:
        self.upsert_conversation(
            source,
            conversation_id,
            updated_at_seen=updated_at_seen,
            coverage_from=updated_at_seen,
            status="gone",
            last_error=reason,
            claim_coverage=False,
        )

    def iter_unfinished(self, source: str) -> list[ConversationState]:
        rows = self._conn.execute(
            """
            SELECT source, conversation_id, updated_at_seen, coverage_from,
                   status, last_error, metadata_json
            FROM sync_conversation_state
            WHERE organization_uuid = ? AND source = ?
              AND status IN ('pending', 'failed')
            """,
            (self._organization_uuid, source),
        ).fetchall()
        return [
            ConversationState(
                source=row[0],
                conversation_id=row[1],
                updated_at_seen=timestamp_str_to_datetime(row[2]),
                coverage_from=_row_ts(row[3]),
                status=row[4],
                last_error=row[5],
                metadata_json=row[6],
            )
            for row in rows
        ]

    def is_emitted(
        self, source: str, conversation_id: str, interaction_key: str
    ) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM sync_emitted_interaction
            WHERE organization_uuid = ? AND source = ?
              AND conversation_id = ? AND interaction_key = ?
            """,
            (self._organization_uuid, source, conversation_id, interaction_key),
        ).fetchone()
        return row is not None

    def record_emitted(
        self,
        source: str,
        conversation_id: str,
        interaction_key: str,
        status: str,
    ) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO sync_emitted_interaction (
              organization_uuid, source, conversation_id, interaction_key,
              status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (organization_uuid, source, conversation_id, interaction_key)
            DO NOTHING
            """,
            (
                self._organization_uuid,
                source,
                conversation_id,
                interaction_key,
                status,
                now,
            ),
        )

    def get_watermark(self, source: str) -> datetime | None:
        row = self._conn.execute(
            """
            SELECT watermark FROM sync_source_state
            WHERE organization_uuid = ? AND source = ?
            """,
            (self._organization_uuid, source),
        ).fetchone()
        if row is None:
            return None
        return _row_ts(row[0])

    def set_watermark(self, source: str, watermark: datetime) -> None:
        now = _now_ts()
        self._conn.execute(
            """
            INSERT INTO sync_source_state (
              organization_uuid, source, watermark, updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT (organization_uuid, source) DO UPDATE SET
              watermark = excluded.watermark,
              updated_at = excluded.updated_at
            """,
            (
                self._organization_uuid,
                source,
                datetime_to_timestamp_str(watermark),
                now,
            ),
        )

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def _needs_backfill(
    requested_from: datetime | None,
    coverage_from: datetime | None,
) -> bool:
    if coverage_from is None:
        return False
    if requested_from is None:
        return True
    return requested_from < coverage_from
