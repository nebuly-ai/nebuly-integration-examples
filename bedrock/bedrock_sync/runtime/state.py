"""What the runtime sync remembers between runs, in one SQLite file.

One database per log source (bucket, prefix, account and region). It keeps:

- the S3 cursor: the covered range and the last key read;
- the calls of interactions still open, as pointers only (S3 key and request
  id): they are read again from S3, which keeps logs without a time limit, so
  no conversation text sits on disk while an interaction is open;
- every closed interaction id with the digest of its owner (conversation and
  user) and the outcome: queued, or why it was skipped. Kept forever, so a
  backfill never queues an interaction twice and a reused id is detected;
- the owner (digest of conversation and user) of every conversation with a
  closed interaction, by the digest of its id, kept forever: a conversation
  has one user, and that owner is final once an interaction closed;
- the outbox, with the delivery rules shared with AgentCore. Payloads of
  sent or rejected interactions are compacted after the content retention.

``checkpoint`` writes the cursor, the open pointers, the closed interactions
and their payloads in one transaction: after a crash the next run starts from
the last checkpoint and rebuilds the same interactions. A lock file prevents
two runs on the same database; a dry run works on an in-memory copy
(``Store`` in ``shared/store.py``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from bedrock_sync.shared import outbox
from bedrock_sync.shared.store import StateError, Store
from bedrock_sync.shared.time_utils import timestamp_str_to_datetime

from .grouping import CallRef

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

__all__ = ["QUEUED", "Closure", "Cursor", "RuntimeState", "StateError"]

QUEUED = "queued"

_SCHEMA = """
CREATE TABLE metadata(
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  log_source TEXT NOT NULL,
  coverage_from TEXT,
  coverage_until TEXT,
  last_key TEXT);
CREATE TABLE pending_call(
  request_id TEXT PRIMARY KEY,
  s3_key TEXT NOT NULL);
CREATE TABLE closed_interaction(
  interaction_id TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  closed_at TEXT NOT NULL,
  outcome TEXT NOT NULL);
CREATE TABLE conversation_owner(
  conversation TEXT PRIMARY KEY,
  owner TEXT NOT NULL);
"""


@dataclass(frozen=True)
class Cursor:
    """How far the logs were read.

    ``coverage_until`` is the delivery time of the last file read in full,
    ``last_key`` its S3 key: the next run lists the keys after it.
    """

    coverage_from: datetime
    coverage_until: datetime
    last_key: str | None


@dataclass(frozen=True)
class Closure:
    """A closed interaction: ``payload`` is None when it was skipped, and
    ``outcome`` then says why."""

    interaction_id: str
    owner: str
    outcome: str
    payload: Mapping[str, object] | None = None


def _ts(value: datetime) -> str:
    """Fixed width, so text order is time order."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="microseconds")


class RuntimeState(Store):
    LABEL = "runtime"
    APPLICATION_ID = 0x42525431
    SCHEMA_VERSION = 2

    def __init__(
        self,
        path: Path,
        log_source: str,
        *,
        dry_run: bool = False,
        now_ms: int | None = None,
    ) -> None:
        self.log_source = log_source
        super().__init__(path, dry_run=dry_run, now_ms=now_ms)

    def _initialize(self) -> None:
        if self._schema_exists():
            row = self._db.execute(
                "SELECT log_source FROM metadata WHERE singleton=1"
            ).fetchone()
            if row is None or row[0] != self.log_source:
                raise StateError("The database belongs to another log source")
            return
        self._create_schema(_SCHEMA)
        with self._transaction():
            self._db.execute(
                "INSERT INTO metadata VALUES (1,?,NULL,NULL,NULL)", (self.log_source,)
            )

    # Cursor and open calls -------------------------------------------------------

    def cursor(self) -> Cursor | None:
        row = self._db.execute(
            "SELECT coverage_from,coverage_until,last_key FROM metadata "
            "WHERE singleton=1"
        ).fetchone()
        if row[0] is None:
            return None
        return Cursor(
            timestamp_str_to_datetime(row[0]), timestamp_str_to_datetime(row[1]), row[2]
        )

    def pending_calls(self) -> list[CallRef]:
        """The calls of open interactions, in S3 key order."""
        return [
            CallRef(request_id=row[0], source_key=row[1])
            for row in self._db.execute(
                "SELECT request_id,s3_key FROM pending_call ORDER BY s3_key,request_id"
            )
        ]

    def closed_owner(self, interaction_id: str) -> str | None:
        row = self._db.execute(
            "SELECT owner FROM closed_interaction WHERE interaction_id=?",
            (interaction_id,),
        ).fetchone()
        return None if row is None else str(row[0])

    def conversation_owner(self, conversation: str) -> str | None:
        row = self._db.execute(
            "SELECT owner FROM conversation_owner WHERE conversation=?",
            (conversation,),
        ).fetchone()
        return None if row is None else str(row[0])

    def checkpoint(
        self,
        cursor: Cursor,
        open_calls: Iterable[CallRef],
        closures: Iterable[Closure],
        conversation_owners: Mapping[str, str] | None = None,
    ) -> None:
        """Save everything a restart needs, in one transaction."""
        with self._transaction():
            # The first owner wins: an owner already stored is never replaced.
            self._db.executemany(
                "INSERT OR IGNORE INTO conversation_owner VALUES (?,?)",
                list((conversation_owners or {}).items()),
            )
            self._db.execute("DELETE FROM pending_call")
            self._db.executemany(
                "INSERT OR IGNORE INTO pending_call VALUES (?,?)",
                [(ref.request_id, ref.source_key) for ref in open_calls],
            )
            for closure in closures:
                self._db.execute(
                    "INSERT INTO closed_interaction VALUES (?,?,?,?)",
                    (
                        closure.interaction_id,
                        closure.owner,
                        _ts(datetime.fromtimestamp(self.now_ms / 1000, UTC)),
                        closure.outcome,
                    ),
                )
                if closure.payload is not None:
                    outbox.put(self._db, closure.interaction_id, closure.payload)
            self._db.execute(
                "UPDATE metadata SET coverage_from=?,coverage_until=?,last_key=? "
                "WHERE singleton=1",
                (
                    _ts(cursor.coverage_from),
                    _ts(cursor.coverage_until),
                    cursor.last_key,
                ),
            )

    def reset(self) -> None:
        """Forget the cursor and the open calls, as the other integrations do
        when a run confirmed with ``--yes`` leaves a gap. Closed interactions
        and the outbox stay, so nothing is queued twice."""
        with self._transaction():
            self._db.execute("DELETE FROM pending_call")
            self._db.execute(
                "UPDATE metadata SET coverage_from=NULL,coverage_until=NULL,"
                "last_key=NULL WHERE singleton=1"
            )

    def prune_content(self, retention_seconds: int) -> int:
        """Compact payloads of interactions sent or rejected before the retention."""
        cutoff = self._retention_cutoff(retention_seconds)
        with self._transaction():
            return outbox.compact(self._db, cutoff)
