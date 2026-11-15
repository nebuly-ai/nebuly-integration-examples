"""The SQLite file both connectors keep their state in.

``Store`` opens the database, takes a lock so two runs never share it, and
gives a dry run an in-memory copy, so a dry run never changes the file. It
also implements the outbox side of ``DeliveryStore`` with ``outbox``.
Each connector adds its own tables and checks on top.
"""

from __future__ import annotations

import fcntl
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from typing import TYPE_CHECKING, ClassVar, Self

from . import outbox
from .errors import StateError

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType
    from typing import BinaryIO

    from .delivery import Delivery, Outcome

__all__ = ["StateError", "Store", "current_ms", "validate_ms"]


def current_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def validate_ms(*values: int) -> None:
    if any(type(value) is not int or value < 0 for value in values):
        raise ValueError("Timestamps must be nonnegative integer milliseconds")


class Store:
    """Single-process SQLite state; subclasses create and check their schema."""

    LABEL: ClassVar[str]
    """Shown in errors, for example "runtime"."""
    APPLICATION_ID: ClassVar[int]
    SCHEMA_VERSION: ClassVar[int]

    def __init__(
        self, path: Path, *, dry_run: bool = False, now_ms: int | None = None
    ) -> None:
        self.dry_run = dry_run
        self.now_ms = current_ms() if now_ms is None else now_ms
        validate_ms(self.now_ms)
        self._lock: BinaryIO | None = None
        self._connection: sqlite3.Connection | None = None
        path = path.expanduser().resolve()
        try:
            self._connection = (
                self._snapshot(path) if dry_run else self._open_locked(path)
            )
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("PRAGMA secure_delete=ON")
            self._initialize()
            if not dry_run:
                with self._transaction():
                    outbox.recover_attempts(self._db)
        except sqlite3.DatabaseError as exc:
            self.close()
            raise StateError(
                f"Cannot use {path.name} as the {self.LABEL} state database ({exc})"
            ) from exc
        except BaseException:
            self.close()
            raise

    def _open_locked(self, path: Path) -> sqlite3.Connection:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = path.with_name(path.name + ".lock").open("a+b")
        try:
            fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise StateError(
                f"Another process is using this {self.LABEL} database"
            ) from exc
        return sqlite3.connect(path, isolation_level=None)

    def _snapshot(self, path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        if path.exists():
            # A read-only copy of a rollback journal creates no sidecar file.
            if any(
                path.with_name(path.name + suffix).exists()
                for suffix in ("-wal", "-journal")
            ):
                connection.close()
                raise StateError("Cannot snapshot a database with active journal files")
            with closing(
                sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
            ) as original:
                original.backup(connection)
        return connection

    @property
    def _db(self) -> sqlite3.Connection:
        if self._connection is None:
            raise StateError(f"The {self.LABEL} database is closed")
        return self._connection

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def _transaction(self) -> _Transaction:
        return _Transaction(self._db)

    # Schema ---------------------------------------------------------------------

    def _initialize(self) -> None:
        """Create the schema in a new file, or check an existing one."""
        raise NotImplementedError

    def _schema_exists(self) -> bool:
        """True for a database of this connector and version, False for an empty
        one; any other file is refused without being changed."""
        tables = self._db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0]
        app_id = self._db.execute("PRAGMA application_id").fetchone()[0]
        version = self._db.execute("PRAGMA user_version").fetchone()[0]
        if not tables:
            if app_id or version:
                raise StateError("Unrecognized empty database metadata")
            return False
        if app_id != self.APPLICATION_ID or version != self.SCHEMA_VERSION:
            raise StateError(
                f"Not a supported {self.LABEL} database (state of another "
                "version of the connector?): use another --state-db"
            )
        return True

    def _create_schema(self, schema: str) -> None:
        self._db.executescript(
            f"BEGIN IMMEDIATE;"
            f"PRAGMA application_id={self.APPLICATION_ID};"
            f"PRAGMA user_version={self.SCHEMA_VERSION};"
            f"{outbox.SCHEMA}{schema}COMMIT;"
        )

    # Outbox (DeliveryStore) -----------------------------------------------------

    def pending_deliveries(self) -> tuple[Delivery, ...]:
        return outbox.pending(self._db, self.now_ms)

    def find_delivery(self, interaction_key: str) -> Delivery | None:
        return outbox.find(self._db, interaction_key)

    def delivery_paused(self) -> bool:
        return outbox.paused(self._db, self.now_ms)

    def delivery_counts(self) -> dict[str, int]:
        return outbox.counts(self._db)

    def begin_attempt(self, interaction_key: str) -> int:
        with self._transaction():
            return outbox.begin_attempt(self._db, interaction_key)

    def finish_attempt(
        self,
        attempt_id: int,
        outcome: Outcome,
        *,
        http_status: int | None = None,
        retry_after_seconds: int = 60,
        finished_ms: int | None = None,
    ) -> None:
        completed_ms = self.now_ms if finished_ms is None else finished_ms
        validate_ms(completed_ms)
        with self._transaction():
            outbox.finish_attempt(
                self._db,
                attempt_id,
                outcome,
                http_status=http_status,
                retry_after_seconds=retry_after_seconds,
                completed_ms=completed_ms,
            )

    def _retention_cutoff(self, retention_seconds: int) -> int:
        if type(retention_seconds) is not int or retention_seconds < 0:
            raise ValueError("Content retention must be nonnegative integer seconds")
        return self.now_ms - retention_seconds * 1000


class _Transaction:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def __enter__(self) -> None:
        self.connection.execute("BEGIN IMMEDIATE")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.connection.execute("COMMIT" if exc_type is None else "ROLLBACK")
