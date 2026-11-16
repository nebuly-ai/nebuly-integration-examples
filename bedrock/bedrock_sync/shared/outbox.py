"""The outbox: what is queued for Nebuly and what happened to each POST.

Both connectors use these tables and rules (see ``delivery`` for the states):
``pending`` -> ``sending`` (committed before the POST) -> ``sent``,
``rejected`` or ``uncertain``; a ``retryable`` outcome goes back to
``pending`` with a per-item and a source-wide retry deadline.

These helpers only run SQL: the caller owns the connection and wraps every
call in a transaction. ``Store`` in ``store.py`` does that.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .delivery import NOT_INGESTED_STATUSES, Delivery, Outcome
from .errors import StateError

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Mapping

SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox(
  interaction_key TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN
    ('pending','sending','sent','rejected','uncertain')));
CREATE INDEX IF NOT EXISTS outbox_status ON outbox(status, interaction_key);
CREATE TABLE IF NOT EXISTS attempts(
  id INTEGER PRIMARY KEY,
  interaction_key TEXT NOT NULL REFERENCES outbox(interaction_key),
  outcome TEXT,
  http_status INTEGER,
  recovered INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS delivery_policy(
  interaction_key TEXT PRIMARY KEY REFERENCES outbox(interaction_key),
  finalized_ms INTEGER,
  next_attempt_ms INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS operations(
  name TEXT PRIMARY KEY,
  value INTEGER NOT NULL);
"""
DELIVERY_NOT_BEFORE = "delivery_not_before"


class OutboxError(StateError):
    """Outbox transition violation."""


def serialize(payload: Mapping[str, object]) -> str:
    return json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def recover_attempts(db: sqlite3.Connection) -> None:
    """A ``sending`` left by a crash may have reached Nebuly: never replay it."""
    db.execute(
        "UPDATE attempts SET outcome='uncertain',recovered=1 WHERE outcome IS NULL"
    )
    db.execute("UPDATE outbox SET status='uncertain' WHERE status='sending'")


def put(
    db: sqlite3.Connection, interaction_key: str, payload: Mapping[str, object]
) -> None:
    """Queue a payload; a pending one may be replaced, an attempted one never."""
    serialized = serialize(payload)
    row = db.execute(
        "SELECT payload,status FROM outbox WHERE interaction_key=?", (interaction_key,)
    ).fetchone()
    if row is not None:
        if row[0] != serialized:
            if row[1] != "pending":
                raise OutboxError("Attempted delivery payload cannot be replaced")
            db.execute(
                "UPDATE outbox SET payload=? WHERE interaction_key=?",
                (serialized, interaction_key),
            )
        return
    db.execute(
        "INSERT INTO outbox VALUES (?,?,'pending')", (interaction_key, serialized)
    )
    db.execute(
        "INSERT INTO delivery_policy(interaction_key) VALUES (?)", (interaction_key,)
    )


def find(db: sqlite3.Connection, interaction_key: str) -> Delivery | None:
    row = db.execute(
        "SELECT status,payload FROM outbox WHERE interaction_key=?", (interaction_key,)
    ).fetchone()
    return (
        None if row is None else Delivery(interaction_key, row[0], json.loads(row[1]))
    )


def pending(
    db: sqlite3.Connection,
    now_ms: int,
    *,
    where: str = "",
    parameters: tuple[object, ...] = (),
) -> tuple[Delivery, ...]:
    """Pending items whose retry deadline passed, oldest interaction first.

    ``where`` is an extra SQL condition on ``outbox`` with its ``parameters``.
    """
    if paused(db, now_ms):
        return ()
    extra = f" AND ({where})" if where else ""
    rows = db.execute(
        "SELECT interaction_key,status,payload FROM outbox WHERE status='pending' "  # noqa: S608 - fixed SQL from the caller
        "AND NOT EXISTS (SELECT 1 FROM delivery_policy p WHERE "
        "p.interaction_key=outbox.interaction_key AND p.next_attempt_ms>?)"
        + extra
        + " ORDER BY json_extract(payload,'$.interaction.time_start'),interaction_key",
        (now_ms, *parameters),
    )
    return tuple(Delivery(row[0], row[1], json.loads(row[2])) for row in rows)


def paused(db: sqlite3.Connection, now_ms: int) -> bool:
    """True while an earlier 429 or connection failure holds pending items."""
    cooldown = db.execute(
        "SELECT value FROM operations WHERE name=?", (DELIVERY_NOT_BEFORE,)
    ).fetchone()
    if cooldown is None or now_ms >= cooldown[0]:
        return False
    waiting = db.execute("SELECT 1 FROM outbox WHERE status='pending' LIMIT 1")
    return waiting.fetchone() is not None


def counts(db: sqlite3.Connection) -> dict[str, int]:
    return dict(db.execute("SELECT status,COUNT(*) FROM outbox GROUP BY status"))


def begin_attempt(db: sqlite3.Connection, interaction_key: str) -> int:
    cursor = db.execute(
        "UPDATE outbox SET status='sending' WHERE interaction_key=? "
        "AND status='pending'",
        (interaction_key,),
    )
    if cursor.rowcount != 1:
        raise OutboxError("Only pending deliveries can begin an attempt")
    cursor = db.execute(
        "INSERT INTO attempts(interaction_key) VALUES (?)", (interaction_key,)
    )
    if cursor.lastrowid is None:
        raise OutboxError("SQLite did not return an inserted row ID")
    return cursor.lastrowid


def validate_outcome(
    outcome: Outcome, http_status: int | None, retry_after_seconds: int
) -> None:
    if outcome not in {"sent", "rejected", "uncertain", "retryable"}:
        raise ValueError("Invalid delivery outcome")
    if http_status is not None and not 100 <= http_status <= 599:
        raise ValueError("Invalid HTTP status")
    if outcome == "sent" and (http_status is None or not 200 <= http_status < 300):
        raise ValueError("Sent requires a confirmed 2xx response")
    if outcome == "rejected" and (
        http_status is None or not 400 <= http_status < 500 or http_status in {408, 429}
    ):
        raise ValueError("Rejected requires an explicit permanent 4xx response")
    if outcome == "retryable" and (
        http_status not in {None, 429, *NOT_INGESTED_STATUSES}
        or retry_after_seconds < 0
    ):
        raise ValueError(
            "Only connection failure, rate limiting or a wrong key or endpoint "
            "is retryable"
        )


def finish_attempt(
    db: sqlite3.Connection,
    attempt_id: int,
    outcome: Outcome,
    *,
    http_status: int | None,
    retry_after_seconds: int,
    completed_ms: int,
) -> None:
    validate_outcome(outcome, http_status, retry_after_seconds)
    row = db.execute(
        "SELECT interaction_key,outcome FROM attempts WHERE id=?", (attempt_id,)
    ).fetchone()
    if row is None or row[1] is not None:
        raise OutboxError("Attempt is unknown or already finished")
    cursor = db.execute(
        "UPDATE outbox SET status=? WHERE interaction_key=? AND status='sending'",
        ("pending" if outcome == "retryable" else outcome, row[0]),
    )
    if cursor.rowcount != 1:
        raise OutboxError("Delivery is not awaiting an attempt outcome")
    db.execute(
        "UPDATE attempts SET outcome=?,http_status=? WHERE id=?",
        (outcome, http_status, attempt_id),
    )
    retry_at = completed_ms + retry_after_seconds * 1000
    db.execute(
        "UPDATE delivery_policy SET finalized_ms=?,next_attempt_ms=? "
        "WHERE interaction_key=?",
        (
            completed_ms if outcome in {"sent", "rejected"} else None,
            retry_at if outcome == "retryable" else 0,
            row[0],
        ),
    )
    if outcome == "retryable":
        db.execute(
            "INSERT INTO operations VALUES (?,?) "
            "ON CONFLICT(name) DO UPDATE SET value=MAX(value,excluded.value)",
            (DELIVERY_NOT_BEFORE, retry_at),
        )


def compact(db: sqlite3.Connection, cutoff_ms: int) -> int:
    """Drop the payload of items finalized before ``cutoff_ms``; keep the status."""
    cursor = db.execute(
        "UPDATE outbox SET payload='{}' WHERE payload!='{}' "
        "AND status IN ('sent','rejected') AND interaction_key IN "
        "(SELECT interaction_key FROM delivery_policy WHERE finalized_ms<=?)",
        (cutoff_ms,),
    )
    return cursor.rowcount
