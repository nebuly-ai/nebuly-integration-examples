"""The outbox: what is queued for Nebuly and what happened to each POST.

Both connectors use these tables and rules (see ``delivery`` for the states):
``pending`` -> ``sending`` (committed before the POST) -> ``sent`` or
``rejected``; a ``retryable`` outcome goes back to ``pending`` with a per-item
and a source-wide retry deadline.

These helpers only run SQL: the caller owns the connection and wraps every
call in a transaction. ``Store`` in ``store.py`` does that.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .delivery import (
    MIN_LATER_RETRY_SECONDS,
    RETRYABLE_4XX,
    Delivery,
    DeliveryStatus,
    Outcome,
)
from .errors import StateError

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Mapping

SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox(
  interaction_key TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN
    ('pending','sending','sent','rejected')));
CREATE INDEX IF NOT EXISTS outbox_status ON outbox(status, interaction_key);
CREATE TABLE IF NOT EXISTS attempts(
  id INTEGER PRIMARY KEY,
  interaction_key TEXT NOT NULL REFERENCES outbox(interaction_key),
  outcome TEXT,
  http_status INTEGER,
  recovered INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS attempts_by_interaction ON attempts(interaction_key);
CREATE TABLE IF NOT EXISTS delivery_policy(
  interaction_key TEXT PRIMARY KEY REFERENCES outbox(interaction_key),
  finalized_ms INTEGER,
  next_attempt_ms INTEGER NOT NULL DEFAULT 0,
  failing_since_ms INTEGER);
CREATE TABLE IF NOT EXISTS operations(
  name TEXT PRIMARY KEY,
  value INTEGER NOT NULL);
"""
DELIVERY_NOT_BEFORE = "delivery_not_before"
MAX_BACKOFF_SECONDS = 6 * 3600
"""A payload Nebuly keeps failing (5xx, timeout) is retried at most this rarely."""
GIVE_UP_SECONDS = 7 * 86400
"""A payload with ambiguous failures spanning this long is given up."""


class OutboxError(StateError):
    """Outbox transition violation."""


def serialize(payload: Mapping[str, object]) -> str:
    serialized = json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    # SQLite and HTTP both require valid UTF-8, unlike Python strings.
    serialized.encode("utf-8")
    return serialized


def recover_attempts(db: sqlite3.Connection, now_ms: int) -> None:
    """A ``sending`` left by a crash is sent again, a minute later at the
    earliest like any POST Nebuly may have received: Nebuly updates a copy."""
    db.execute(
        "UPDATE attempts SET outcome='retryable',recovered=1 WHERE outcome IS NULL"
    )
    db.execute(
        "UPDATE delivery_policy SET next_attempt_ms=MAX(next_attempt_ms,?) "
        "WHERE interaction_key IN (SELECT interaction_key FROM outbox "
        "WHERE status='sending')",
        (now_ms + MIN_LATER_RETRY_SECONDS * 1000,),
    )
    db.execute("UPDATE outbox SET status='pending' WHERE status='sending'")


def put(
    db: sqlite3.Connection, interaction_key: str, payload: Mapping[str, object]
) -> None:
    """Queue a payload or replace a pending payload after caller validation."""
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


def requeue(
    db: sqlite3.Connection, interaction_key: str, payload: Mapping[str, object]
) -> None:
    """Queue a corrected payload of a delivered item, to send it again. Nebuly
    updates the interaction, so it waits like any POST it may still process."""
    cursor = db.execute(
        "UPDATE outbox SET payload=?,status='pending' WHERE interaction_key=? "
        "AND status IN ('sent','rejected')",
        (serialize(payload), interaction_key),
    )
    if cursor.rowcount != 1:
        raise OutboxError("Only a delivered item can be sent again")
    db.execute(
        "UPDATE delivery_policy SET next_attempt_ms=COALESCE(finalized_ms,0)+?,"
        "finalized_ms=NULL,failing_since_ms=NULL WHERE interaction_key=?",
        (MIN_LATER_RETRY_SECONDS * 1000, interaction_key),
    )


def find(db: sqlite3.Connection, interaction_key: str) -> Delivery | None:
    row = db.execute(
        "SELECT status,payload FROM outbox WHERE interaction_key=?", (interaction_key,)
    ).fetchone()
    return (
        None if row is None else Delivery(interaction_key, row[0], json.loads(row[1]))
    )


def was_attempted(db: sqlite3.Connection, interaction_key: str) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM attempts WHERE interaction_key=? LIMIT 1",
            (interaction_key,),
        ).fetchone()
        is not None
    )


def gave_up(db: sqlite3.Connection, interaction_key: str) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM outbox JOIN delivery_policy USING(interaction_key) "
            "WHERE interaction_key=? AND status='rejected' "
            "AND failing_since_ms IS NOT NULL",
            (interaction_key,),
        ).fetchone()
        is not None
    )


def retry_given_up(db: sqlite3.Connection, interaction_key: str) -> None:
    """Keep the original payload and retry deadline floor after giving up."""
    cursor = db.execute(
        "UPDATE outbox SET status='pending' WHERE interaction_key=? "
        "AND status='rejected' AND payload!='{}' AND EXISTS ("
        "SELECT 1 FROM delivery_policy p WHERE "
        "p.interaction_key=outbox.interaction_key AND p.failing_since_ms IS NOT NULL)",
        (interaction_key,),
    )
    if cursor.rowcount != 1:
        raise OutboxError("No retained delivery was given up for that interaction key")
    db.execute(
        "UPDATE delivery_policy SET "
        "next_attempt_ms=MAX(next_attempt_ms,finalized_ms+?),"
        "finalized_ms=NULL,failing_since_ms=NULL WHERE interaction_key=?",
        (MIN_LATER_RETRY_SECONDS * 1000, interaction_key),
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
        # Items that keep failing go last, so one bad payload never holds
        # the others.
        + " ORDER BY (SELECT failing_since_ms IS NOT NULL FROM delivery_policy p "
        "WHERE p.interaction_key=outbox.interaction_key),"
        "json_extract(payload,'$.interaction.time_start'),interaction_key",
        (now_ms, *parameters),
    )
    return tuple(Delivery(row[0], row[1], json.loads(row[2])) for row in rows)


def paused(db: sqlite3.Connection, now_ms: int) -> bool:
    """True while an earlier 429 or connection failure holds pending items.
    A 5xx or a timeout waits only for its own item."""
    cooldown = db.execute(
        "SELECT value FROM operations WHERE name=?", (DELIVERY_NOT_BEFORE,)
    ).fetchone()
    if cooldown is None or now_ms >= cooldown[0]:
        return False
    waiting = db.execute("SELECT 1 FROM outbox WHERE status='pending' LIMIT 1")
    return waiting.fetchone() is not None


def waiting_for_retry(
    db: sqlite3.Connection,
    now_ms: int,
    *,
    where: str = "",
    parameters: tuple[object, ...] = (),
) -> bool:
    """Whether a scoped pending delivery is held by an item or source deadline."""
    extra = f" AND ({where})" if where else ""
    waiting = db.execute(
        "SELECT 1 FROM outbox WHERE status='pending' "  # noqa: S608 - fixed SQL from caller
        "AND (EXISTS (SELECT 1 FROM delivery_policy p WHERE "
        "p.interaction_key=outbox.interaction_key AND p.next_attempt_ms>?) "
        "OR EXISTS (SELECT 1 FROM operations WHERE name=? AND value>?))"
        + extra
        + " LIMIT 1",
        (now_ms, DELIVERY_NOT_BEFORE, now_ms, *parameters),
    )
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
    if outcome not in {"sent", "rejected", "retryable"}:
        raise ValueError("Invalid delivery outcome")
    if http_status is not None and not 100 <= http_status <= 599:
        raise ValueError("Invalid HTTP status")
    if outcome == "sent" and (http_status is None or not 200 <= http_status < 300):
        raise ValueError("Sent requires a confirmed 2xx response")
    permanent = (
        http_status is not None
        and 400 <= http_status < 500
        and http_status not in RETRYABLE_4XX
    )
    if outcome == "rejected" and not permanent:
        raise ValueError("Rejected requires an explicit permanent 4xx response")
    if outcome == "retryable" and (
        permanent
        or (http_status is not None and 200 <= http_status < 300)
        or retry_after_seconds < 0
    ):
        raise ValueError("A confirmed or permanently rejected POST is not retryable")


def finish_attempt(
    db: sqlite3.Connection,
    attempt_id: int,
    outcome: Outcome,
    *,
    http_status: int | None,
    retry_after_seconds: int,
    completed_ms: int,
    maybe_received: bool = False,
) -> DeliveryStatus:
    """Record the outcome and return the item's new status.

    A ``retryable`` that did not reach Nebuly (429, connection) pauses the
    whole source until its retry time. One that may have (5xx, timeout) waits
    only for its own item, longer each time (the time it has been failing,
    from one minute to ``MAX_BACKOFF_SECONDS``), and after
    ``GIVE_UP_SECONDS`` of failures the item is given up as ``rejected``.
    """
    validate_outcome(outcome, http_status, retry_after_seconds)
    row = db.execute(
        "SELECT a.interaction_key,a.outcome,p.failing_since_ms FROM attempts a "
        "JOIN delivery_policy p USING(interaction_key) WHERE a.id=?",
        (attempt_id,),
    ).fetchone()
    if row is None or row[1] is not None:
        raise OutboxError("Attempt is unknown or already finished")
    key, failing_since = row[0], row[2]
    status: DeliveryStatus = "pending" if outcome == "retryable" else outcome
    retry_at = completed_ms + retry_after_seconds * 1000
    if outcome == "retryable" and maybe_received:
        failing_since = completed_ms if failing_since is None else failing_since
        failing_ms = completed_ms - failing_since
        if failing_ms >= GIVE_UP_SECONDS * 1000:
            status = "rejected"
        retry_at = max(
            retry_at, completed_ms + min(failing_ms, MAX_BACKOFF_SECONDS * 1000)
        )
    elif outcome != "retryable":
        failing_since = None
    cursor = db.execute(
        "UPDATE outbox SET status=? WHERE interaction_key=? AND status='sending'",
        (status, key),
    )
    if cursor.rowcount != 1:
        raise OutboxError("Delivery is not awaiting an attempt outcome")
    db.execute(
        "UPDATE attempts SET outcome=?,http_status=? WHERE id=?",
        (outcome, http_status, attempt_id),
    )
    db.execute(
        "UPDATE delivery_policy SET finalized_ms=?,next_attempt_ms=?,"
        "failing_since_ms=? WHERE interaction_key=?",
        (
            completed_ms if status != "pending" else None,
            retry_at if status == "pending" else 0,
            failing_since,
            key,
        ),
    )
    if outcome == "retryable" and not maybe_received:
        db.execute(
            "INSERT INTO operations VALUES (?,?) "
            "ON CONFLICT(name) DO UPDATE SET value=MAX(value,excluded.value)",
            (DELIVERY_NOT_BEFORE, retry_at),
        )
    return status


def compact(db: sqlite3.Connection, cutoff_ms: int) -> int:
    """Drop the payload of items finalized before ``cutoff_ms``; keep the status."""
    cursor = db.execute(
        "UPDATE outbox SET payload='{}' WHERE payload!='{}' "
        "AND status IN ('sent','rejected') AND interaction_key IN "
        "(SELECT interaction_key FROM delivery_policy WHERE finalized_ms<=?)",
        (cutoff_ms,),
    )
    return cursor.rowcount


def purge(db: sqlite3.Connection, cutoff_ms: int) -> int:
    """Delete the items sent or rejected before ``cutoff_ms``, with their
    attempts. Returns how many."""
    keys = [
        key
        for (key,) in db.execute(
            "SELECT interaction_key FROM outbox JOIN delivery_policy "
            "USING(interaction_key) WHERE status IN ('sent','rejected') "
            "AND finalized_ms<=?",
            (cutoff_ms,),
        )
    ]
    delete(db, keys)
    return len(keys)


def delete(db: sqlite3.Connection, keys: Iterable[str]) -> None:
    """Delete these items and their attempts, whatever their status."""
    rows = [(key,) for key in keys]
    for table in ("attempts", "delivery_policy", "outbox"):
        db.executemany(
            f"DELETE FROM {table} WHERE interaction_key=?",  # noqa: S608
            rows,
        )
