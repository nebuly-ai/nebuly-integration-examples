"""Unambiguous composite keys and keyed pseudonyms shared by both sources."""

from __future__ import annotations

import hashlib
import hmac
import json


def canonical_key(*parts: object) -> str:
    """Encode opaque identifiers without delimiter ambiguity."""
    return json.dumps(parts, ensure_ascii=False, separators=(",", ":"))


def digest(*parts: object) -> str:
    """SHA-256 of ``canonical_key(*parts)``: stores identity without the value."""
    return hashlib.sha256(canonical_key(*parts).encode()).hexdigest()


def pseudonymize(namespace: str, value: str, secret: str) -> str:
    """HMAC-SHA256 with a versioned namespace, keeping the value's exact spelling.

    The namespace keeps the same raw id coming from two sources from producing
    the same pseudonym by accident.
    """
    if not secret.strip():
        raise ValueError("A pseudonymization secret is required")
    return hmac.new(
        secret.encode(), canonical_key(namespace, 1, value).encode(), hashlib.sha256
    ).hexdigest()
