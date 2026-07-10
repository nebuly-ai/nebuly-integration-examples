from __future__ import annotations

import hashlib
import hmac
import uuid


def pseudonymize_email(email: str | None, *, secret: str) -> str | None:
    """Return a deterministic UUID-shaped pseudonym for an email using keyed
    HMAC-SHA256. Returns None for a missing email. Same email + same secret
    always yields the same UUID, with no stored state."""
    if not email:
        return None
    normalized = email.strip().lower()
    if not normalized:
        return None
    digest = hmac.new(
        secret.encode("utf-8"), normalized.encode("utf-8"), hashlib.sha256
    ).digest()
    return str(uuid.UUID(bytes=digest[:16], version=5))
