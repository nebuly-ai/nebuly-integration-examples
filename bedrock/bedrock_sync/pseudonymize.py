from __future__ import annotations

import hashlib
import hmac
import uuid


def pseudonymize_user(user_id: str | None, *, secret: str) -> str | None:
    """Return a deterministic UUID-shaped pseudonym for a user identifier
    (an IAM principal ARN or a requestMetadata user id) using keyed
    HMAC-SHA256. Returns None for a missing identifier. Same identifier +
    same secret always yields the same UUID, with no stored state."""
    if not user_id:
        return None
    normalized = user_id.strip().lower()
    if not normalized:
        return None
    digest = hmac.new(
        secret.encode("utf-8"), normalized.encode("utf-8"), hashlib.sha256
    ).digest()
    return str(uuid.UUID(bytes=digest[:16], version=5))
