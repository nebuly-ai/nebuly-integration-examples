"""Extract actor IDs from configured Memory namespace paths, never API reads."""

from __future__ import annotations

import re
from functools import lru_cache

DEFAULT_ACTOR_NAMESPACE = "/actors/{actorId}/"
_RETRIEVAL = re.compile(r"^Retrieved \d+ memories from namespace: (.+)$")
_VARIABLE = re.compile(r"\{[A-Za-z][A-Za-z0-9_]*\}")


@lru_cache(maxsize=32)
def namespace_pattern(template: str) -> re.Pattern[str]:
    """Accept path templates, not arbitrary customer regular expressions."""
    parts = template.split("/")
    if not template.startswith("/") or parts.count("{actorId}") != 1:
        raise ValueError("Actor namespace must contain one full {actorId} path segment")
    encoded = []
    for part in parts:
        if part == "{actorId}":
            encoded.append(r"(?P<actor>[^/]+)")
        elif _VARIABLE.fullmatch(part):
            encoded.append(r"[^/]+")
        elif "{" in part or "}" in part:
            raise ValueError("Namespace variables must occupy complete path segments")
        else:
            encoded.append(re.escape(part))
    return re.compile(
        "^" + "/".join(encoded) + ("" if template.endswith("/") else r"(?=/|$)")
    )


def actor_from_message(
    message: str, template: str = DEFAULT_ACTOR_NAMESPACE
) -> str | None:
    retrieval = _RETRIEVAL.fullmatch(message)
    if retrieval is None:
        return None
    match = namespace_pattern(template).match(retrieval[1])
    return match["actor"] if match is not None else None
