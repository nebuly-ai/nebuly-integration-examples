from __future__ import annotations

import pytest

from bedrock_sync.agentcore.converter import pseudonymize_actor
from bedrock_sync.shared.keys import canonical_key, digest, pseudonymize


def test_canonical_key_has_no_delimiter_ambiguity() -> None:
    assert canonical_key("a,b", "c") != canonical_key("a", "b,c")


def test_pseudonym_depends_on_namespace_and_matches_agentcore() -> None:
    assert pseudonymize("one", "u", "s") != pseudonymize("two", "u", "s")
    assert pseudonymize("agentcore.actor", "u", "s") == pseudonymize_actor("u", "s")
    with pytest.raises(ValueError, match="secret"):
        pseudonymize("one", "u", " ")


def test_digest_is_stable() -> None:
    assert digest("x", 1) == digest("x", 1)
    assert digest("x", 1) != digest("x", 2)
