"""Customer enrichment hooks over raw actor IDs and normalized interactions.

Load optional customer mappings once per run in load_context. Return only explicit
customer-provided segments; the connector cannot infer team, tenure or market.
End-user pseudonymization happens after these hooks have seen the native actor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .models import InteractionSnapshot


def load_context() -> Mapping[str, object]:
    """Load customer mappings here; do not do per-interaction remote lookups."""
    return {}


def end_user_id(
    interaction: InteractionSnapshot,
    context: Mapping[str, object],
) -> str:
    """The end user sent to Nebuly. One session still belongs to one raw
    actor id: mapping two actors to the same user does not merge sessions."""
    del context
    return interaction.actor_id or ""


def build_tags(
    interaction: InteractionSnapshot,
    context: Mapping[str, object],
) -> dict[str, str]:
    """The tools used. The connector always adds ``agentcore.status`` (ok or
    error) after this hook."""
    del context
    tags: dict[str, str] = {}
    if interaction.tools:
        tags["tools used"] = ",".join(sorted({tool.name for tool in interaction.tools}))
    return tags


def build_segments(
    interaction: InteractionSnapshot,
    context: Mapping[str, object],
) -> dict[str, str | None]:
    del interaction, context
    return {}


def build_user_feedback(
    interaction: InteractionSnapshot,
    context: Mapping[str, object],
) -> list[dict[str, Any]]:
    del interaction, context
    return []


def customize_traces(
    interaction: InteractionSnapshot,
    traces: list[dict[str, Any]],
    context: Mapping[str, object],
) -> list[dict[str, Any]]:
    del interaction, context
    return traces
