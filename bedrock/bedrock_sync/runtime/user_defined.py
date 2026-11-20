"""The parts of the conversion a customer is expected to adapt.

Same hooks as the AgentCore connector. ``load_context`` runs once per sync:
load customer mappings there, never per interaction. The end user is the
``user_id`` of the triplet; with PSEUDONYMIZE_END_USER it is pseudonymized
after these hooks have seen it.
The connector cannot infer team, market or tenure: return them from
``build_segments`` only if the customer provides them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .formats.base import canonical_json
from .grouping import RESERVED_METADATA_KEYS
from .models import BlockKind, Message, tool_loop

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .grouping import Interaction
    from .models import Block, Invocation

MAX_CONTENT_CHARS = 8000
"""Same cap as the compliance integrations, applied to every text we send."""


def load_context() -> Mapping[str, object]:
    """Load customer mappings here; do not do per-interaction remote lookups."""
    return {}


def truncate(text: str) -> str:
    return text[:MAX_CONTENT_CHARS]


def end_user_id(interaction: Interaction, context: Mapping[str, object]) -> str:
    """The end user sent to Nebuly. One conversation still belongs to one raw
    ``user_id``: mapping two ids to the same user does not merge them."""
    del context
    return interaction.triplet.user_id


def tool_calls(invocation: Invocation) -> list[tuple[Block, str | None]]:
    """Every tool call of the turn as the history of ``invocation`` shows it, in
    order, with its result (None while unanswered).

    Given the final call, this is every tool of the main agent; a sub-agent
    launched through a tool is one of them, with its answer as the result.
    Each result is paired with the call of its own round: some models reuse
    the same id (``functions.search:0``) in every round.
    """
    rounds = [m for m in tool_loop(invocation.messages) if m.blocks]
    rounds.append(Message("assistant", invocation.tool_uses))
    calls: list[tuple[Block, str | None]] = []
    pending: list[int] = []
    for message in rounds:
        if message.role == "assistant":
            pending = []
            for block in message.blocks:
                if block.kind is BlockKind.TOOL_USE:
                    pending.append(len(calls))
                    calls.append((block, None))
            continue
        for block in message.blocks:
            if block.kind is not BlockKind.TOOL_RESULT:
                continue
            index = next(
                (
                    i
                    for i in pending
                    if calls[i][1] is None
                    and calls[i][0].tool_use_id == block.tool_use_id
                ),
                None,
            )
            if index is not None:
                calls[index] = (calls[index][0], block.text)
    return calls


def model_name(model: str, context: Mapping[str, object]) -> str:
    """The model sent to Nebuly, used for cost and model statistics. ``model``
    is already normalized (``claude-haiku-4-5``). An application inference
    profile logs only its ARN, so ``model`` is the profile id at its end
    (``abc123``): map it to the model behind it, for example with a mapping
    loaded in ``load_context``::

        return context.get("profiles", {}).get(model, model)
    """
    del context
    return model


def build_tags(
    interaction: Interaction,
    context: Mapping[str, object],
    *,
    model: str,
    models: list[str],
) -> dict[str, str]:
    """The model of the answer (and every model if more than one), the AWS
    account, the tools used, plus the caller's other ``requestMetadata`` keys,
    which are tags of its choice."""
    del context
    first = interaction.first
    tags: dict[str, str] = {"model": model, "aws account": first.account_id}
    if len(models) > 1:
        tags["models"] = ",".join(models)
    final = interaction.final
    tools = (
        sorted({tool.tool_name for tool, _ in tool_calls(final) if tool.tool_name})
        if final
        else []
    )
    if tools:
        tags["tools used"] = ",".join(tools)
    for invocation in interaction.main_calls or interaction.invocations:
        for key, value in invocation.request_metadata.items():
            if key not in RESERVED_METADATA_KEYS and value:
                tags.setdefault(key, value)
    return tags


def build_segments(
    interaction: Interaction, context: Mapping[str, object]
) -> dict[str, str | None]:
    del interaction, context
    return {}


def trace_cost(invocations: list[Invocation]) -> int | None:  # noqa: ARG001
    """Cost of these calls in micro-dollars, or None to let Nebuly compute it
    from the model and the tokens."""
    return None


def build_retrieval_traces(interaction: Interaction) -> list[dict[str, Any]]:
    """One retrieval trace per answered tool call of the main agent."""
    final = interaction.final
    if final is None:
        return []
    return [
        {
            "source": tool.tool_name or "tool",
            "input": truncate(canonical_json(tool.tool_input)),
            "outputs": [truncate(result)],
        }
        for tool, result in tool_calls(final)
        if result is not None
    ]


def customize_traces(
    interaction: Interaction,
    traces: list[dict[str, Any]],
    context: Mapping[str, object],
) -> list[dict[str, Any]]:
    del interaction, context
    return traces


def build_user_feedback(
    interaction: Interaction, context: Mapping[str, object]
) -> list[dict[str, Any]]:
    """Bedrock logs carry no feedback."""
    del interaction, context
    return []
