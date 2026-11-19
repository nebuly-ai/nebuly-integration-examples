"""Turn a closed interaction into the Nebuly trace-interaction payload.

- input: the user's words in the first call not marked as sub-agent;
- output: the text of the final call (the last one not marked as
  sub-agent), its text blocks joined;
- one LLM trace per model with the tokens of every call, sub-agents included:
  ``input_tokens`` without the cached prompt, cache read and write as extra
  keys, as in the AgentCore connector;
- one retrieval trace per answered tool call of the final call's history.
"""

from __future__ import annotations

from collections import defaultdict
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from bedrock_sync.shared.keys import pseudonymize
from bedrock_sync.shared.model_names import nebuly_model_name
from bedrock_sync.shared.time_utils import datetime_to_timestamp_str

from . import user_defined
from .models import join_text, turn_start

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .grouping import Interaction
    from .models import Invocation

END_USER_NAMESPACE = "bedrock.runtime.user"


class InteractionSkip(StrEnum):
    """Why a closed interaction was not queued."""

    NO_MAIN_CALL = "no_main_call"
    """Every call is marked as sub-agent: no call holds the answer."""
    EMPTY_INTERACTION = "empty_interaction"
    """No input and no output: the API accepts neither."""


def _model_name(invocation: Invocation, context: Mapping[str, object]) -> str:
    name = user_defined.model_name(nebuly_model_name(invocation.model), context)
    if not isinstance(name, str):
        raise TypeError("The model name hook must return a string")
    return name.strip() or "unknown"


def _user_input(invocation: Invocation) -> str:
    """The text of the message that opened the turn."""
    start = turn_start(invocation.messages)
    return "" if start is None else user_defined.truncate(join_text(start.blocks))


def _llm_traces(
    interaction: Interaction, user_input: str, context: Mapping[str, object]
) -> list[dict[str, Any]]:
    by_model: dict[str, list[Invocation]] = defaultdict(list)
    for invocation in interaction.invocations:
        by_model[_model_name(invocation, context)].append(invocation)
    traces = []
    for model, invocations in sorted(by_model.items()):
        output = next(
            (
                text
                for invocation in reversed(invocations)
                if (text := invocation.output_text.strip())
            ),
            "",
        )
        trace: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": user_input}],
            "output": user_defined.truncate(output),
            "input_tokens": sum(i.usage.input_tokens for i in invocations),
            "output_tokens": sum(i.usage.output_tokens for i in invocations),
            "cache_read_input_tokens": sum(
                i.usage.cache_read_tokens for i in invocations
            ),
            "cache_write_input_tokens": sum(
                i.usage.cache_write_tokens for i in invocations
            ),
        }
        if (cost := user_defined.trace_cost(invocations)) is not None:
            trace["cost"] = cost
        traces.append(trace)
    return traces


def interaction_to_payload(
    interaction: Interaction,
    *,
    anonymize: bool,
    pseudonymize_end_user: bool,
    user_hash_secret: str,
    context: Mapping[str, object] | None = None,
) -> dict[str, Any] | InteractionSkip:
    context = {} if context is None else context
    final = interaction.final
    if final is None:
        return InteractionSkip.NO_MAIN_CALL
    user_input = _user_input(interaction.main_calls[0])
    output = user_defined.truncate(final.output_text.strip())
    if not user_input and not output:
        return InteractionSkip.EMPTY_INTERACTION

    raw_user = user_defined.end_user_id(interaction, context)
    if not isinstance(raw_user, str) or not raw_user.strip():
        raise ValueError("The end-user hook must return a nonblank string")
    end_user = (
        pseudonymize(END_USER_NAMESPACE, raw_user, user_hash_secret)
        if pseudonymize_end_user
        else raw_user
    )
    models = list(
        dict.fromkeys(_model_name(i, context) for i in interaction.invocations)
    )
    tags = user_defined.build_tags(
        interaction, context, model=_model_name(final, context), models=models
    )
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in tags.items()):
        raise ValueError("Tags must be strings")
    segments = user_defined.build_segments(interaction, context)
    if any(key not in {"team", "market", "tenure"} for key in segments):
        raise ValueError("Unsupported segment field")

    traces = _llm_traces(interaction, user_input, context)
    traces.extend(user_defined.build_retrieval_traces(interaction))
    traces = user_defined.customize_traces(interaction, traces, context)
    return {
        "interaction": {
            "conversation_id": interaction.triplet.conversation_id,
            "input": user_input,
            "output": output,
            "time_start": datetime_to_timestamp_str(interaction.time_start),
            "time_end": datetime_to_timestamp_str(interaction.time_end),
            "end_user": end_user,
            "hide_content": False,
            "tags": tags,
            **{key: value for key, value in segments.items() if value},
        },
        "traces": traces,
        "user_feedback": user_defined.build_user_feedback(interaction, context),
        "anonymize": anonymize,
    }
