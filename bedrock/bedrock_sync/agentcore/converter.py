"""Convert assembled interactions without inference or price lookup; every
text is cut with ``user_defined.truncate``.

An interaction may be sent with warnings (``assembler.BLOCKING``): the parts
that could not be read are left out, never invented.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from bedrock_sync.shared.keys import pseudonymize
from bedrock_sync.shared.model_names import observed_model_name
from bedrock_sync.shared.time_utils import datetime_to_timestamp_str

from . import user_defined

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .config import AgentCoreConfig
    from .models import InteractionSnapshot, ModelCall
    from .records import SourceScope


def _time(ns: int) -> str:
    value = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=ns // 1000)
    return datetime_to_timestamp_str(value)


END_USER_NAMESPACE = "agentcore.actor"


def pseudonymize_actor(actor: str, secret: str) -> str:
    """HMAC of the actor id, keeping its exact spelling (``shared/keys.py``)."""
    return pseudonymize(END_USER_NAMESPACE, actor, secret)


def _model_traces(
    interaction: InteractionSnapshot,
) -> list[dict[str, Any]]:
    input_text = interaction.input or ""
    by_model: dict[str, list[ModelCall]] = defaultdict(list)
    for call in interaction.model_calls:
        by_model[call.model_id].append(call)
    traces = []
    for model, calls in sorted(by_model.items()):
        for call in calls:
            counts = (
                call.usage.input_tokens,
                call.usage.output_tokens,
                call.usage.cache_read_input_tokens,
                call.usage.cache_write_input_tokens,
            )
            if any(type(count) is not int or count < 0 for count in counts):
                raise ValueError("Model usage must be nonnegative integers")
        totals = {
            name: sum(getattr(call.usage, name) for call in calls)
            for name in (
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_write_input_tokens",
            )
        }
        output = next(
            (
                call.output
                for call in sorted(
                    calls, key=lambda call: (call.end_ns, call.span_id), reverse=True
                )
                if call.output is not None
            ),
            "",
        )
        traces.append(
            {
                "model": observed_model_name(model),
                "messages": [
                    {"role": "user", "content": user_defined.truncate(input_text)}
                ],
                "output": user_defined.truncate(output),
                "input_tokens": totals["input_tokens"],
                "output_tokens": totals["output_tokens"],
                "cache_read_input_tokens": totals["cache_read_input_tokens"],
                "cache_write_input_tokens": totals["cache_write_input_tokens"],
            }
        )
    return traces


def _tool_text(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def snapshot_to_payload(
    interaction: InteractionSnapshot,
    *,
    source: SourceScope,
    config: AgentCoreConfig,
    context: Mapping[str, object],
) -> dict[str, Any]:
    if (
        interaction.readiness != "ready"
        or interaction.end_ns is None
        or interaction.actor_id is None
    ):
        raise ValueError("Only ended, identified interactions can be converted")
    if interaction.interaction_key != source.interaction_key(
        interaction.trace_id, interaction.root_span_id
    ):
        raise ValueError("Interaction belongs to a different source")
    if interaction.end_ns < interaction.start_ns:
        raise ValueError("Invalid interaction interval")
    if not interaction.input and not interaction.output:
        raise ValueError("Interaction must contain input or output")
    actor = user_defined.end_user_id(interaction, context)
    if not isinstance(actor, str) or not actor.strip():
        raise ValueError("Customer end-user hook must return a nonblank string")
    tags = user_defined.build_tags(interaction, context)
    if not all(
        isinstance(key, str) and isinstance(value, str) for key, value in tags.items()
    ):
        raise ValueError("Tags must be strings")
    status = interaction.terminal_status
    tags = {
        **tags,
        "agentcore.status": status.lower() if status in {"OK", "ERROR"} else "unknown",
    }
    segments = user_defined.build_segments(interaction, context)
    if any(key not in {"team", "market", "tenure"} for key in segments):
        raise ValueError("Unsupported segment field")
    if any(
        value is not None and (not isinstance(value, str) or not value.strip())
        for value in segments.values()
    ):
        raise ValueError("Segments must be nonblank strings or None")
    traces = _model_traces(interaction)
    traces.extend(
        {
            "source": tool.name,
            "input": user_defined.truncate(_tool_text(tool.input)),
            "outputs": []
            if tool.output is None
            else [user_defined.truncate(_tool_text(tool.output))],
        }
        for tool in sorted(
            interaction.tools, key=lambda tool: (tool.start_ns, tool.span_id)
        )
    )
    traces = user_defined.customize_traces(interaction, traces, context)
    return {
        "interaction": {
            "conversation_id": interaction.conversation_id,
            "input": user_defined.truncate(interaction.input or ""),
            "output": user_defined.truncate(interaction.output or ""),
            "time_start": _time(interaction.start_ns),
            "time_end": _time(interaction.end_ns),
            "end_user": pseudonymize_actor(actor, config.user_hash_secret)
            if config.pseudonymize_end_user
            else actor,
            "hide_content": False,
            "tags": tags,
            **{key: value for key, value in segments.items() if value is not None},
        },
        "traces": traces,
        "user_feedback": user_defined.build_user_feedback(interaction, context),
        "anonymize": config.anonymize,
    }
