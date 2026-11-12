"""Turn a grouped user turn into the Nebuly trace-interaction payload.

Same shape as the other integrations: one interaction per POST, with
``interaction``, ``traces``, ``user_feedback`` and ``anonymize``. The parts a
customer may adapt (end user, tags, traces, service-call rules) live in
``user_defined``.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from . import user_defined
from .config import datetime_to_timestamp_str
from .pseudonymize import pseudonymize_user

if TYPE_CHECKING:
    from .grouping import Turn
    from .models import Invocation

_PROVIDERS = (
    "anthropic",
    "openai",
    "amazon",
    "meta",
    "mistral",
    "cohere",
    "ai21",
    "deepseek",
    "qwen",
    "writer",
    "google",
    "moonshotai",
    "minimax",
    "nvidia",
    "zai",
)
_PROVIDER_RE = re.compile(rf"^({'|'.join(_PROVIDERS)})\.")
_NAME_WITHOUT_PROVIDER = frozenset({"deepseek"})
"""Providers whose Bedrock ids drop the family name: ``deepseek.r1-v1:0``."""
_BEDROCK_VERSION_RE = re.compile(r"(?:-v\d+(?::\d+)?|-\d+:\d+|:\d+)(?::\d+k|:mm)?$")
_DATE_RE = re.compile(r"-(?:\d{8}|\d{4}-\d{2}-\d{2})$")
_LEGACY_NAMES = {
    "anthropic.claude-v2": "claude-2.0",
    "anthropic.claude-v2:1": "claude-2.1",
    "anthropic.claude-instant-v1": "claude-instant-1.2",
}
"""Old ids whose ``-vN`` is the model version, not the Bedrock one."""


class SkipReason(StrEnum):
    SINGLE_TOKEN_PROBE = "single_token_probe"  # noqa: S105 - not a secret
    SESSION_TITLE = "session_title"
    WEB_FETCH_SUMMARY = "web_fetch_summary"
    SERVICE_CALL = "service_call"
    EMPTY_INPUT = "empty_input"
    PARTIAL_START = "partial_start"


def nebuly_model_name(model: str) -> str:
    """Bedrock model id -> the model name as Nebuly stores it: no provider,
    no Bedrock version, no release date.
    ``anthropic.claude-haiku-4-5-20251001-v1:0`` -> ``claude-haiku-4-5``,
    ``openai.gpt-oss-120b-1:0`` -> ``gpt-oss-120b``,
    ``deepseek.r1-v1:0`` -> ``deepseek-r1``; the context-window suffix of
    provisioned models (``:200k``) goes too. Expects the id already stripped
    of ARN and geo prefix (``Invocation.model``).

    An application inference profile logs only its own ARN, so the name is
    the profile id at its end (``abc123``): the model is not in the log.
    """
    if model in _LEGACY_NAMES:
        return _LEGACY_NAMES[model]
    provider_match = _PROVIDER_RE.match(model)
    name = _BEDROCK_VERSION_RE.sub("", _PROVIDER_RE.sub("", model))
    name = _DATE_RE.sub("", name)
    if provider_match and provider_match.group(1) in _NAME_WITHOUT_PROVIDER:
        return f"{provider_match.group(1)}-{name}"
    return name


def _model_name(invocation: Invocation) -> str:
    return nebuly_model_name(invocation.model) or "unknown"


def _user_input(turn: Turn) -> str:
    """The user's words that opened the turn.

    A ``partial_start`` turn opens on a tool result, but its history still
    holds the question: the latest user message that is not a tool result.
    """
    messages = turn.first.messages
    if not turn.partial_start:
        last = turn.first.last_message
        if last is None or last.role != "user":
            return ""
        return user_defined.truncate(user_defined.message_text(last))
    for message in reversed(messages):
        if message.role != "user" or message.tool_result_ids:
            continue
        if text := user_defined.message_text(message):
            return user_defined.truncate(text)
    return ""


def _assistant_output(turn: Turn) -> str:
    """The final answer; for a turn cut short (timed out) the last text the
    model wrote."""
    for invocation in reversed(turn.invocations):
        text = user_defined.clean_text(invocation.output_text)
        if text:
            return user_defined.truncate(text)
    return ""


def skip_reason(turn: Turn) -> SkipReason | None:
    reason = user_defined.service_call_reason(turn.first)
    if reason is not None:
        try:
            return SkipReason(reason)
        except ValueError:
            return SkipReason.SERVICE_CALL
    return None


def turn_to_payload(
    turn: Turn,
    *,
    anonymize: bool,
    send_plain_end_user: bool,
    user_hash_secret: str,
) -> dict[str, Any] | SkipReason:
    if (reason := skip_reason(turn)) is not None:
        return reason

    user_input = _user_input(turn)
    if not user_input:
        return (
            SkipReason.PARTIAL_START if turn.partial_start else SkipReason.EMPTY_INPUT
        )
    # May be empty: a turn that timed out on a tool call has no answer yet,
    # and the API accepts an interaction with input only.
    assistant_output = _assistant_output(turn)

    models = list(dict.fromkeys(_model_name(i) for i in turn.invocations))
    raw_user = user_defined.end_user_id(turn).strip() or user_defined.UNKNOWN_END_USER
    end_user = (
        raw_user
        if send_plain_end_user
        else pseudonymize_user(raw_user, secret=user_hash_secret)
    )

    return {
        "interaction": {
            "conversation_id": turn.conversation_id,
            "input": user_input,
            "output": assistant_output,
            "time_start": datetime_to_timestamp_str(turn.time_start),
            "time_end": datetime_to_timestamp_str(turn.time_end),
            "end_user": end_user,
            "hide_content": False,
            "tags": user_defined.build_tags(
                turn, model=_model_name(turn.last), models=models
            ),
        },
        "traces": user_defined.build_traces(
            turn, user_input=user_input, model_name=_model_name
        ),
        "user_feedback": user_defined.build_user_feedback(turn),
        "anonymize": anonymize,
    }
