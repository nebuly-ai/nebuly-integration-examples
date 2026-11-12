"""The parts of the conversion a customer is expected to adapt.

``converter.py`` calls into this module for the end user, the tags, the
traces and the user feedback, and for the rules that recognise calls that are
not user conversations. The defaults are tuned on Claude Code and on plain
Converse / InvokeModel applications.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .formats.base import canonical_json
from .grouping import SESSION_METADATA_KEYS
from .models import BlockKind, Message, tool_loop

if TYPE_CHECKING:
    from collections.abc import Callable

    from .grouping import Turn
    from .models import Block, Invocation

MAX_CONTENT_CHARS = 8000
"""Same cap as the compliance integrations, applied to every text we send."""

INJECTED_CONTEXT_PATTERNS: list[re.Pattern[str]] = [
    # Claude Code wraps hints for the model in system reminders.
    re.compile(r"<system-reminder>.*?</system-reminder>\s*", re.DOTALL),
    # Claude Code's summary after a compaction, as a block of its own.
    re.compile(
        r"\A\s*This session is being continued from a previous conversation.*",
        re.DOTALL,
    ),
    # First line of the Claude Code system prompt, not instructions.
    re.compile(r"^x-anthropic-billing-header:[^\n]*\n?"),
]
"""Text the client adds around the user's words. Applied to each text block on
its own, so a pattern never eats the user's words in a later block. Removed
from what we send, never from what we hash."""

SERVICE_CALL_PATTERNS: dict[str, re.Pattern[str]] = {
    # Claude Code names the session with a side call.
    "session_title": re.compile(r"You write short session titles"),
    # Claude Code's WebFetch hands the fetched page to a small model.
    "web_fetch_summary": re.compile(r"^\s*Web page content:\s*\n---"),
}
"""Side calls an agent makes for itself. Matched on the system prompt and on
the text of the first user message; the key is the skip reason."""

PROBE_MAX_CHARS = 8
"""A ``max_tokens <= 1`` call without Claude Code metadata counts as a probe
only when its question is this short (``.``, ``ping``): an application that
classifies with a one-token answer sends a real question."""

USER_METADATA_KEYS = ("user_id", "userId", "end_user", "endUser", "user")
"""``requestMetadata`` keys a caller can use to name the end user."""

UNKNOWN_END_USER = "unknown"
"""End user when the log names nobody: the API requires one."""

_SSO_ROLE_PREFIX = "AWSReservedSSO_"


def clean_text(text: str) -> str:
    for pattern in INJECTED_CONTEXT_PATTERNS:
        text = pattern.sub("", text)
    return text.strip()


def message_text(message: Message) -> str:
    """The text of a message without injected context, cleaned per block."""
    texts = (
        clean_text(b.text)
        for b in message.blocks
        if b.kind is BlockKind.TEXT and b.text
    )
    return "\n".join(t for t in texts if t)


def truncate(text: str) -> str:
    return text[:MAX_CONTENT_CHARS]


def service_call_reason(invocation: Invocation) -> str | None:
    """Why the first call of a turn is not a user conversation, if it is not.

    ``max_tokens <= 1`` calls are the probes Claude Code fires to check a
    model or warm the prompt cache: they never produce an answer. Some carry
    no Claude Code metadata, only a trivial question (``.``).
    """
    last = invocation.last_message
    first_text = last.text if last and last.role == "user" else ""
    if invocation.max_tokens is not None and invocation.max_tokens <= 1:
        question = message_text(last) if last and last.role == "user" else ""
        if invocation.claude_code or len(question) <= PROBE_MAX_CHARS:
            return "single_token_probe"
    for reason, pattern in SERVICE_CALL_PATTERNS.items():
        if pattern.search(invocation.system) or pattern.search(first_text):
            return reason
    return None


def _arn_principal(arn: str) -> tuple[str, str, str]:
    """``(kind, name, session)`` of an IAM / STS ARN.

    ``arn:aws:sts::1:assumed-role/Role/session`` -> ``("assumed-role", "Role",
    "session")``; ``arn:aws:iam::1:user/name`` -> ``("user", "name", "")``.
    """
    resource = arn.split(":", 5)[-1]
    kind, _, rest = resource.partition("/")
    name, _, session = rest.partition("/")
    return kind, name, session


def end_user_id(turn: Turn) -> str:
    """Who asked, before pseudonymization.

    1. a user id the caller put in ``requestMetadata``;
    2. the person behind an IAM Identity Center (SSO) session, whose session
       name is the user name;
    3. the Claude Code account, then the Claude Code device: with a shared
       Bedrock API key they are the only per-person hint;
    4. the caller ARN, which is often an application role shared by all users;
    5. ``UNKNOWN_END_USER``.
    """
    first = turn.first
    for key in USER_METADATA_KEYS:
        value = first.request_metadata.get(key, "").strip()
        if value:
            return value
    kind, name, session = _arn_principal(first.identity_arn)
    if kind == "assumed-role" and name.startswith(_SSO_ROLE_PREFIX) and session:
        return session
    claude_code = first.claude_code
    if claude_code and (claude_code.account_uuid or claude_code.device_id):
        return claude_code.account_uuid or claude_code.device_id or ""
    return first.identity_arn.strip() or UNKNOWN_END_USER


def tool_calls(turn: Turn) -> list[tuple[Block, str | None]]:
    """Every tool call of the turn, in order, with its result (None while
    unanswered, in a timed-out turn).

    The last call re-sends the whole agent loop of the turn, so its history
    holds every round trip, also those of calls delivered too late or before
    the synced window. Each result is paired with the call of its own round:
    some models reuse the same id (``functions.search:0``) in every round.
    """
    last = turn.last
    rounds = [m for m in tool_loop(last.messages) if m.blocks]
    rounds.append(Message("assistant", last.tool_uses))
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


def build_tags(turn: Turn, *, model: str, models: list[str]) -> dict[str, str]:
    """Few tags, on the pattern of the other integrations: the model that
    gave the answer (and every model of the turn if more than one), the tools
    used, the session the client gave, the deployment it ran in, plus
    whatever the caller chose to put in ``requestMetadata``."""
    first = turn.first
    tags: dict[str, str] = {"model": model, "aws account": first.account_id}
    if len(models) > 1:
        tags["models"] = ",".join(models)
    tools = sorted({tool.tool_name for tool, _ in tool_calls(turn) if tool.tool_name})
    if tools:
        tags["tools used"] = ",".join(tools)
    if first.claude_code and first.claude_code.session_id:
        tags["claude code session-id"] = first.claude_code.session_id
    skip = {*USER_METADATA_KEYS, *SESSION_METADATA_KEYS}
    for key, value in first.request_metadata.items():
        if key not in skip and value:
            tags.setdefault(key, value)
    return tags


def trace_cost(invocations: list[Invocation]) -> int | None:  # noqa: ARG001
    """Cost of these calls in micro-dollars, or None to let Nebuly compute it
    from the model and the tokens. Override it with your Bedrock prices to
    count prompt caching exactly."""
    return None


def _llm_trace(
    model: str, invocations: list[Invocation], user_input: str
) -> dict[str, Any]:
    output = next(
        (
            text
            for invocation in reversed(invocations)
            if (text := clean_text(invocation.output_text))
        ),
        "",
    )
    trace: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": user_input}],
        "output": truncate(output),
        "input_tokens": sum(i.usage.total_input_tokens for i in invocations),
        "output_tokens": sum(i.usage.output_tokens for i in invocations),
    }
    if (cost := trace_cost(invocations)) is not None:
        trace["cost"] = cost
    return trace


def build_traces(
    turn: Turn, *, user_input: str, model_name: Callable[[Invocation], str]
) -> list[dict[str, Any]]:
    """One LLM trace per model with the tokens of all its calls in the turn,
    like the Gemini integration sums the tokens of its spans: the cost of the
    whole agent loop without the text of every intermediate step. Then one
    retrieval trace per tool call with its result, like claude_compliance.

    Calls are split by model because Nebuly prices tokens per model.
    """
    by_model: dict[str, list[Invocation]] = {}
    for invocation in turn.invocations:
        by_model.setdefault(model_name(invocation), []).append(invocation)
    traces = [
        _llm_trace(model, invocations, user_input)
        for model, invocations in by_model.items()
    ]

    traces.extend(
        {
            "source": tool.tool_name or "tool",
            "input": truncate(canonical_json(tool.tool_input)),
            "outputs": [truncate(result)],
        }
        for tool, result in tool_calls(turn)
        if result is not None
    )
    return traces


def build_user_feedback(turn: Turn) -> list[dict[str, Any]]:  # noqa: ARG001
    """Bedrock logs carry no feedback."""
    return []
