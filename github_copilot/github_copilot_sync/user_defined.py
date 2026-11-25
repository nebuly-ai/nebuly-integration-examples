"""Customer hooks and default helper-call heuristics."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Call

# Short list of known Copilot helper system prompts (editable by customers).
HELPER_SYSTEM_PREFIXES: tuple[str, ...] = (
    "Generate a concise title",
    "You are a helpful assistant that generates chat titles",
)


def is_background_call(call: Call) -> bool:
    """Return True to drop a paired call before turn grouping."""
    return False


def default_is_background_call(call: Call, request_json: dict[str, object]) -> bool:
    """Default heuristic: no tools, single short user message, helper system text."""
    messages = request_json.get("messages")
    if not isinstance(messages, list):
        return False
    tools = request_json.get("tools")
    if tools:
        return False
    user_messages = [
        message
        for message in messages
        if isinstance(message, dict) and message.get("role") == "user"
    ]
    if len(user_messages) != 1:
        return False
    system_text = " ".join(
        str(message.get("content") or "")
        for message in messages
        if isinstance(message, dict) and message.get("role") in ("system", "developer")
    )
    return any(system_text.startswith(prefix) for prefix in HELPER_SYSTEM_PREFIXES)
