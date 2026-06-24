from __future__ import annotations

from typing import TYPE_CHECKING, Any

from . import parser

if TYPE_CHECKING:
    from .converter import InteractionTurn
    from .models import AiInteraction

_ADAPTIVE_CARD_CONTENT_TYPE = "application/vnd.microsoft.card.adaptive"


def build_tags(turn: InteractionTurn) -> dict[str, str]:
    prompt = turn.prompt
    final = turn.final_response
    return {
        "app_class": str(prompt.app_class or ""),
        "conversation_type": str(prompt.conversation_type or ""),
        "locale": str(prompt.locale or ""),
        "session_id": str(prompt.session_id or ""),
        "request_id": str(prompt.request_id or ""),
        "response_count": str(len(turn.responses)),
        "final_model": str((final.sender_model_name if final else None) or ""),
        "user_attachments_count": str(len(prompt.attachments)),
        "user_links_count": str(len(prompt.links)),
        "user_mentions_count": str(len(prompt.mentions)),
    }


def build_traces(turn: InteractionTurn) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []

    llm_trace = _build_llm_trace(turn)
    if llm_trace is not None:
        traces.append(llm_trace)

    final = turn.final_response
    if final is not None:
        traces.extend(_build_retrieval_traces(final))

    return traces


def _build_llm_trace(turn: InteractionTurn) -> dict[str, Any] | None:
    final = turn.final_response
    if final is None:
        return None

    messages: list[dict[str, str]] = []
    user_text = parser.parse_interaction_text(turn.prompt)
    if user_text:
        messages.append({"role": "user", "content": user_text})
    for response in turn.responses:
        text = parser.parse_interaction_text(response)
        if text:
            messages.append({"role": "assistant", "content": text})

    if not any(m["role"] == "assistant" for m in messages):
        return None

    return {
        "model": final.sender_model_name or "copilot",
        "messages": messages,
        "output": parser.parse_interaction_text(final),
    }


def _build_retrieval_traces(final: AiInteraction) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []

    for att in final.attachments:
        if att.content_type == _ADAPTIVE_CARD_CONTENT_TYPE:
            continue
        source = att.name or att.content_url or "attachment"
        traces.append(
            {
                "source": source,
                "input": att.content_url or source,
                "outputs": [att.name or source],
            },
        )

    traces.extend(
        {
            "source": link.link_url,
            "input": link.link_url,
            "outputs": [link.display_name or link.link_url],
        }
        for link in final.links
        if link.link_url
    )

    for ment in final.mentions:
        text = ment.get("mentionText") or str(ment.get("id") or "")
        if not text:
            continue
        traces.append({"source": text, "input": text, "outputs": []})

    return traces


def build_user_feedback(turn: InteractionTurn) -> list[dict[str, Any]]:  # noqa: ARG001
    return []
