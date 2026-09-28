from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .converter import InteractionTurn
    from .models import AiInteraction, CopilotUser

_ADAPTIVE_CARD_CONTENT_TYPE = "application/vnd.microsoft.card.adaptive"


def build_tags(turn: InteractionTurn, user: CopilotUser) -> dict[str, str]:
    prompt = turn.prompt
    final = turn.final_response
    raw: dict[str, str | None] = {
        "app_class": prompt.app_class,
        "conversation_type": prompt.conversation_type,
        "locale": prompt.locale,
        "session_id": prompt.session_id,
        "request_id": prompt.request_id,
        "final_model": final.sender_model_name if final else None,
        "department": user.department,
        "job_title": user.job_title,
        "office_location": user.office_location,
        "city": user.city,
        "country": user.country or user.usage_location,
        "company_name": user.company_name,
    }
    return {key: value for key, value in raw.items() if value is not None}


def build_traces(turn: InteractionTurn) -> list[dict[str, Any]]:
    final = turn.final_response
    if final is None:
        return []
    return _build_retrieval_traces(final)


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
