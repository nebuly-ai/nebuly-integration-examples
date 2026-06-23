from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .converter import InteractionPair


def build_tags(pair: InteractionPair) -> dict[str, str]:
    prompt = pair.prompt
    return {
        "app_class": str(prompt.app_class or ""),
        "conversation_type": str(prompt.conversation_type or ""),
        "locale": str(prompt.locale or ""),
        "session_id": str(prompt.session_id or ""),
        "request_id": str(prompt.request_id or ""),
        "user_attachments_count": str(len(prompt.attachments)),
        "user_links_count": str(len(prompt.links)),
        "user_mentions_count": str(len(prompt.mentions)),
    }


def build_traces(pair: InteractionPair) -> list[dict[str, Any]]:
    response = pair.response
    retrieval_traces: list[dict[str, Any]] = []

    for att in response.attachments:
        source = att.get("name") or att.get("contentUrl") or "attachment"
        retrieval_traces.append(
            {
                "source": source,
                "input": att.get("contentUrl") or source,
                "outputs": [att.get("name") or source],
            },
        )

    for link in response.links:
        url = link.get("href") or link.get("url") or ""
        if url:
            retrieval_traces.append(
                {
                    "source": url,
                    "input": url,
                    "outputs": [link.get("displayName") or url],
                },
            )

    for ment in response.mentions:
        text = ment.get("mentionText") or str(ment.get("id", ""))
        retrieval_traces.append({"source": text, "input": text, "outputs": []})

    return retrieval_traces


def build_user_feedback(pair: InteractionPair) -> list[dict[str, Any]]:  # noqa: ARG001
    return []
