from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .converter import Interaction


def build_tags(pair: Interaction) -> dict[str, str]:
    chat = pair.chat
    # Default Claude metadata tags. Remove any you don't need, or add your own below.
    tags = {
        "claude chat-id": chat.id,
        "claude project-id": chat.project_id,
        "model": str(chat.model or "unknown"),
        "chat name": chat.name,
        "href": chat.href,
    }
    # Example — enrich with your own business metadata:
    # tags["department"] = lookup_department(chat.user.email_address)
    # tags["country"] = "US"
    return tags


def build_traces(pair: Interaction) -> list[dict[str, Any]]:  # noqa: ARG001
    # Return intermediate steps behind the answer (LLM/Retrieval/Embedding traces).
    # Example RetrievalTrace built from your RAG layer:
    # return [{"source": "kb_search", "input": query, "outputs": [doc1, doc2]}]
    return []


def build_user_feedback(pair: Interaction) -> list[dict[str, Any]]:  # noqa: ARG001
    # Return explicit feedback for this interaction.
    # Valid slugs: thumbs_up, thumbs_down, copy_input, copy_output, paste,
    #              comment, regenerate, edit, rating.
    # Example:
    # return [{"slug": "thumbs_up", "text": "Helpful!"}, {"slug": "rating", "value": 4}]
    return []
