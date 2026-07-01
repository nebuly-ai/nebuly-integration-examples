from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .converter import Turn
    from .models import GroundingReference, Session


def _reference_source(reference: GroundingReference) -> str:
    metadata = reference.document_metadata
    if metadata is None:
        return reference.content or "unknown"
    if metadata.uri:
        return metadata.uri
    if metadata.title:
        return metadata.title
    if metadata.domain:
        return metadata.domain
    return reference.content or "unknown"


def build_tags(turn: Turn, session: Session) -> dict[str, str]:
    return {
        "session_id": session.session_id,
        "query_id": turn.query_id,
        "session_display_name": session.display_name or "",
        "answer_generation_mode": turn.session_turn.answer_generation_mode,
        "model_id": turn.session_turn.model_id,
        "answer_state": turn.answer_state,
        "session_state": session.state or "",
    }


def build_traces(turn: Turn) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []
    user_input = turn.user_input
    for metadata in turn.grounding:
        for reference in metadata.references:
            title = (
                reference.document_metadata.title
                if reference.document_metadata
                else None
            )
            output = reference.content or title or ""
            traces.append(
                {
                    "source": _reference_source(reference),
                    "input": user_input,
                    "outputs": [output] if output else [],
                }
            )
    return traces


def build_user_feedback(turn: Turn) -> list[dict[str, Any]]:  # noqa: ARG001
    return []
