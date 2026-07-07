from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .models import LogPayload, TextGroundingMetadata

if TYPE_CHECKING:
    from .logging_client import LogRecord
    from .models import GroundingReference
    from .trace_client import TraceData


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


def _user_input(payload: LogPayload) -> str:
    if payload.request is None or payload.request.query is None:
        return ""
    parts = payload.request.query.parts
    if not parts or parts[0].text is None:
        return ""
    return parts[0].text.strip()


def _grounding_metadata(payload: LogPayload) -> list[TextGroundingMetadata]:
    if payload.response is None or payload.response.answer is None:
        return []
    metadata = []
    for reply in payload.response.answer.replies:
        grounded = reply.grounded_content
        if grounded is None or grounded.text_grounding_metadata is None:
            continue
        metadata.append(grounded.text_grounding_metadata)
    return metadata


def build_tags(
    record: LogRecord,
    *,
    engine_id: str,
    session_id: str | None,
    answer_id: str | None,
) -> dict[str, str]:
    payload = LogPayload.validate_from_logging_or_bigquery(record.payload)
    tags: dict[str, str] = {
        "session_id": session_id or "",
        "engine_id": engine_id,
        "answer_id": answer_id or "",
        "method_name": (
            payload.log_metadata.method_name if payload.log_metadata else ""
        )
        or "",
        "service_label": (
            payload.log_metadata.service_label if payload.log_metadata else ""
        )
        or "",
    }

    return tags


def build_traces(record: LogRecord, trace: TraceData | None) -> list[dict[str, Any]]:
    payload = LogPayload.validate_from_logging_or_bigquery(record.payload)
    traces: list[dict[str, Any]] = []
    user_input = _user_input(payload)
    assistant_output = (payload.service_text_reply or "").strip()

    if assistant_output:
        traces.append(
            {
                "model": "gemini",
                "messages": [{"role": "user", "content": user_input}],
                "output": assistant_output,
                "input_tokens": trace.input_tokens if trace else 0,
                "output_tokens": trace.output_tokens if trace else 0,
            }
        )

    for metadata in _grounding_metadata(payload):
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


def build_user_feedback(record: LogRecord) -> list[dict[str, Any]]:  # noqa: ARG001
    return []
