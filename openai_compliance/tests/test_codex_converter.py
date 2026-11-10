from __future__ import annotations

from datetime import UTC, datetime

from openai_compliance_sync.codex_converter import codex_record_to_payload
from openai_compliance_sync.models import CodexEventDetails, CodexLogRecord, TokenUsage


def _ts() -> datetime:
    return datetime(2025, 6, 15, 9, 0, tzinfo=UTC)


def _record(
    *,
    detail_type: str = "PROMPT_RESPONSE_RECEIVED",
    status: str = "ok",
    prompt: str = "hello",
    response: str = "world",
) -> CodexLogRecord:
    return CodexLogRecord(
        event_id="evt-1",
        client_id="client-1",
        timestamp=_ts(),
        event_details=CodexEventDetails(
            detail_type=detail_type,
            session_id="sess-1",
            turn_id="turn-1",
            prompt_text=prompt,
            response_text=response,
            status=status,
            model="gpt-5-codex",
            token_usage=TokenUsage(input_tokens=10, output_tokens=20),
        ),
    )


def test_full_codex_payload_includes_tokens() -> None:
    payload = codex_record_to_payload(_record(), anonymize=False, directory=None)
    assert payload is not None
    assert payload["interaction"]["conversation_id"] == "sess-1"
    trace = payload["traces"][0]
    assert trace["input_tokens"] == 10
    assert trace["output_tokens"] == 20
    assert trace["model"] == "gpt-5-codex"


def test_non_prompt_detail_type_returns_none() -> None:
    assert (
        codex_record_to_payload(
            _record(detail_type="TOOL_CALL"),
            anonymize=False,
            directory=None,
        )
        is None
    )


def test_missing_prompt_returns_none() -> None:
    assert (
        codex_record_to_payload(
            _record(prompt=""),
            anonymize=False,
            directory=None,
        )
        is None
    )


def test_error_status_returns_none() -> None:
    assert (
        codex_record_to_payload(
            _record(status="error"),
            anonymize=False,
            directory=None,
        )
        is None
    )
