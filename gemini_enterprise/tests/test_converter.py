from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from gemini_enterprise_sync.converter import SkipReason, turn_to_payload
from gemini_enterprise_sync.logging_client import LogRecord
from gemini_enterprise_sync.trace_client import TraceData

_ENGINE_ID = "gemini-enterprise-17828149_1782814981868"
_TIMESTAMP = datetime(2026, 7, 2, 16, 14, 38, 513765, tzinfo=UTC)
_TRACE_START = datetime(2026, 7, 2, 16, 14, 27, 554361, tzinfo=UTC)
_TRACE_END = datetime(2026, 7, 2, 16, 14, 38, 513741, tzinfo=UTC)


def _record_from_fixture() -> LogRecord:
    payload = json.loads(
        (Path(__file__).parent / "data" / "log_entry.json").read_text(encoding="utf-8")
    )
    return LogRecord(
        timestamp=_TIMESTAMP,
        insert_id="test-insert",
        trace_id="abc123trace",
        payload=payload,
    )


def test_turn_to_payload_with_tokens() -> None:
    record = _record_from_fixture()
    trace = TraceData(
        input_tokens=13725,
        output_tokens=1998,
        time_start=_TRACE_START,
        time_end=_TRACE_END,
    )
    result = turn_to_payload(record, trace, engine_id=_ENGINE_ID, anonymize=False)
    assert not isinstance(result, SkipReason)
    assert result["interaction"]["end_user"] == "l.mammana@nebuly.ai"
    assert (
        result["interaction"]["conversation_id"] == f"{_ENGINE_ID}-8490740223094055586"
    )
    assert result["interaction"]["time_start"] == "2026-07-02T16:14:27.554361Z"
    assert result["interaction"]["time_end"] == "2026-07-02T16:14:38.513741Z"

    llm_traces = [t for t in result["traces"] if "model" in t]
    assert len(llm_traces) == 1
    assert llm_traces[0]["input_tokens"] == 13725
    assert llm_traces[0]["output_tokens"] == 1998

    retrieval_traces = [t for t in result["traces"] if "source" in t]
    assert len(retrieval_traces) == 1
    assert retrieval_traces[0]["source"] == "https://example.com/doc-1"


def test_turn_to_payload_falls_back_to_log_timestamp_without_trace() -> None:
    record = _record_from_fixture()
    result = turn_to_payload(record, None, engine_id=_ENGINE_ID, anonymize=False)
    assert not isinstance(result, SkipReason)
    assert result["interaction"]["time_start"] == "2026-07-02T16:14:38.513765Z"
    assert result["interaction"]["time_end"] == "2026-07-02T16:14:38.513765Z"


def test_empty_input_skip() -> None:
    record = _record_from_fixture()
    record.payload["request"]["query"]["parts"][0]["text"] = "   "
    result = turn_to_payload(record, None, engine_id=_ENGINE_ID, anonymize=False)
    assert result is SkipReason.EMPTY_INPUT


def test_empty_output_skip() -> None:
    record = _record_from_fixture()
    record.payload["serviceTextReply"] = ""
    result = turn_to_payload(record, None, engine_id=_ENGINE_ID, anonymize=False)
    assert result is SkipReason.EMPTY_OUTPUT
