from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

from gemini_enterprise_sync.converter import SkipReason, turn_to_payload
from gemini_enterprise_sync.logging_client import LogRecord
from gemini_enterprise_sync.pseudonymize import pseudonymize_email
from gemini_enterprise_sync.trace_client import TraceData

_ENGINE_ID = "gemini-enterprise-17828149_1782814981868"
_TIMESTAMP = datetime(2026, 7, 2, 16, 14, 38, 513765, tzinfo=UTC)
_TRACE_START = datetime(2026, 7, 2, 16, 14, 27, 554361, tzinfo=UTC)
_TRACE_END = datetime(2026, 7, 2, 16, 14, 38, 513741, tzinfo=UTC)
_TEST_SECRET = "test-secret"


class PayloadKwargs(TypedDict):
    send_plain_end_user: bool
    user_hash_secret: str


def _payload_kwargs(
    *,
    send_plain_end_user: bool = False,
    user_hash_secret: str = _TEST_SECRET,
) -> PayloadKwargs:
    return {
        "send_plain_end_user": send_plain_end_user,
        "user_hash_secret": user_hash_secret,
    }


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
    result = turn_to_payload(
        record,
        trace,
        engine_id=_ENGINE_ID,
        anonymize=False,
        **_payload_kwargs(),
    )
    assert not isinstance(result, SkipReason)
    end_user = result["interaction"]["end_user"]
    assert end_user == pseudonymize_email("l.mammana@nebuly.ai", secret=_TEST_SECRET)
    assert end_user != "l.mammana@nebuly.ai"
    uuid.UUID(end_user)
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
    result = turn_to_payload(
        record,
        None,
        engine_id=_ENGINE_ID,
        anonymize=False,
        **_payload_kwargs(),
    )
    assert not isinstance(result, SkipReason)
    assert result["interaction"]["time_start"] == "2026-07-02T16:14:38.513765Z"
    assert result["interaction"]["time_end"] == "2026-07-02T16:14:38.513765Z"


def test_empty_input_skip() -> None:
    record = _record_from_fixture()
    record.payload["request"]["query"]["parts"][0]["text"] = "   "
    result = turn_to_payload(
        record,
        None,
        engine_id=_ENGINE_ID,
        anonymize=False,
        **_payload_kwargs(),
    )
    assert result is SkipReason.EMPTY_INPUT


def test_empty_output_skip() -> None:
    record = _record_from_fixture()
    record.payload["serviceTextReply"] = ""
    result = turn_to_payload(
        record,
        None,
        engine_id=_ENGINE_ID,
        anonymize=False,
        **_payload_kwargs(),
    )
    assert result is SkipReason.EMPTY_OUTPUT


def test_end_user_pseudonym_deterministic() -> None:
    record = _record_from_fixture()
    kwargs = _payload_kwargs()
    first = turn_to_payload(
        record, None, engine_id=_ENGINE_ID, anonymize=False, **kwargs
    )
    second = turn_to_payload(
        record, None, engine_id=_ENGINE_ID, anonymize=False, **kwargs
    )
    assert not isinstance(first, SkipReason)
    assert not isinstance(second, SkipReason)
    assert first["interaction"]["end_user"] == second["interaction"]["end_user"]

    other = turn_to_payload(
        record,
        None,
        engine_id=_ENGINE_ID,
        anonymize=False,
        **_payload_kwargs(user_hash_secret="other-secret"),
    )
    assert not isinstance(other, SkipReason)
    assert other["interaction"]["end_user"] != first["interaction"]["end_user"]


def test_end_user_plain_opt_out() -> None:
    record = _record_from_fixture()
    result = turn_to_payload(
        record,
        None,
        engine_id=_ENGINE_ID,
        anonymize=False,
        **_payload_kwargs(send_plain_end_user=True),
    )
    assert not isinstance(result, SkipReason)
    assert result["interaction"]["end_user"] == "l.mammana@nebuly.ai"


def test_end_user_none_when_principal_missing() -> None:
    record = _record_from_fixture()
    record.payload["userIamPrincipal"] = None
    result = turn_to_payload(
        record,
        None,
        engine_id=_ENGINE_ID,
        anonymize=False,
        **_payload_kwargs(),
    )
    assert not isinstance(result, SkipReason)
    assert result["interaction"]["end_user"] is None


def test_pseudonymize_email_case_normalization() -> None:
    mixed = pseudonymize_email("L.Mammana@Nebuly.AI", secret=_TEST_SECRET)
    lower = pseudonymize_email("l.mammana@nebuly.ai", secret=_TEST_SECRET)
    assert mixed == lower
    assert mixed is not None
    uuid.UUID(mixed)


def test_pseudonymize_email_empty_returns_none() -> None:
    assert pseudonymize_email(None, secret=_TEST_SECRET) is None
    assert pseudonymize_email("", secret=_TEST_SECRET) is None
    assert pseudonymize_email("   ", secret=_TEST_SECRET) is None
