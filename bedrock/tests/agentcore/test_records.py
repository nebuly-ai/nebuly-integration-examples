"""Opaque identifiers retain their source scope and exact spelling."""

import json

from bedrock_sync.agentcore.records import RawLogRecord, SourceScope


def test_keys_do_not_merge_sources_delimiters_or_case() -> None:
    source = SourceScope("123456789012", "eu-north-1", "endpoint-a")
    other = SourceScope("123456789012", "eu-north-1", "endpoint-b")
    assert source.interaction_key("a:b", "c") != source.interaction_key("a", "b:c")
    assert source.conversation_key("Session") != source.conversation_key("session")
    assert source.record_key("group", "id") != other.record_key("group", "id")
    assert source.record_key("group-a", "id") != source.record_key("group-b", "id")
    assert json.loads(source.key) == [
        "agentcore",
        1,
        "123456789012",
        "eu-north-1",
        "endpoint-a",
    ]


def test_raw_record_repr_omits_customer_content() -> None:
    record = RawLogRecord("group", "stream", "id", 1, 2, "private-content")
    assert "private-content" not in repr(record)
