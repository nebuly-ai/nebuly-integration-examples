"""The connector on sanitized real logs: every API, stream and grouping case.

The expected input, output, user and tokens of each case were checked, when the
case was built, against what the client sent and received and against the log
envelopes. The ``notes`` of a case explain where the connector differs on purpose.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest

from bedrock_sync.runtime.models import Invocation
from bedrock_sync.runtime.parser import normalize_model_id, parse_record
from bedrock_sync.runtime.s3_client import LocalObjectStore, LogSource
from tests.runtime.cases import (
    CASES,
    config,
    counters,
    load,
    run,
    summary_of,
    write_bucket,
)

if TYPE_CHECKING:
    from pathlib import Path

ACCOUNT = "123456789012"


def test_every_case_is_present() -> None:
    assert set(CASES) == {
        "basic",
        "cache",
        "continuation",
        "conversation",
        "history_reset",
        "identities",
        "interleaved",
        "large",
        "limits",
        "mixed_api",
        "native",
        "negative",
        "openai_cache",
        "subagents",
        "tools",
    }


def _order(payload: dict[str, Any]) -> tuple[str, str, str]:
    """Logs have whole seconds: break ties by conversation, then input."""
    interaction = payload["interaction"]
    return (
        interaction["time_start"],
        interaction["conversation_id"],
        interaction["input"],
    )


@pytest.mark.parametrize("name", CASES)
def test_connector_output_matches_the_case(name: str, tmp_path: Path) -> None:
    case = load(name)
    summary, payloads = run(case, tmp_path)
    ordered = sorted(payloads, key=_order)
    assert [summary_of(p) for p in ordered] == case["expected"]["interactions"]
    expected = {k: v for k, v in case["expected"].items() if k != "interactions"}
    assert counters(summary) == expected


@pytest.mark.parametrize("name", CASES)
def test_every_record_parses_except_errors(name: str, tmp_path: Path) -> None:
    case = load(name)
    write_bucket(case, tmp_path)
    cfg = config(case, tmp_path)
    source = LogSource(LocalObjectStore(tmp_path), root_prefix=cfg.logs_root_prefix())
    objects = source.list_objects(
        since=datetime.fromisoformat(case["run"]["from_date"]),
        until=datetime.fromisoformat(case["run"]["now"]),
    )
    records = [record for obj in objects for record in source.read_object(obj)]
    assert len(records) == sum(len(obj["records"]) for obj in case["objects"])
    for record in records:
        parsed = parse_record(record.record, source_key=record.key)
        assert isinstance(parsed, Invocation) or "errorCode" in record.record


def test_external_body_is_read_from_the_data_folder(tmp_path: Path) -> None:
    case = load("large")
    (record,) = (r for obj in case["objects"] for r in obj["records"])
    assert "inputBodyJson" not in record["input"]
    assert "/data/" in record["input"]["inputBodyS3Path"]
    _, payloads = run(case, tmp_path)
    assert payloads[0]["interaction"]["input"].startswith("Summarize briefly:")


def test_all_four_apis_and_both_providers_are_covered() -> None:
    seen = {
        (record["operation"], normalize_model_id(record["modelId"]).split(".")[0])
        for name in CASES
        for obj in load(name)["objects"]
        for record in obj["records"]
    }
    for operation in (
        "Converse",
        "ConverseStream",
        "InvokeModel",
        "InvokeModelWithResponseStream",
    ):
        assert (operation, "anthropic") in seen
        assert (operation, "openai") in seen


def test_openai_body_without_envelope_counters_gives_the_same_tokens() -> None:
    """Bedrock's inputTokenCount excludes cached tokens, OpenAI's prompt_tokens
    includes them: the body fallback must subtract them to agree."""
    records = [r for obj in load("openai_cache")["objects"] for r in obj["records"]]
    unary = [r for r in records if r["operation"] == "InvokeModel"]
    assert {r["input"]["cacheWriteInputTokenCount"] for r in unary} == {0, 4214}
    for record in unary:
        envelope = parse_record(record)
        body_only = parse_record(
            {
                **record,
                "input": {
                    k: v
                    for k, v in record["input"].items()
                    if not k.endswith("TokenCount")
                },
            }
        )
        assert isinstance(envelope, Invocation)
        assert isinstance(body_only, Invocation)
        assert body_only.usage == envelope.usage


def _text(case: dict[str, Any]) -> str:
    return json.dumps({key: case[key] for key in ("source", "objects", "bodies")})


@pytest.mark.parametrize("name", CASES)
def test_fixtures_hold_only_synthetic_aws_identifiers(name: str) -> None:
    case = load(name)
    text = _text(case)
    # 12-digit numbers that are not the tail of a UUID.
    accounts = set(re.findall(r"(?<![\d-])\d{12}(?!\d)", text))
    assert accounts <= {ACCOUNT}
    arns = set(re.findall(r"arn:aws:sts::[^\"]+", text))
    assert arns <= {
        f"arn:aws:sts::{ACCOUNT}:assumed-role/synthetic-role/synthetic-session"
    }
    buckets = set(re.findall(r"s3://([^/\"]+)/", text))
    assert buckets <= {case["source"]["bucket"]}
    for record in (r for obj in case["objects"] for r in obj["records"]):
        assert record["accountId"] == ACCOUNT
        assert "nebuly_test_run" not in (record.get("requestMetadata") or {})
