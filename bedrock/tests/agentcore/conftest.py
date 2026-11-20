"""AgentCore cases: every built scenario plus labeled contract mutations."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from tests.agentcore.spans import SCENARIOS, load

# Contract expectations on mutated text_cache_read, not observed AWS behavior.
VARIANTS: dict[str, tuple[str, dict[str, Any]]] = {
    "structured_actor_without_memory_log": (
        "replace_fallback_with_structured_actor",
        {"actor_id": "structured-actor", "identity_source": "structured"},
    ),
    "structured_actor_overrides_conflicting_fallback": (
        "add_structured_actor",
        {"actor_id": "structured-actor", "diagnostic": "actor_conflict"},
    ),
    "missing_identity": (
        "remove_fallback",
        {"readiness": "blocked", "diagnostic": "missing_actor", "actor_id": None},
    ),
    "duplicate_model_span_in_shared_group": ("duplicate_model_span", {}),
    "unfinished_main_span": (
        "remove_root_end",
        {"readiness": "open", "terminal_status": "UNSET"},
    ),
    "missing_model_usage": (
        "remove_input_usage_aliases",
        {"diagnostic": "missing_usage", "usage_by_model": {}},
    ),
}


@pytest.fixture(params=[*sorted(SCENARIOS), *VARIANTS])
def agentcore_case(request: pytest.FixtureRequest) -> dict[str, Any]:
    if request.param in SCENARIOS:
        return load(request.param)
    mutation, expected = VARIANTS[request.param]
    case = load("text_cache_read")
    case["expected"].update(expected)
    records = []
    for event in case["records"]:
        record = json.loads(event["message"])
        scope = record["scope"]["name"]
        if scope == "bedrock_agentcore.memory.client" and mutation in {
            "remove_fallback",
            "replace_fallback_with_structured_actor",
        }:
            continue
        if record.get("name") == "invoke_agent Strands Agents":
            if mutation in {
                "add_structured_actor",
                "replace_fallback_with_structured_actor",
            }:
                record["attributes"]["actor.id"] = "structured-actor"
            elif mutation == "remove_root_end":
                del record["endTimeUnixNano"], record["durationNano"]
                record["status"] = {"code": "UNSET"}
        if record.get("name") == "chat" and mutation == "remove_input_usage_aliases":
            del record["attributes"]["gen_ai.usage.input_tokens"]
            del record["attributes"]["gen_ai.usage.prompt_tokens"]
        records.append({**event, "message": json.dumps(record)})
        if record.get("name") == "chat" and mutation == "duplicate_model_span":
            records.append(
                {
                    **copy.deepcopy(records[-1]),
                    "eventId": event["eventId"] + "-dup",
                    "logGroupName": "aws/spans",
                }
            )
    case["records"] = records
    return case
