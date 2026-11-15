"""Reusable AgentCore cases without private raw archives."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).with_name("fixtures")
VARIANTS = json.loads((FIXTURES / "synthetic_variants.json").read_text())["cases"]
CASES = [
    path.stem
    for path in sorted(FIXTURES.glob("*.json"))
    if path.stem != "synthetic_variants"
] + [variant["name"] for variant in VARIANTS]


@pytest.fixture(params=CASES)
def agentcore_case(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Load a real-shape case or materialize a clearly labeled synthetic variant."""
    name = request.param
    variant = next((item for item in VARIANTS if item["name"] == name), None)
    base = variant["base"] if variant else name
    case: dict[str, Any] = json.loads((FIXTURES / f"{base}.json").read_text())
    if variant is None:
        return case
    case["provenance"] = {
        "kind": "synthetic",
        "base": base,
        "mutation": variant["mutation"],
        "description": "Contract example; not observed AWS behavior.",
    }
    case["expected"].update(variant["expected"])
    mutation = variant["mutation"]
    if mutation in {"remove_fallback", "replace_fallback_with_structured_actor"}:
        case["records"] = [
            event
            for event in case["records"]
            if json.loads(event["message"])["scope"]["name"]
            != "bedrock_agentcore.memory.client"
        ]
    if mutation == "remove_fallback":
        case["expected"]["actor_id"] = None
    for event in list(case["records"]):
        record = json.loads(event["message"])
        if record.get("name") == "invoke_agent Strands Agents":
            _mutate_root(record, mutation, case["expected"])
        if record.get("name") == "chat":
            if mutation == "remove_input_usage_aliases":
                del record["attributes"]["gen_ai.usage.input_tokens"]
                del record["attributes"]["gen_ai.usage.prompt_tokens"]
            elif mutation == "duplicate_model_span":
                duplicate = copy.deepcopy(event)
                duplicate["eventId"] += "-duplicate"
                duplicate["logGroupName"] = "aws/spans"
                case["records"].append(duplicate)
        event["message"] = json.dumps(record, separators=(",", ":"))
    return case


def _mutate_root(
    record: dict[str, Any], mutation: str, expected: dict[str, Any]
) -> None:
    if mutation in {"add_structured_actor", "replace_fallback_with_structured_actor"}:
        record["attributes"]["actor.id"] = "synthetic-structured-actor"
    elif mutation == "remove_root_end":
        del record["endTimeUnixNano"]
        del record["durationNano"]
        record["status"] = {"code": "UNSET"}
        expected["terminal_status"] = "UNSET"
