"""Payload contract: actual model usage, identity and explicit enrichment."""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest.mock import patch

import pytest

from bedrock_sync.agentcore import user_defined
from bedrock_sync.agentcore.converter import pseudonymize_actor, snapshot_to_payload
from bedrock_sync.agentcore.records import SourceScope
from bedrock_sync.shared.model_names import observed_model_name
from tests.agentcore.cases import config, load, snapshot


def test_payload_matches_case_usage_and_visibility(
    agentcore_case: dict[str, Any],
) -> None:
    case = agentcore_case
    source, item = snapshot(case)
    if item.readiness != "ready":
        with pytest.raises(ValueError, match="Only complete"):
            snapshot_to_payload(item, source=source, config=config(), context={})
        return
    payload = snapshot_to_payload(item, source=source, config=config(), context={})
    assert payload["interaction"]["input"] == case["expected"]["input"]
    assert payload["interaction"]["output"] == (case["expected"]["output"] or "")
    assert payload["interaction"]["end_user"] != item.actor_id
    llms = [trace for trace in payload["traces"] if "model" in trace]
    usage = case["expected"]["usage_by_model"]
    assert len(llms) == len(usage)
    for trace, (model, counters) in zip(llms, sorted(usage.items()), strict=True):
        assert trace["model"] == observed_model_name(model)
        assert trace["input_tokens"] == counters["input_tokens"]
        assert trace["output_tokens"] == counters["output_tokens"]
        assert trace["cache_read_input_tokens"] == counters["cache_read_input_tokens"]
        assert trace["cache_write_input_tokens"] == counters["cache_write_input_tokens"]
        assert "cost" not in trace
    assert "agentcore_metadata" not in payload
    assert payload["interaction"]["conversation_id"] == item.session_id
    assert len(payload["traces"]) == len(llms) + sum(
        not tool["internal_to_subagent"] for tool in case["expected"]["tools"]
    )
    assert payload == snapshot_to_payload(
        item, source=source, config=config(), context={}
    )


def test_actor_identity_is_exact_and_public_session_id_is_preserved() -> None:
    source, item = snapshot(load())
    first = snapshot_to_payload(item, source=source, config=config(), context={})
    assert pseudonymize_actor("Actor", "secret") != pseudonymize_actor(
        "actor", "secret"
    )
    assert pseudonymize_actor(" Actor", "secret") != pseudonymize_actor(
        "Actor", "secret"
    )
    other = SourceScope(source.account_id, source.region, source.source_id + "-other")
    moved = replace(
        item, interaction_key=other.interaction_key(item.trace_id, item.root_span_id)
    )
    second = snapshot_to_payload(moved, source=other, config=config(), context={})
    assert (
        first["interaction"]["conversation_id"]
        == second["interaction"]["conversation_id"]
        == item.session_id
    )
    with pytest.raises(ValueError, match="different source"):
        snapshot_to_payload(item, source=other, config=config(), context={})


def test_hooks_receive_native_identity_and_none_segments_are_omitted() -> None:
    source, item = snapshot(load("terminal_error"))
    context = {"mapping": "synthetic"}
    with (
        patch.object(
            user_defined,
            "build_segments",
            return_value={
                "team": "Engineering",
                "market": None,
                "tenure": "1_to_3_years",
            },
        ) as segments,
        patch.object(user_defined, "build_tags", return_value={"custom": "tag"}),
    ):
        payload = snapshot_to_payload(
            item,
            source=source,
            config=config(SEND_PLAIN_END_USER="true"),
            context=context,
        )
    segments.assert_called_once_with(item, context)
    interaction = payload["interaction"]
    assert interaction["end_user"] == item.actor_id
    assert interaction["team"] == "Engineering"
    assert "market" not in interaction
    assert interaction["tenure"] == "1_to_3_years"
    assert interaction["tags"]["agentcore.status"] == "error"
    assert payload["user_feedback"] == []


def test_content_is_not_cleaned_or_truncated_and_models_keep_their_own_output() -> None:
    source, item = snapshot(load())
    call = item.model_calls[0]
    alternate = replace(
        call, span_id="other", model_id="custom.unrecognized-v1:0", output="other-model"
    )
    item = replace(
        item,
        input="<system-reminder>user text</system-reminder>" + "x" * 9000,
        output="final",
        model_calls=(call, alternate),
    )
    payload = snapshot_to_payload(item, source=source, config=config(), context={})
    assert payload["interaction"]["input"] == item.input
    by_model = {trace["model"]: trace for trace in payload["traces"]}
    assert by_model["custom.unrecognized-v1:0"]["output"] == "other-model"
    assert by_model["claude-haiku-5-5"]["output"] == call.output
    assert payload["interaction"]["output"] == "final"


@pytest.mark.parametrize(("field", "value"), [("team", " "), ("other", "value")])
def test_invalid_segments_fail_before_delivery(field: str, value: str) -> None:
    source, item = snapshot(load())
    with (
        patch.object(user_defined, "build_segments", return_value={field: value}),
        pytest.raises(ValueError, match=r"[Ss]egment"),
    ):
        snapshot_to_payload(item, source=source, config=config(), context={})
