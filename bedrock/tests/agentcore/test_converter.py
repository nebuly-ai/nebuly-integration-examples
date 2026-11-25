"""Payload contract: actual model usage, identity and explicit enrichment."""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest.mock import patch

import pytest
from bedrock_sync.agentcore import user_defined
from bedrock_sync.agentcore.converter import pseudonymize_actor, snapshot_to_payload
from bedrock_sync.agentcore.models import ToolCall
from bedrock_sync.agentcore.records import SourceScope
from bedrock_sync.shared.model_names import observed_model_name

from tests.agentcore.cases import config, load, snapshot


def test_payload_matches_case_usage_and_visibility(
    agentcore_case: dict[str, Any],
) -> None:
    case = agentcore_case
    source, item = snapshot(case)
    if item.readiness != "ready":
        with pytest.raises(ValueError, match="Only ended"):
            snapshot_to_payload(item, source=source, config=config(), context={})
        return
    payload = snapshot_to_payload(item, source=source, config=config(), context={})
    assert payload["interaction"]["input"] == case["expected"]["input"]
    assert payload["interaction"]["output"] == (case["expected"]["output"] or "")
    assert payload["interaction"]["end_user"] == item.actor_id
    pseudonymized = snapshot_to_payload(
        item, source=source, config=config(PSEUDONYMIZE_END_USER="true"), context={}
    )
    assert item.actor_id is not None
    assert pseudonymized["interaction"]["end_user"] == pseudonymize_actor(
        item.actor_id, "test-secret"
    )
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
            config=config(),
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


def test_content_is_not_cleaned_and_models_keep_their_own_output() -> None:
    source, item = snapshot(load())
    call = item.model_calls[0]
    alternate = replace(
        call, span_id="other", model_id="custom.unrecognized-v1:0", output="other-model"
    )
    item = replace(
        item,
        input="<system-reminder>user text</system-reminder>",
        output="final",
        model_calls=(call, alternate),
    )
    payload = snapshot_to_payload(item, source=source, config=config(), context={})
    assert payload["interaction"]["input"] == item.input
    by_model = {trace["model"]: trace for trace in payload["traces"]}
    assert by_model["custom.unrecognized-v1:0"]["output"] == "other-model"
    assert by_model["claude-haiku-5-5"]["output"] == call.output
    assert payload["interaction"]["output"] == "final"


def test_every_text_is_cut_at_the_content_limit() -> None:
    source, item = snapshot(load())
    long = "x" * (user_defined.MAX_CONTENT_CHARS + 500)
    tool = ToolCall(
        "t1", "c1", "read_pdf", item.start_ns, item.start_ns, long, long, "OK"
    )
    item = replace(
        item,
        input=long,
        output=long,
        model_calls=(replace(item.model_calls[0], output=long),),
        tools=(tool,),
    )
    payload = snapshot_to_payload(item, source=source, config=config(), context={})
    texts = [payload["interaction"]["input"], payload["interaction"]["output"]]
    for trace in payload["traces"]:
        if "messages" in trace:
            texts += [trace["messages"][0]["content"], trace["output"]]
        else:
            texts += [trace["input"], *trace["outputs"]]
    assert len(texts) == 6
    assert all(len(text) == user_defined.MAX_CONTENT_CHARS for text in texts)


@pytest.mark.parametrize(("field", "value"), [("team", " "), ("other", "value")])
def test_invalid_segments_fail_before_delivery(field: str, value: str) -> None:
    source, item = snapshot(load())
    with (
        patch.object(user_defined, "build_segments", return_value={field: value}),
        pytest.raises(ValueError, match=r"[Ss]egment"),
    ):
        snapshot_to_payload(item, source=source, config=config(), context={})


def test_interaction_with_warnings_is_sent_with_what_could_be_read() -> None:
    source, item = snapshot(load())
    degraded = replace(
        item, session_id=None, terminal_status="UNSET", output=None, input="question"
    )
    payload = snapshot_to_payload(degraded, source=source, config=config(), context={})
    interaction = payload["interaction"]
    assert interaction["conversation_id"] == item.trace_id
    assert interaction["output"] == ""
    assert interaction["tags"]["agentcore.status"] == "unknown"
    with pytest.raises(ValueError, match="input or output"):
        snapshot_to_payload(
            replace(degraded, input=None), source=source, config=config(), context={}
        )
