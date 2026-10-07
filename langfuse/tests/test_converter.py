from __future__ import annotations

import json

from langfuse_sync.converter import (
    convert_observations_to_traces,
    interaction_from_langfuse_trace,
)
from langfuse_sync.models import (
    EmbeddingTrace,
    JsonValue,
    LangfuseObservation,
    LangfuseTrace,
    LLMTrace,
    RetrievalTrace,
)


def test_tag_key_value_parsing() -> None:
    trace: LangfuseTrace = {
        "id": "t1",
        "timestamp": "2026-01-01T00:00:00Z",
        "input": "hi",
        "output": "bye",
        "tags": ["team:Engineering", "beta", "team:Ops"],
    }
    interaction = interaction_from_langfuse_trace(trace, [])
    assert interaction is not None
    assert interaction.tags["team"] == "Engineering, Ops"
    assert interaction.tags["beta"] == "true"


def _minimal_trace(**overrides: object) -> LangfuseTrace:
    base: LangfuseTrace = {
        "id": "t1",
        "timestamp": "2026-01-01T00:00:00Z",
        "input": "hi",
        "output": "bye",
    }
    base.update(overrides)  # type: ignore[typeddict-item]
    return base


def test_metadata_scalar_flattening() -> None:
    trace = _minimal_trace(
        metadata={"tenant": "nebuly", "temperature": 0, "stream": True},
    )
    interaction = interaction_from_langfuse_trace(trace, [])
    assert interaction is not None
    assert interaction.tags["tenant"] == "nebuly"
    assert interaction.tags["temperature"] == "0"
    assert interaction.tags["stream"] == "true"


def test_metadata_nested_flattening() -> None:
    trace = _minimal_trace(
        metadata={"scope": {"attributes": {"public_key": "pk"}}},
    )
    interaction = interaction_from_langfuse_trace(trace, [])
    assert interaction is not None
    assert interaction.tags["scope.attributes.public_key"] == "pk"


def test_metadata_list_handling() -> None:
    trace = _minimal_trace(metadata={"labels": ["a", "b"]})
    interaction = interaction_from_langfuse_trace(trace, [])
    assert interaction is not None
    assert interaction.tags["labels"] == "a, b"

    trace_nested = _minimal_trace(metadata={"items": [{"x": 1}]})
    interaction_nested = interaction_from_langfuse_trace(trace_nested, [])
    assert interaction_nested is not None
    assert interaction_nested.tags["items"] == json.dumps([{"x": 1}])

    empty_cases: tuple[dict[str, JsonValue] | None, ...] = (
        {"empty_list": []},
        {"empty_dict": {}},
        None,
    )
    for empty_meta in empty_cases:
        trace_empty = _minimal_trace(metadata=empty_meta)
        interaction_empty = interaction_from_langfuse_trace(trace_empty, [])
        assert interaction_empty is not None
        assert "empty_list" not in interaction_empty.tags
        assert "empty_dict" not in interaction_empty.tags


def test_metadata_tag_collision_langfuse_wins() -> None:
    trace = _minimal_trace(
        tags=["team:Ops"],
        metadata={"team": "Eng"},
    )
    interaction = interaction_from_langfuse_trace(trace, [])
    assert interaction is not None
    assert interaction.tags["team"] == "Ops"


def test_nested_generation_emitted() -> None:
    observations: list[LangfuseObservation] = [
        {
            "id": "parent",
            "type": "SPAN",
            "startTime": "2026-01-01T00:00:00Z",
            "input": "x",
            "output": "y",
        },
        {
            "id": "child",
            "parentObservationId": "parent",
            "type": "GENERATION",
            "model": "gpt-4",
            "startTime": "2026-01-01T00:00:01Z",
            "input": {"role": "user", "content": "hello"},
            "output": "world",
            "usageDetails": {"input": 3, "output": 2},
            "calculatedTotalCost": 0.000012,
        },
    ]
    traces = convert_observations_to_traces(observations)
    assert len(traces) == 1
    assert isinstance(traces[0], LLMTrace)
    assert traces[0].model == "gpt-4"
    assert traces[0].cost == 12


def test_wrapper_span_skipped_leaf_span_becomes_retrieval() -> None:
    observations: list[LangfuseObservation] = [
        {
            "id": "wrapper",
            "type": "SPAN",
            "startTime": "2026-01-01T00:00:00Z",
            "input": "in",
            "output": "out",
        },
        {
            "id": "child",
            "parentObservationId": "wrapper",
            "type": "GENERATION",
            "model": "gpt-4",
            "startTime": "2026-01-01T00:00:01Z",
            "input": "q",
            "output": "a",
        },
        {
            "id": "leaf",
            "type": "SPAN",
            "name": "lookup",
            "startTime": "2026-01-01T00:00:02Z",
            "input": "query",
            "output": "doc",
        },
    ]
    traces = convert_observations_to_traces(observations)
    kinds = {type(t) for t in traces}
    assert LLMTrace in kinds
    assert RetrievalTrace in kinds
    assert not any(
        isinstance(t, RetrievalTrace) and t.source == "wrapper" for t in traces
    )


def test_embedding_trace_and_usage_fallback() -> None:
    observations: list[LangfuseObservation] = [
        {
            "id": "emb",
            "type": "EMBEDDING",
            "model": "text-embedding-3",
            "startTime": "2026-01-01T00:00:00Z",
            "input": "vec",
            "usage": {"input": 7},
        },
        {
            "id": "gen",
            "type": "GENERATION",
            "model": "gpt-4",
            "startTime": "2026-01-01T00:00:01Z",
            "input": "q",
            "output": "a",
            "usage": {"input": 4, "output": 1},
            "costDetails": {"total": 0.000002},
        },
    ]
    traces = convert_observations_to_traces(observations)
    embedding = next(t for t in traces if isinstance(t, EmbeddingTrace))
    assert embedding.input_tokens == 7
    llm = next(t for t in traces if isinstance(t, LLMTrace))
    assert llm.input_tokens == 4
    assert llm.output_tokens == 1
    assert llm.cost == 2
