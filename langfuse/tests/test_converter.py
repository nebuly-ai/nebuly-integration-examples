from __future__ import annotations

from typing import Any

from langfuse_sync.converter import (
    convert_observations_to_traces,
    interaction_from_langfuse_trace,
)
from langfuse_sync.models import EmbeddingTrace, LLMTrace, RetrievalTrace


def test_tag_key_value_parsing() -> None:
    trace: dict[str, Any] = {
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


def test_nested_generation_emitted() -> None:
    observations: list[dict[str, Any]] = [
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
    observations: list[dict[str, Any]] = [
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
    observations: list[dict[str, Any]] = [
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
