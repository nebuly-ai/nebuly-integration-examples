"""Typed models for Langfuse records and Nebuly interaction payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list[JsonValue] | dict[str, JsonValue]
type ChatMessage = dict[str, JsonValue]


class LangfuseObservation(TypedDict, total=False):
    id: str
    traceId: str
    parentObservationId: str | None
    model: str | None
    name: str | None
    input: JsonValue
    output: JsonValue
    startTime: str
    endTime: str
    usageDetails: dict[str, int]


class LangfuseTrace(TypedDict, total=False):
    id: str
    sessionId: str | None
    userId: str | None
    timestamp: str
    input: JsonValue
    output: JsonValue
    tags: list[str] | dict[str, str]


class LangfuseListMeta(TypedDict, total=False):
    totalPages: int


class LangfuseTracesResponse(TypedDict, total=False):
    data: list[LangfuseTrace]
    meta: LangfuseListMeta


class LangfuseObservationsResponse(TypedDict, total=False):
    data: list[LangfuseObservation]
    meta: LangfuseListMeta


class InteractionPayload(TypedDict):
    conversation_id: str
    input: str
    output: str
    time_start: str
    time_end: str
    end_user: str
    hide_content: bool
    tags: dict[str, str]


class LLMTracePayload(TypedDict, total=False):
    messages: list[ChatMessage]
    model: str
    output: str
    input_tokens: int
    output_tokens: int


class RetrievalTracePayload(TypedDict):
    source: str
    input: str
    outputs: list[str]


class NebulyRequestPayload(TypedDict):
    interaction: InteractionPayload
    traces: list[LLMTracePayload | RetrievalTracePayload]
    user_feedback: list[dict[str, str | int]]
    anonymize: bool


@dataclass
class LLMTrace:
    messages: list[ChatMessage]
    model: str
    output: str
    input_tokens: int | None = None
    output_tokens: int | None = None

    def to_dict(self) -> LLMTracePayload:
        payload: LLMTracePayload = {
            "messages": self.messages,
            "model": self.model,
            "output": self.output,
        }
        if self.input_tokens is not None:
            payload["input_tokens"] = self.input_tokens
        if self.output_tokens is not None:
            payload["output_tokens"] = self.output_tokens
        return payload


@dataclass
class RetrievalTrace:
    source: str
    input: str
    outputs: list[str]

    def to_dict(self) -> RetrievalTracePayload:
        return {"source": self.source, "input": self.input, "outputs": self.outputs}


@dataclass
class Interaction:
    conversation_id: str
    input: str
    output: str
    time_start: str
    time_end: str
    end_user: str
    tags: dict[str, str]
    traces: list[RetrievalTrace | LLMTrace]

    def to_interaction_dict(self) -> InteractionPayload:
        return {
            "conversation_id": self.conversation_id,
            "input": self.input,
            "output": self.output,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "end_user": self.end_user,
            "hide_content": False,
            "tags": self.tags,
        }
