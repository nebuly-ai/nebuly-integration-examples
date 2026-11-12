"""Small, anonymized Bedrock log records modelled on real ones."""

from __future__ import annotations

from typing import Any, TypedDict, Unpack

ACCOUNT = "123456789012"
ARN = f"arn:aws:sts::{ACCOUNT}:assumed-role/app-role/session"
PROFILE_ARN = f"arn:aws:bedrock:eu-north-1:{ACCOUNT}:inference-profile/"


class EnvelopeArgs(TypedDict, total=False):
    request_id: str
    operation: str
    model_id: str
    timestamp: str
    metadata: dict[str, str] | None
    arn: str


def envelope(
    *,
    request_id: str = "req-1",
    operation: str = "Converse",
    model_id: str = "global.anthropic.claude-haiku-5-5",
    timestamp: str = "2026-11-11T10:00:00Z",
    metadata: dict[str, str] | None = None,
    arn: str = ARN,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "timestamp": timestamp,
        "accountId": ACCOUNT,
        "region": "eu-north-1",
        "requestId": request_id,
        "operation": operation,
        "modelId": model_id,
        "identity": {"arn": arn},
        "inferenceRegion": "us-east-1",
        "schemaVersion": "1.0",
        "schemaType": "ModelInvocationLog",
    }
    if metadata is not None:
        record["requestMetadata"] = metadata
    return record


def with_bodies(
    record: dict[str, Any],
    input_body: object,
    output_body: object,
    *,
    input_tokens: int = 10,
    output_tokens: int = 5,
    cache_read: int | None = 0,
    cache_write: int | None = 0,
) -> dict[str, Any]:
    input_section: dict[str, Any] = {
        "inputContentType": "application/json",
        "inputBodyJson": input_body,
        "inputTokenCount": input_tokens,
    }
    if cache_read is not None:
        input_section["cacheReadInputTokenCount"] = cache_read
    if cache_write is not None:
        input_section["cacheWriteInputTokenCount"] = cache_write
    return {
        **record,
        "input": input_section,
        "output": {
            "outputContentType": "application/json",
            "outputBodyJson": output_body,
            "outputTokenCount": output_tokens,
        },
    }


def converse_output(
    content: list[dict[str, Any]], *, stop: str = "end_turn", latency_ms: int = 1000
) -> dict[str, Any]:
    return {
        "output": {"message": {"role": "assistant", "content": content}},
        "stopReason": stop,
        "metrics": {"latencyMs": latency_ms},
        "usage": {
            "inputTokens": 10,
            "cacheReadInputTokens": 0,
            "cacheWriteInputTokens": 0,
            "outputTokens": 5,
            "totalTokens": 15,
        },
    }


def converse_record(
    messages: list[dict[str, Any]],
    answer: str | list[dict[str, Any]],
    *,
    stop: str = "end_turn",
    system: str | None = None,
    **envelope_kwargs: Unpack[EnvelopeArgs],
) -> dict[str, Any]:
    body: dict[str, Any] = {"messages": messages, "inferenceConfig": {"maxTokens": 512}}
    if system is not None:
        body["system"] = [{"text": system}]
    content = [{"text": answer}] if isinstance(answer, str) else answer
    return with_bodies(
        envelope(**envelope_kwargs), body, converse_output(content, stop=stop)
    )


def user(text: str) -> dict[str, Any]:
    return {"role": "user", "content": [{"text": text}]}


def assistant(text: str) -> dict[str, Any]:
    return {"role": "assistant", "content": [{"text": text}]}
