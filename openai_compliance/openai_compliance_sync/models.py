from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class LogFile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str


class LogFileList(BaseModel):
    model_config = ConfigDict(extra="ignore")

    data: list[LogFile] = Field(default_factory=list)
    has_more: bool = False
    last_end_time: str | None = None


class Actor(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str | None = None
    user_id: str | None = None
    user_email: str | None = None


class MessageAuthor(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str | None = None
    client_type: str | None = None
    model: str | None = None
    tools_used: list[str] = Field(default_factory=list)
    skills_used: list[str] = Field(default_factory=list)
    skills_used_names: list[str] = Field(default_factory=list)


class MessageContent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str | None = None
    value: str | None = None
    annotations: list[Any] = Field(default_factory=list)


class ConversationRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    title: str | None = None
    created_at: datetime | None = None
    is_temporary_chat: bool | None = None
    gpt_id: str | None = None
    gpt_name: str | None = None
    project_id: str | None = None
    project_name: str | None = None
    mode: str | None = None


class ChatMessagePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    created_at: datetime | None = None
    author: MessageAuthor | None = None
    content: MessageContent | None = None


class ConversationMessageRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str | None = None
    actor: Actor | None = None
    timestamp: datetime | None = None
    previous_message_id: str | None = None
    message: ChatMessagePayload
    conversation: ConversationRef


class TokenUsage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None
    uncached_input_tokens: int | None = None
    reasoning_output_tokens: int | None = None


class CodexAgentRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str | None = None


class CodexEventDetails(BaseModel):
    model_config = ConfigDict(extra="ignore")

    detail_type: str | None = None
    session_id: str | None = None
    turn_id: str | None = None
    call_id: str | None = None
    prompt_text: str | None = None
    response_text: str | None = None
    status: str | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    service_tier: str | None = None
    token_usage: TokenUsage | None = None
    environment_id: str | None = None
    tool_name: str | None = None
    tool_type: str | None = None
    tool_input: dict[str, Any] | None = None
    tool_call_id: str | None = None


class CodexLogRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str
    event_type: str | None = None
    client_id: str | None = None
    timestamp: datetime | None = None
    actor: Actor | None = None
    agent: CodexAgentRef | None = None
    event_details: CodexEventDetails | None = None


class CodexTurnClassification(BaseModel):
    model_config = ConfigDict(extra="ignore")

    level_1: str | None = None
    level_2: str | None = None


class CodexTurnPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    turn_id: str
    session_id: str | None = None
    model: str | None = None
    cost_usd: float | None = None
    estimated_cost_usd: float | None = None
    token_usage: TokenUsage | None = None
    classification: CodexTurnClassification | None = None


class CodexTurnRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str
    timestamp: datetime | None = None
    payload: CodexTurnPayload


class AppLogRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str
    app_id: str | None = None
    app_name: str | None = None
    app_type: str | None = None
    conversation_id: str | None = None
    timestamp: datetime | None = None
    log_type: Literal["request", "response"] | None = None
    input: dict[str, Any] | None = None
    output: list[dict[str, Any]] = Field(default_factory=list)
    request: dict[str, Any] | None = None
    response: dict[str, Any] | None = None

    @property
    def record_kind(self) -> Literal["request", "response", "unknown"]:
        if self.log_type is not None:
            return self.log_type
        if self.request is not None:
            return "request"
        if self.response is not None:
            return "response"
        return "unknown"

    def request_input(self) -> str | None:
        if self.request is None:
            return None
        raw = self.request.get("input")
        return raw if isinstance(raw, str) else None

    def response_output_text(self) -> str | None:
        if self.response is None:
            return None
        output = self.response.get("output")
        if not isinstance(output, list):
            return None
        parts = _legacy_text_blocks_from_app_output(output)
        if not parts:
            return None
        return "\n".join(parts)


def _legacy_text_blocks_from_app_output(output: list[object]) -> list[str]:
    parts: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        nested = item.get("response")
        if not isinstance(nested, dict):
            continue
        result = nested.get("result")
        if not isinstance(result, dict):
            continue
        content = result.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
    return parts
