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

    user_id: str | None = None
    user_email: str | None = None


class MessageAuthor(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str | None = None
    client_type: str | None = None
    model: str | None = None
    tools_used: list[str] = Field(default_factory=list)
    skills_used: list[str] = Field(default_factory=list)


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


class ChatMessagePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    created_at: datetime
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
    reasoning_output_tokens: int | None = None


class CodexEventDetails(BaseModel):
    model_config = ConfigDict(extra="ignore")

    detail_type: str | None = None
    session_id: str | None = None
    turn_id: str | None = None
    prompt_text: str | None = None
    response_text: str | None = None
    status: str | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    service_tier: str | None = None
    token_usage: TokenUsage | None = None
    environment_id: str | None = None


class CodexLogRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str
    client_id: str | None = None
    timestamp: datetime | None = None
    event_details: CodexEventDetails | None = None


class AppLogRequestOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    text: str | None = None


class AppLogResponseContent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    text: str | None = None


class AppLogResponseResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    content: list[AppLogResponseContent] = Field(default_factory=list)


class AppLogResponseOutputItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    response: dict[str, Any] | None = None

    def result_texts(self) -> list[str]:
        if self.response is None:
            return []
        result = self.response.get("result")
        if not isinstance(result, dict):
            return []
        content = result.get("content")
        if not isinstance(content, list):
            return []
        texts: list[str] = []
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str) and text:
                    texts.append(text)
        return texts


class AppLogRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str
    app_id: str | None = None
    app_name: str | None = None
    app_type: str | None = None
    conversation_id: str | None = None
    timestamp: datetime | None = None
    request: dict[str, Any] | None = None
    response: dict[str, Any] | None = None

    @property
    def record_kind(self) -> Literal["request", "response", "unknown"]:
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
        parts = _text_blocks_from_app_output(output)
        if not parts:
            return None
        return "\n".join(parts)


def _text_blocks_from_app_output(output: list[object]) -> list[str]:
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
