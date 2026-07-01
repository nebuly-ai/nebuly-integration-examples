from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Part(BaseModel):
    text: str | None = None
    function_call: dict[str, Any] | None = Field(default=None, alias="functionCall")
    function_response: dict[str, Any] | None = Field(
        default=None, alias="functionResponse"
    )
    thought_signature: str | None = Field(default=None, alias="thoughtSignature")

    model_config = {"populate_by_name": True, "extra": "ignore"}


class Content(BaseModel):
    role: str | None = None
    parts: list[Part] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


class UsageMetadata(BaseModel):
    prompt_token_count: int | None = None
    candidates_token_count: int | None = None
    thoughts_token_count: int | None = None
    total_token_count: int | None = None
    traffic_type: str | None = None

    model_config = {"extra": "ignore"}


class EventMetadata(BaseModel):
    custom_metadata: dict[str, Any] | None = Field(default=None, alias="customMetadata")

    model_config = {"populate_by_name": True, "extra": "ignore"}


class RawEvent(BaseModel):
    model_version: str | None = Field(default=None, alias="modelVersion")
    node_info: dict[str, Any] | None = Field(default=None, alias="nodeInfo")

    model_config = {"populate_by_name": True, "extra": "ignore"}


class Event(BaseModel):
    name: str | None = None
    author: str
    content: Content | None = None
    invocation_id: str | None = Field(default=None, alias="invocationId")
    timestamp: str
    event_metadata: EventMetadata | None = Field(default=None, alias="eventMetadata")
    raw_event: RawEvent | None = Field(default=None, alias="rawEvent")

    model_config = {"populate_by_name": True, "extra": "ignore"}


class Session(BaseModel):
    name: str
    user_id: str = Field(alias="userId")
    create_time: str = Field(alias="createTime")
    update_time: str = Field(alias="updateTime")

    model_config = {"populate_by_name": True, "extra": "ignore"}

    @property
    def session_id(self) -> str:
        return self.name.rsplit("/", maxsplit=1)[-1]


class SessionListResponse(BaseModel):
    sessions: list[Session] = Field(default_factory=list)
    next_page_token: str | None = Field(default=None, alias="nextPageToken")

    model_config = {"populate_by_name": True, "extra": "ignore"}


class EventListResponse(BaseModel):
    session_events: list[Event] = Field(default_factory=list, alias="sessionEvents")
    next_page_token: str | None = Field(default=None, alias="nextPageToken")

    model_config = {"populate_by_name": True, "extra": "ignore"}
