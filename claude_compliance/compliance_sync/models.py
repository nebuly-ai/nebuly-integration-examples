from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from datetime import datetime


class ChatUser(BaseModel):
    id: str
    email_address: str


class OrganizationUser(BaseModel):
    id: str
    created_at: datetime
    email: str
    full_name: str
    organization_role: Literal[
        "admin",
        "billing",
        "claude_code_user",
        "developer",
        "managed",
        "membership_admin",
        "owner",
        "primary_owner",
        "user",
    ]


class TextContent(BaseModel):
    type: Literal["text"]
    text: str


class ToolUseContent(BaseModel):
    type: Literal["tool_use"]
    id: str
    name: str
    input: str
    truncated: bool = False
    integration_name: str | None = None
    mcp_server_url: str | None = None


class ToolResultTextContent(BaseModel):
    type: Literal["text"]
    text: str


class ToolResultContent(BaseModel):
    type: Literal["tool_result"]
    tool_use_id: str
    is_error: bool
    content: list[ToolResultTextContent]
    truncated: bool = False
    integration_name: str | None = None
    mcp_server_url: str | None = None
    name: str | None = None


ContentBlock = Annotated[
    TextContent | ToolUseContent | ToolResultContent,
    Field(discriminator="type"),
]


class FileRef(BaseModel):
    id: str
    filename: str
    mime_type: str


class ArtifactRef(BaseModel):
    id: str
    version_id: str
    title: str
    artifact_type: str


class ChatMessage(BaseModel):
    id: str
    role: Literal["user", "assistant"]
    created_at: datetime
    content: list[ContentBlock]
    files: list[FileRef] | None = None
    generated_files: list[FileRef] | None = None
    artifacts: list[ArtifactRef] | None = None


class ChatSummary(BaseModel):
    id: str
    name: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
    href: str
    model: str | None
    organization_id: str
    organization_uuid: str
    project_id: str
    user: ChatUser


class PaginatedChatsResponse(BaseModel):
    data: list[ChatSummary]
    has_more: bool
    first_id: str | None = None
    last_id: str | None = None


class ChatMessagesResponse(BaseModel):
    id: str
    name: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None
    href: str
    model: str | None
    organization_id: str
    organization_uuid: str
    project_id: str
    user: ChatUser
    chat_messages: list[ChatMessage]
    has_more: bool
    first_id: str | None = None
    last_id: str | None = None


class PaginatedUsersResponse(BaseModel):
    data: list[OrganizationUser]
    has_more: bool
    next_page: str | None = None
