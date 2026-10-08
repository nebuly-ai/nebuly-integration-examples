from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class ChatUser(BaseModel):
    id: str
    email_address: str | None = None


class TextContent(BaseModel):
    type: Literal["text"]
    text: str
    thinking_redacted: bool = False
    truncated: bool = False


class ToolUseContent(BaseModel):
    type: Literal["tool_use"]
    id: str | None = None
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
    tool_use_id: str | None = None
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
    model_config = ConfigDict(extra="ignore")

    id: str
    filename: str
    mime_type: str | None = None
    md5: str | None = None
    size_bytes: int | None = None
    created_at: datetime | None = None


class ArtifactRef(BaseModel):
    id: str
    version_id: str
    title: str | None = None
    artifact_type: str | None = None


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
    model: str | None = None
    organization_id: str | None = None
    organization_uuid: str
    project_id: str | None = None
    user: ChatUser | None = None


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
    model: str | None = None
    organization_id: str | None = None
    organization_uuid: str
    project_id: str | None = None
    user: ChatUser | None = None
    chat_messages: list[ChatMessage]
    has_more: bool
    first_id: str | None = None
    last_id: str | None = None


class ProvenanceSyntheticMarker(BaseModel):
    type: Literal["synthetic_marker"]


class ProvenanceClientAsserted(BaseModel):
    type: Literal["client_asserted"]


class ProvenanceContentUnavailable(BaseModel):
    type: Literal["content_unavailable"]
    reason: str


Provenance = Annotated[
    ProvenanceSyntheticMarker | ProvenanceClientAsserted | ProvenanceContentUnavailable,
    Field(discriminator="type"),
]


class SessionUser(BaseModel):
    id: str
    email_address: str | None = None


class StartedByUser(BaseModel):
    id: str
    email_address: str | None = None


class LocalSession(BaseModel):
    id: str
    organization_uuid: str
    workspace_id: str | None = None
    user: SessionUser | None = None
    product_surface: str | None = None
    created_at: datetime
    updated_at: datetime
    truncated: bool = False


class RemoteSession(BaseModel):
    id: str
    organization_uuid: str
    user: SessionUser | None = None
    agent_id: str | None = None
    started_by_user: StartedByUser | None = None
    status: str
    created_at: datetime
    updated_at: datetime
    product_surface: str | None = None
    claude_project_id: str | None = None


class PaginatedLocalSessionsResponse(BaseModel):
    data: list[LocalSession]
    next_page: str | None = None


class PaginatedRemoteSessionsResponse(BaseModel):
    data: list[RemoteSession]
    next_page: str | None = None


class SessionMessage(BaseModel):
    id: str
    role: str
    created_at: datetime
    content: list[ContentBlock]
    model: str | None = None
    provenance: Provenance | None = None
    content_unavailable: bool | None = None
    sent_by_user_id: str | None = None


class LocalSessionMessagesResponse(BaseModel):
    session: LocalSession
    data: list[SessionMessage]
    next_page: str | None = None


class RemoteSessionMessagesResponse(BaseModel):
    session: RemoteSession
    data: list[SessionMessage]
    next_page: str | None = None


class RemoteSessionListMetadata(BaseModel):
    started_by_user: StartedByUser | None = None
    claude_project_id: str | None = None
    user: SessionUser | None = None
    agent_id: str | None = None
    status: str | None = None
    product_surface: str | None = None

    @classmethod
    def from_remote_session(cls, session: RemoteSession) -> RemoteSessionListMetadata:
        return cls(
            started_by_user=session.started_by_user,
            claude_project_id=session.claude_project_id,
            user=session.user,
            agent_id=session.agent_id,
            status=session.status,
            product_surface=session.product_surface,
        )


def content_unavailable_reason(provenance: Provenance | None) -> str | None:
    if isinstance(provenance, ProvenanceContentUnavailable):
        return provenance.reason
    return None
