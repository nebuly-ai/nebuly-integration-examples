from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class CopilotUser(BaseModel):
    id: str
    mail: str | None = None
    user_principal_name: str | None = Field(None, alias="userPrincipalName")

    model_config = ConfigDict(validate_by_name=True, extra="allow")

    @property
    def email(self) -> str:
        return self.mail or self.user_principal_name or self.id


class InteractionBody(BaseModel):
    content_type: str = Field(..., alias="contentType")
    content: str = Field(..., alias="content")

    model_config = ConfigDict(populate_by_name=True)


class Attachment(BaseModel):
    attachment_id: str = Field(..., alias="attachmentId")
    content: str = Field(..., alias="content")
    content_type: str = Field(..., alias="contentType")
    content_url: str = Field(..., alias="contentUrl")
    name: str = Field(..., alias="name")


class Link(BaseModel):
    display_name: str = Field(..., alias="displayName")
    link_type: str = Field(..., alias="linkType")
    link_url: str = Field(..., alias="linkUrl")


class Context(BaseModel):
    context_reference: str = Field(..., alias="contextReference")
    context_type: str = Field(..., alias="contextType")
    display_name: str = Field(..., alias="displayName")


# https://learn.microsoft.com/en-us/microsoft-365/copilot/extensibility/api/ai-services/interaction-export/resources/aiinteraction?pivots=graph-v1
class AiInteraction(BaseModel):
    id: str
    # The thread ID or conversation identifier that maps to all Copilot sessions
    # for the user.
    session_id: str = Field(..., alias="sessionId")
    # The identifier that groups a user prompt with its Copilot response.
    request_id: str = Field(..., alias="requestId")
    interaction_type: Literal["userPrompt", "aiResponse", "unknownFutureValue"] = Field(
        ..., alias="interactionType"
    )
    conversation_type: str = Field(..., alias="conversationType")
    app_class: str = Field(..., alias="appClass")
    locale: str = Field(..., alias="locale")
    created_date_time: datetime = Field(..., alias="createdDateTime")
    completed_date_time: datetime = Field(..., alias="completedDateTime")
    body: InteractionBody = Field(..., alias="body")
    attachments: list[Attachment] = Field(default_factory=list)
    links: list[Link] = Field(default_factory=list)
    # Kept simple as it a very complex object
    mentions: list[dict[str, Any]] = Field(default_factory=list)
    contexts: list[Context] = Field(default_factory=list)

    model_config = ConfigDict(validate_by_name=True, extra="allow")
