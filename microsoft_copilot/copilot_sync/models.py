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
    attachment_id: str | None = Field(None, alias="attachmentId")
    content: str | None = Field(None, alias="content")
    content_type: str = Field(..., alias="contentType")
    content_url: str | None = Field(None, alias="contentUrl")
    name: str | None = Field(None, alias="name")


class Link(BaseModel):
    display_name: str | None = Field(None, alias="displayName")
    link_type: str | None = Field(None, alias="linkType")
    link_url: str | None = Field(None, alias="linkUrl")


class Context(BaseModel):
    context_reference: str | None = Field(None, alias="contextReference")
    context_type: str | None = Field(None, alias="contextType")
    display_name: str | None = Field(None, alias="displayName")


class TeamworkApplicationIdentity(BaseModel):
    id: str | None = None
    display_name: str | None = Field(None, alias="displayName")

    model_config = ConfigDict(validate_by_name=True, extra="allow")


class FromIdentitySet(BaseModel):
    user: dict[str, Any] | None = None
    application: TeamworkApplicationIdentity | None = None

    model_config = ConfigDict(validate_by_name=True, extra="allow")


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
    created_datetime: datetime = Field(..., alias="createdDateTime")
    body: InteractionBody = Field(..., alias="body")
    attachments: list[Attachment] = Field(default_factory=list)
    links: list[Link] = Field(default_factory=list)
    # Kept simple as it a very complex object
    mentions: list[dict[str, Any]] = Field(default_factory=list)
    contexts: list[Context] = Field(default_factory=list)
    sender: FromIdentitySet | None = Field(None, alias="from")

    model_config = ConfigDict(validate_by_name=True, extra="allow")

    @property
    def sender_model_name(self) -> str | None:
        if self.sender and self.sender.application:
            return self.sender.application.display_name
        return None
