from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from datetime import datetime


class CopilotUser(BaseModel):
    id: str
    mail: str | None = None
    user_principal_name: str | None = Field(None, alias="userPrincipalName")

    model_config = ConfigDict(validate_by_name=True)

    @property
    def email(self) -> str:
        return self.mail or self.user_principal_name or self.id


class InteractionBody(BaseModel):
    content_type: str | None = Field(None, alias="contentType")
    content: str | None = None

    model_config = ConfigDict(populate_by_name=True)


class AiInteraction(BaseModel):
    id: str | None = None
    session_id: str | None = Field(None, alias="sessionId")
    request_id: str | None = Field(None, alias="requestId")
    interaction_type: str | None = Field(None, alias="interactionType")
    conversation_type: str | None = Field(None, alias="conversationType")
    app_class: str | None = Field(None, alias="appClass")
    locale: str | None = None
    created_date_time: datetime | None = Field(None, alias="createdDateTime")
    completed_date_time: datetime | None = Field(None, alias="completedDateTime")
    body: InteractionBody | None = None
    attachments: list[dict[str, Any]] = Field(default_factory=list)
    links: list[dict[str, Any]] = Field(default_factory=list)
    mentions: list[dict[str, Any]] = Field(default_factory=list)
    contexts: list[dict[str, Any]] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)
