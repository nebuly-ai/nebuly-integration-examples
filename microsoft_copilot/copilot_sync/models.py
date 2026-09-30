import json
from datetime import datetime
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator

type JsonValue = (
    str | int | float | bool | None | list[JsonValue] | dict[str, JsonValue]
)


class CopilotUser(BaseModel):
    id: str
    mail: str | None = None
    user_principal_name: str | None = Field(None, alias="userPrincipalName")
    department: str | None = None
    job_title: str | None = Field(None, alias="jobTitle")
    office_location: str | None = Field(None, alias="officeLocation")
    city: str | None = None
    country: str | None = None
    usage_location: str | None = Field(None, alias="usageLocation")
    company_name: str | None = Field(None, alias="companyName")

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


class AuditMessage(BaseModel):
    id: str = Field(..., validation_alias=AliasChoices("Id", "id"))
    is_prompt: bool = Field(
        default=False,
        validation_alias=AliasChoices("isPrompt", "IsPrompt"),
    )
    jailbreak_detected: bool | None = Field(
        None,
        validation_alias=AliasChoices("JailbreakDetected", "jailbreakDetected"),
    )

    model_config = ConfigDict(validate_by_name=True, extra="allow")


class AccessedResource(BaseModel):
    resource_id: str | None = Field(None, validation_alias=AliasChoices("Id", "id"))
    name: str | None = Field(None, validation_alias=AliasChoices("Name", "name"))
    site_url: str | None = Field(
        None, validation_alias=AliasChoices("SiteUrl", "siteUrl")
    )
    sensitivity_label_id: str | None = Field(
        None,
        validation_alias=AliasChoices("SensitivityLabelId", "sensitivityLabelId"),
    )
    action: str | None = Field(None, validation_alias=AliasChoices("Action", "action"))
    status: str | None = Field(None, validation_alias=AliasChoices("Status", "status"))
    xpia_detected: bool | None = Field(
        None,
        validation_alias=AliasChoices("XPIADetected", "xpiaDetected"),
    )

    model_config = ConfigDict(validate_by_name=True, extra="allow")


class ModelTransparencyDetail(BaseModel):
    model_provider_name: str | None = Field(
        None,
        validation_alias=AliasChoices("ModelProviderName", "modelProviderName"),
    )
    model_name: str | None = Field(
        None,
        validation_alias=AliasChoices("ModelName", "modelName"),
    )
    model_version: str | None = Field(
        None,
        validation_alias=AliasChoices("ModelVersion", "modelVersion"),
    )

    model_config = ConfigDict(validate_by_name=True, extra="allow")


class AISystemPlugin(BaseModel):
    plugin_id: str | None = Field(None, validation_alias=AliasChoices("Id", "id"))
    name: str | None = Field(None, validation_alias=AliasChoices("Name", "name"))
    version: str | None = Field(
        None, validation_alias=AliasChoices("Version", "version")
    )

    model_config = ConfigDict(validate_by_name=True, extra="allow")


_COPILOT_EVENT_FIELD_MAP: tuple[tuple[str, str], ...] = (
    ("ThreadId", "thread_id"),
    ("AppHost", "app_host"),
    ("AppIdentity", "app_identity"),
    ("AgentId", "agent_id"),
    ("AgentName", "agent_name"),
    ("AgentVersion", "agent_version"),
    ("Messages", "messages"),
    ("AccessedResources", "accessed_resources"),
    ("AISystemPlugin", "ai_system_plugins"),
    ("ModelTransparencyDetails", "model_details"),
)


def _parse_audit_data_blob(audit_data: JsonValue | None) -> dict[str, JsonValue] | None:
    if audit_data is None:
        return None
    if isinstance(audit_data, str):
        try:
            parsed: JsonValue = json.loads(audit_data)
        except json.JSONDecodeError:
            return None
        audit_data = parsed
    if not isinstance(audit_data, dict):
        return None
    return audit_data


_COPILOT_LIST_FIELD_KEYS: frozenset[str] = frozenset(
    {
        "messages",
        "accessed_resources",
        "ai_system_plugins",
        "model_details",
    },
)


def _normalize_list_field(value: JsonValue | None) -> JsonValue | list[JsonValue]:
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]
    return value


def _merge_copilot_event_fields(
    target: dict[str, JsonValue],
    event: dict[str, JsonValue],
) -> None:
    for src_key, dst_key in _COPILOT_EVENT_FIELD_MAP:
        if src_key not in event:
            continue
        incoming = event[src_key]
        if dst_key in _COPILOT_LIST_FIELD_KEYS:
            incoming = _normalize_list_field(incoming)
        existing = target.get(dst_key)
        if existing is None or (dst_key in _COPILOT_LIST_FIELD_KEYS and not existing):
            target[dst_key] = incoming
    if "ClientRegion" in event and "client_region" not in target:
        target["client_region"] = event["ClientRegion"]


class CopilotAuditRecord(BaseModel):
    """Purview CopilotInteraction record from Graph auditLog/queries."""

    id: str
    created_datetime: datetime = Field(..., alias="createdDateTime")
    user_principal_name: str | None = Field(None, alias="userPrincipalName")
    thread_id: str | None = None
    app_host: str | None = None
    app_identity: str | None = None
    agent_id: str | None = None
    agent_name: str | None = None
    agent_version: str | None = None
    client_region: str | None = None
    messages: list[AuditMessage] = Field(default_factory=list)
    accessed_resources: list[AccessedResource] = Field(default_factory=list)
    ai_system_plugins: list[AISystemPlugin] = Field(default_factory=list)
    model_details: list[ModelTransparencyDetail] = Field(default_factory=list)

    model_config = ConfigDict(validate_by_name=True, extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _extract_copilot_event_data(cls, data: JsonValue) -> JsonValue:
        if not isinstance(data, dict):
            return data
        out: dict[str, JsonValue] = dict(data)
        audit_data = _parse_audit_data_blob(
            out.get("auditData") or out.get("AuditData"),
        )
        if audit_data is None:
            return out

        event = audit_data.get("CopilotEventData")
        if isinstance(event, dict):
            _merge_copilot_event_fields(out, event)
        else:
            _merge_copilot_event_fields(out, audit_data)

        if "ClientRegion" in audit_data and "client_region" not in out:
            out["client_region"] = audit_data["ClientRegion"]
        if "UserId" in audit_data and "user_principal_name" not in out:
            out["user_principal_name"] = audit_data["UserId"]
        return out

    @property
    def message_ids(self) -> list[str]:
        return [message.id for message in self.messages if message.id]

    @property
    def primary_model(self) -> ModelTransparencyDetail | None:
        for detail in self.model_details:
            if detail.model_name:
                return detail
        return None

    @property
    def web_search_used(self) -> bool:
        return any(
            (plugin.plugin_id or "").lower() == "bingwebsearch"
            or (plugin.name or "").lower() == "bingwebsearch"
            for plugin in self.ai_system_plugins
        )

    @property
    def jailbreak_detected(self) -> bool:
        return any(
            message.jailbreak_detected
            for message in self.messages
            if message.jailbreak_detected
        )

    @property
    def xpia_detected(self) -> bool:
        return any(
            resource.xpia_detected
            for resource in self.accessed_resources
            if resource.xpia_detected
        )
