from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

_ANSWER_MODE_KEY = (
    "google.discoveryengine.googleapis.com.Assistant.answer_generation_mode"
)
_MODEL_ID_KEY = "google.discoveryengine.googleapis.com.Assistant.model_id"


class Query(BaseModel):
    query_id: str | None = Field(default=None, alias="queryId")
    text: str | None = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class DocumentMetadata(BaseModel):
    uri: str | None = None
    title: str | None = None
    domain: str | None = None
    page_identifier: str | None = Field(default=None, alias="pageIdentifier")
    mime_type: str | None = Field(default=None, alias="mimeType")
    document: str | None = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class GroundingReference(BaseModel):
    content: str | None = None
    document_metadata: DocumentMetadata | None = Field(
        default=None, alias="documentMetadata"
    )

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class GroundingSegment(BaseModel):
    text: str | None = None
    start_index: str | None = Field(default=None, alias="startIndex")
    end_index: str | None = Field(default=None, alias="endIndex")
    reference_indices: list[int] = Field(default_factory=list, alias="referenceIndices")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class TextGroundingMetadata(BaseModel):
    references: list[GroundingReference] = Field(default_factory=list)
    segments: list[GroundingSegment] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class InlineData(BaseModel):
    mime_type: str | None = Field(default=None, alias="mimeType")
    data: str | None = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class ReplyContent(BaseModel):
    role: str | None = None
    text: str | None = None
    thought: bool = False
    inline_data: InlineData | None = Field(default=None, alias="inlineData")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class GroundedContent(BaseModel):
    content: ReplyContent | None = None
    text_grounding_metadata: TextGroundingMetadata | None = Field(
        default=None, alias="textGroundingMetadata"
    )

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class Reply(BaseModel):
    grounded_content: GroundedContent | None = Field(
        default=None, alias="groundedContent"
    )
    create_time: str | None = Field(default=None, alias="createTime")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class DetailedAssistAnswer(BaseModel):
    name: str | None = None
    state: str | None = None
    replies: list[Reply] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class SessionTurn(BaseModel):
    query: Query | None = None
    detailed_assist_answer: DetailedAssistAnswer | None = Field(
        default=None, alias="detailedAssistAnswer"
    )
    query_config: dict[str, Any] | None = Field(default=None, alias="queryConfig")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    @property
    def answer_generation_mode(self) -> str:
        if not self.query_config:
            return ""
        return str(self.query_config.get(_ANSWER_MODE_KEY, ""))

    @property
    def model_id(self) -> str:
        if not self.query_config:
            return ""
        return str(self.query_config.get(_MODEL_ID_KEY, ""))


class Session(BaseModel):
    name: str
    state: str | None = None
    user_pseudo_id: str | None = Field(default=None, alias="userPseudoId")
    display_name: str | None = Field(default=None, alias="displayName")
    start_time: str | None = Field(default=None, alias="startTime")
    end_time: str | None = Field(default=None, alias="endTime")
    update_time: str | None = Field(default=None, alias="updateTime")
    turns: list[SessionTurn] = Field(default_factory=list)
    labels: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    @property
    def session_id(self) -> str:
        return self.name.rsplit("/", maxsplit=1)[-1]

    @property
    def user_id(self) -> str | None:
        return self.user_pseudo_id


class SessionListResponse(BaseModel):
    sessions: list[Session] = Field(default_factory=list)
    next_page_token: str | None = Field(default=None, alias="nextPageToken")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)
