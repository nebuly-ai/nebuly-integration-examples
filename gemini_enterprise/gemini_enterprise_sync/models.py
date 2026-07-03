from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


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


class GroundedContent(BaseModel):
    text_grounding_metadata: TextGroundingMetadata | None = Field(
        default=None, alias="textGroundingMetadata"
    )

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class Reply(BaseModel):
    grounded_content: GroundedContent | None = Field(
        default=None, alias="groundedContent"
    )

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class Answer(BaseModel):
    name: str | None = None
    state: str | None = None
    replies: list[Reply] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class Response(BaseModel):
    answer: Answer | None = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class QueryPart(BaseModel):
    text: str | None = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class Query(BaseModel):
    parts: list[QueryPart] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class Request(BaseModel):
    query: Query | None = None
    name: str | None = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class LogMetadata(BaseModel):
    method_name: str | None = Field(default=None, alias="methodName")
    service_label: str | None = Field(default=None, alias="serviceLabel")
    service_name: str | None = Field(default=None, alias="serviceName")
    timestamp: str | None = None
    name: str | None = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class LogPayload(BaseModel):
    request: Request | None = None
    service_text_reply: str | None = Field(default=None, alias="serviceTextReply")
    response: Response | None = None
    user_iam_principal: str | None = Field(default=None, alias="userIamPrincipal")
    log_metadata: LogMetadata | None = Field(default=None, alias="logMetadata")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)
