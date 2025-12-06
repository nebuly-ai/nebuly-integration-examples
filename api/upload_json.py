import asyncio
import logging
import sys
from datetime import datetime
from typing import Annotated, Literal, Union

import aiohttp
from pydantic import BaseModel, Field, RootModel

"""
Upload a JSONL file to the Nebuly API.
The JSONL file should be in the following format:
[{
    "interaction": {
        "conversation_id": "1234567890",
        "input": "Hello, how are you?",
        "output": "I'm good, thank you!",
        "time_start": "2025-01-01T00:00:00Z",
        "time_end": "2025-01-01T00:00:00Z",
        "end_user": "1234567890",
        "tags": {"custom_tag": "custom_value"}
    },
    "traces": [
        {
            "source": "llm",
            "input": "Hello, how are you?",
            "outputs": ["I'm good, thank you!"],
        }
    ]
}]

Usage: python upload_json.py <file_path> <api_key>

Requirements (pip install/poetry add/...):
- aiohttp
- pydantic
"""

API_URL = "https://backend.dev.nebuly.com/event-ingestion/api/v2/events/trace_interaction"
GROUP_SIZE = 20
DELAY_MS = 100

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TracesTruncatedString = Annotated[str, str]


class Message(BaseModel):
    role: str = Field(title="Role")
    content: str = Field(title="Content")


class PreviousInteractions(BaseModel):
    input: str = Field(title="Input")
    output: str = Field(title="Output")


class LLMTraceV2(BaseModel):
    model: str = Field(title="Model")
    messages: list[Message] = Field(title="Messages")
    output: TracesTruncatedString = Field(title="Output")
    input_tokens: int | None = Field(title="Input Tokens", default=None)
    output_tokens: int | None = Field(title="Output Tokens", default=None)


class RetrievalTrace(BaseModel):
    source: str = Field(title="Source")
    input: TracesTruncatedString = Field(title="Input")
    outputs: list[TracesTruncatedString] = Field(title="Outputs")


class EmbeddingTrace(BaseModel):
    model: str = Field(title="Model")
    input: TracesTruncatedString = Field(title="Input")
    input_tokens: int | None = Field(title="Input Tokens", default=None)


TracesV2 = Union[LLMTraceV2, RetrievalTrace, EmbeddingTrace]


class ThumbsUpFeedbackAction(BaseModel):
    slug: Literal["thumbs_up"] = Field(default="thumbs_up", title="Slug")
    text: str | None = Field(title="Text", default=None)


class ThumbsDownFeedbackAction(BaseModel):
    slug: Literal["thumbs_down"] = Field(default="thumbs_down", title="Slug")
    text: str | None = Field(title="Text", default=None)


class CopyInputFeedbackAction(BaseModel):
    slug: Literal["copy_input"] = Field(default="copy_input", title="Slug")
    text: str = Field(title="Text")


class CopyOutputFeedbackAction(BaseModel):
    slug: Literal["copy_output"] = Field(default="copy_output", title="Slug")
    text: str = Field(title="Text")


class PasteFeedbackAction(BaseModel):
    slug: Literal["paste"] = Field(default="paste", title="Slug")
    text: str = Field(title="Text")


class UserCommentAction(BaseModel):
    slug: Literal["comment"] = Field(default="comment", title="Slug")
    text: str = Field(title="Text")


class RegenerateAction(BaseModel):
    slug: Literal["regenerate"] = Field(default="regenerate", title="Slug")


class EditAction(BaseModel):
    slug: Literal["edit"] = Field(default="edit", title="Slug")
    text: str = Field(title="Text")


class RatingAction(BaseModel):
    slug: Literal["rating"] = Field(default="rating", title="Slug")
    value: int = Field(title="Value")
    text: str | None = Field(title="Text", default=None)


FeedbackActionType = Union[
    ThumbsUpFeedbackAction,
    ThumbsDownFeedbackAction,
    CopyInputFeedbackAction,
    CopyOutputFeedbackAction,
    PasteFeedbackAction,
    UserCommentAction,
    RegenerateAction,
    EditAction,
    RatingAction,
]


class TraceInteractionV2(BaseModel):
    input: str = Field(description="Input prompt from the user", title="Input")
    output: str = Field(description="Output from the LLM model", title="Output")
    previous_interactions: list[PreviousInteractions] | None = Field(
        description="Previous interactions",
        default=None,
        title="Previous Interactions",
    )
    time_start: datetime = Field(
        description="When the end-user request process started",
        title="Time Start",
    )
    time_end: datetime = Field(
        description="When the end-user request process ended",
        title="Time End",
    )
    end_user: str = Field(description="End user", title="End User")
    end_user_group_profile: str | None = Field(
        description="End user group profile",
        default=None,
        title="End User Group Profile",
    )
    tags: dict[str, str] | None = Field(
        description="Custom Tags",
        default=None,
        title="Tags",
        json_schema_extra={"example": {"custom_tag": "custom_value"}},
    )
    feature_flags: list[str] | None = Field(
        description="Feature flags",
        default=None,
        title="Feature Flags",
    )
    conversation_id: str | None = Field(
        description="Conversation ID",
        default=None,
        title="Conversation Id",
    )


class TraceInteractionSchemaV2(BaseModel):
    interaction: TraceInteractionV2 = Field(title="Interaction")
    traces: list[TracesV2] = Field(title="Traces")
    feedback_actions: list[FeedbackActionType] | None = Field(
        title="Feedback Actions",
        default=None,
    )
    anonymize: bool = Field(title="Anonymize", default=False)


class TraceInteractions(RootModel[list[TraceInteractionSchemaV2]]):
    pass


def load_file(file_path: str) -> str:
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = file.read()
        return data
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error loading file {file_path}: {str(e)}")


# --- Recommended way to define your request function ---
async def make_post_request(
    session: aiohttp.ClientSession, url: str, payload_data: dict, headers: dict
):
    """
    Makes a single POST request and handles its response.
    """
    try:
        async with session.post(url, json=payload_data, headers=headers) as response:
            text = await response.text()
            if response.status != 200:
                logger.error(f"HTTP Error {response.status}: {text}")
                raise Exception(f"HTTP Error {response.status}: {text}")
            return response.status
    except aiohttp.ClientResponseError as e:
        logger.error(f"HTTP Error for {url}: {e.status} - {e.message}")
        raise
    except aiohttp.ClientError as e:
        logger.error(f"Network or Client Error for {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred for {url}: {e}")
        raise


async def main():
    logger.info("Starting upload")
    if len(sys.argv) != 3:
        logger.error("Usage: python upload_json.py <file_path> <api_key>")
        sys.exit(1)

    file_path = sys.argv[1]
    api_key = sys.argv[2]
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    data = load_file(file_path)

    trace_interactions = TraceInteractions.model_validate_json(data)
    async with aiohttp.ClientSession() as session:  # type: ignore
        for i in range(0, len(trace_interactions.root), GROUP_SIZE):
            group = trace_interactions.root[i : i + GROUP_SIZE]
            logger.info(f"Uploading {i + GROUP_SIZE} of {len(trace_interactions.root)}")
            results = await asyncio.gather(
                *[
                    make_post_request(
                        session, API_URL, payload.model_dump(mode="json"), headers
                    )
                    for payload in group
                ]
            )

            for result in results:
                if result != 200:
                    logger.error(f"Error uploading group {i}: {result}")

            await asyncio.sleep(DELAY_MS / 1000)


if __name__ == "__main__":
    asyncio.run(main())
