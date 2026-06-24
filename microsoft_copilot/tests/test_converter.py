from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from copilot_sync import user_defined
from copilot_sync.converter import (
    InteractionPair,
    pair_interactions,
    pair_to_payload,
)
from copilot_sync.models import (
    AiInteraction,
    Attachment,
    CopilotUser,
    InteractionBody,
    Link,
)


def _user() -> CopilotUser:
    return CopilotUser(id="user_1", mail="alice@example.com")


def _prompt(
    request_id: str = "req_1",
    *,
    content: str = "hello",
    session_id: str = "sess_1",
) -> AiInteraction:
    return AiInteraction(
        id=f"prompt_{request_id}",
        request_id=request_id,
        session_id=session_id,
        interaction_type="userPrompt",
        app_class="IPM.SkypeTeams.Message.Copilot.Word",
        conversation_type="appchat",
        locale="en-US",
        created_datetime=datetime(2025, 6, 15, 10, 0, tzinfo=UTC),
        body=InteractionBody(content_type="text", content=content),
        attachments=[
            Attachment(
                attachmentId="att_1",
                content="",
                contentType="application/pdf",
                contentUrl="https://example.com/doc.pdf",
                name="doc.pdf",
            ),
        ],
    )


def _response(
    request_id: str = "req_1",
    *,
    content: str = "hi there",
) -> AiInteraction:
    return AiInteraction(
        id=f"response_{request_id}",
        request_id=request_id,
        session_id="sess_1",
        interaction_type="aiResponse",
        app_class="IPM.SkypeTeams.Message.Copilot.Word",
        conversation_type="appchat",
        locale="en-US",
        created_datetime=datetime(2025, 6, 15, 10, 1, tzinfo=UTC),
        body=InteractionBody(content_type="text", content=content),
        links=[
            Link(
                displayName="Example",
                linkType="web",
                linkUrl="https://example.com",
            ),
        ],
    )


def test_pairs_prompt_before_response() -> None:
    pairs = pair_interactions([_prompt(), _response()])
    assert len(pairs) == 1
    assert pairs[0].prompt.interaction_type == "userPrompt"
    assert pairs[0].response.interaction_type == "aiResponse"


def test_pairs_response_before_prompt() -> None:
    pairs = pair_interactions([_response(), _prompt()])
    assert len(pairs) == 1


def test_orphan_prompt_dropped() -> None:
    pairs = pair_interactions([_prompt()])
    assert pairs == []


def test_orphan_response_dropped() -> None:
    pairs = pair_interactions([_response()])
    assert pairs == []


def test_payload_shape() -> None:
    pair = InteractionPair(_prompt(), _response())
    payload = pair_to_payload(pair, user=_user(), anonymize=False)

    assert payload is not None
    assert payload["interaction"]["conversation_id"] == "sess_1"
    assert payload["interaction"]["input"] == "hello"
    assert payload["interaction"]["output"] == "hi there"
    assert payload["interaction"]["end_user"] == "alice@example.com"
    assert payload["interaction"]["hide_content"] is False
    assert payload["anonymize"] is False
    assert payload["user_feedback"] == []


def test_empty_input_skipped() -> None:
    pair = InteractionPair(_prompt(content=""), _response())
    assert pair_to_payload(pair, user=_user(), anonymize=False) is None


def test_build_tags() -> None:
    pair = InteractionPair(_prompt(), _response())
    tags = user_defined.build_tags(pair)

    assert tags["app_class"] == "IPM.SkypeTeams.Message.Copilot.Word"
    assert tags["conversation_type"] == "appchat"
    assert tags["session_id"] == "sess_1"
    assert tags["request_id"] == "req_1"
    assert tags["user_attachments_count"] == "1"


def test_build_traces_from_response() -> None:
    pair = InteractionPair(_prompt(), _response())
    traces = user_defined.build_traces(pair)

    assert len(traces) == 1
    assert traces[0]["source"] == "https://example.com"


def test_user_defined_hooks_in_payload() -> None:
    pair = InteractionPair(_prompt(), _response())
    custom_tags = {"custom": "tag"}
    custom_traces = [{"source": "kb", "input": "q", "outputs": ["a"]}]
    custom_feedback = [{"slug": "thumbs_up", "text": "nice"}]

    with (
        patch.object(user_defined, "build_tags", return_value=custom_tags),
        patch.object(user_defined, "build_traces", return_value=custom_traces),
        patch.object(user_defined, "build_user_feedback", return_value=custom_feedback),
    ):
        payload = pair_to_payload(pair, user=_user(), anonymize=False)

    assert payload is not None
    assert payload["interaction"]["tags"] == custom_tags
    assert payload["traces"] == custom_traces
    assert payload["user_feedback"] == custom_feedback
