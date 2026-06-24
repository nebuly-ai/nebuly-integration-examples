from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from copilot_sync import user_defined
from copilot_sync.converter import (
    InteractionTurn,
    SkipReason,
    group_interactions,
    turn_to_payload,
)
from copilot_sync.models import (
    AiInteraction,
    Attachment,
    CopilotUser,
    FromIdentitySet,
    InteractionBody,
    Link,
    TeamworkApplicationIdentity,
)


def _user() -> CopilotUser:
    return CopilotUser(id="user_1", mail="alice@example.com")


def _prompt(
    request_id: str = "req_1",
    *,
    content: str = "hello",
    session_id: str = "sess_1",
    minute: int = 0,
) -> AiInteraction:
    return AiInteraction(
        id=f"prompt_{request_id}_{minute}",
        request_id=request_id,
        session_id=session_id,
        interaction_type="userPrompt",
        app_class="IPM.SkypeTeams.Message.Copilot.Word",
        conversation_type="appchat",
        locale="en-US",
        created_datetime=datetime(2025, 6, 15, 10, minute, tzinfo=UTC),
        body=InteractionBody(content_type="text", content=content),
    )


def _response(
    request_id: str = "req_1",
    *,
    content: str = "hi there",
    model: str = "Microsoft 365 Chat",
    minute: int = 1,
    links: list[Link] | None = None,
) -> AiInteraction:
    return AiInteraction(
        id=f"response_{request_id}_{minute}",
        request_id=request_id,
        session_id="sess_1",
        interaction_type="aiResponse",
        app_class="IPM.SkypeTeams.Message.Copilot.Word",
        conversation_type="appchat",
        locale="en-US",
        created_datetime=datetime(2025, 6, 15, 10, minute, tzinfo=UTC),
        body=InteractionBody(content_type="text", content=content),
        sender=FromIdentitySet(
            application=TeamworkApplicationIdentity(displayName=model),
        ),
        links=links or [],
    )


def test_validates_record_with_null_context_reference() -> None:
    record = {
        "id": "1",
        "requestId": "req_1",
        "sessionId": "sess_1",
        "interactionType": "userPrompt",
        "conversationType": "appchat",
        "appClass": "IPM.SkypeTeams.Message.Copilot.PowerPoint",
        "locale": "en-us",
        "createdDateTime": "2026-06-24T13:25:59.946Z",
        "body": {"contentType": "text", "content": "hi"},
        "contexts": [
            {
                "contextReference": None,
                "displayName": "unknown-file-name",
                "contextType": "",
            },
        ],
        "links": [
            {
                "displayName": None,
                "linkType": None,
                "linkUrl": "https://example.com",
            },
        ],
    }
    inter = AiInteraction.model_validate(record)
    assert inter.contexts[0].context_reference is None
    assert inter.links[0].link_url == "https://example.com"


def test_groups_single_turn() -> None:
    turns, dangling = group_interactions([_prompt(), _response()])
    assert len(turns) == 1
    assert turns[0].prompt.interaction_type == "userPrompt"
    assert len(turns[0].responses) == 1
    assert dangling == []


def test_groups_multi_response_turn_ordered() -> None:
    responses = [
        _response(content="step1", minute=3),
        _response(content="step2", minute=2),
        _response(content="final", minute=4),
    ]
    turns, _ = group_interactions([_prompt(), *responses])
    assert len(turns) == 1
    turn = turns[0]
    assert [r.body.content for r in turn.responses] == ["step2", "step1", "final"]
    assert turn.final_response is not None
    assert turn.final_response.body.content == "final"
    assert turn.time_start == datetime(2025, 6, 15, 10, 0, tzinfo=UTC)
    assert turn.time_end == datetime(2025, 6, 15, 10, 4, tzinfo=UTC)


def test_dangling_prompt_reported() -> None:
    prompt = _prompt()
    turns, dangling = group_interactions([prompt])
    assert turns == []
    assert dangling == [prompt]


def test_orphan_response_dropped() -> None:
    turns, dangling = group_interactions([_response()])
    assert turns == []
    assert dangling == []


def test_duplicate_prompts_pick_non_empty() -> None:
    empty_prompt = _prompt(content="", minute=0)
    real_prompt = _prompt(content="real question", minute=0)
    turns, _ = group_interactions([empty_prompt, real_prompt, _response()])
    assert len(turns) == 1
    assert turns[0].prompt.body.content == "real question"


def test_turn_payload_shape() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(),))
    payload = turn_to_payload(turn, user=_user(), anonymize=False)

    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["conversation_id"] == "sess_1"
    assert payload["interaction"]["input"] == "hello"
    assert payload["interaction"]["output"] == "hi there"
    assert payload["interaction"]["end_user"] == "alice@example.com"
    assert payload["interaction"]["time_start"] == "2025-06-15T10:00:00Z"
    assert payload["interaction"]["time_end"] == "2025-06-15T10:01:00Z"
    assert payload["anonymize"] is False
    assert payload["user_feedback"] == []


def test_empty_input_returns_skip_reason() -> None:
    turn = InteractionTurn("req_1", _prompt(content=""), (_response(),))
    result = turn_to_payload(turn, user=_user(), anonymize=False)
    assert result is SkipReason.EMPTY_INPUT


def test_empty_output_returns_skip_reason() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(content=""),))
    result = turn_to_payload(turn, user=_user(), anonymize=False)
    assert result is SkipReason.EMPTY_OUTPUT


def test_build_tags() -> None:
    turn = InteractionTurn(
        "req_1",
        _prompt(),
        (_response(content="step"), _response(content="final", minute=2)),
    )
    tags = user_defined.build_tags(turn)

    assert tags["app_class"] == "IPM.SkypeTeams.Message.Copilot.Word"
    assert tags["session_id"] == "sess_1"
    assert tags["request_id"] == "req_1"
    assert tags["response_count"] == "2"
    assert tags["final_model"] == "Microsoft 365 Chat"


def test_build_traces_llm_then_retrieval() -> None:
    final = _response(
        content="final answer",
        minute=2,
        links=[
            Link(displayName="Example", linkType="web", linkUrl="https://example.com")
        ],
    )
    turn = InteractionTurn("req_1", _prompt(), (_response(content="step1"), final))
    traces = user_defined.build_traces(turn)

    llm = traces[0]
    assert llm["model"] == "Microsoft 365 Chat"
    assert llm["output"] == "final answer"
    assert llm["messages"] == [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "step1"},
        {"role": "assistant", "content": "final answer"},
    ]
    assert traces[1]["source"] == "https://example.com"


def test_build_traces_skips_empty_assistant_messages() -> None:
    turn = InteractionTurn(
        "req_1",
        _prompt(),
        (_response(content=""), _response(content="final", minute=2)),
    )
    llm = user_defined.build_traces(turn)[0]
    assert llm["messages"] == [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "final"},
    ]


def test_user_defined_hooks_in_payload() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(),))
    custom_tags = {"custom": "tag"}
    custom_traces = [{"source": "kb", "input": "q", "outputs": ["a"]}]
    custom_feedback = [{"slug": "thumbs_up", "text": "nice"}]

    with (
        patch.object(user_defined, "build_tags", return_value=custom_tags),
        patch.object(user_defined, "build_traces", return_value=custom_traces),
        patch.object(user_defined, "build_user_feedback", return_value=custom_feedback),
    ):
        payload = turn_to_payload(turn, user=_user(), anonymize=False)

    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["tags"] == custom_tags
    assert payload["traces"] == custom_traces
    assert payload["user_feedback"] == custom_feedback


def test_adaptive_card_response_parsed_as_output() -> None:
    card = (
        '{"type":"AdaptiveCard","version":"1.0",'
        '"body":[{"type":"TextBlock","text":"Risposta dalla card"}]}'
    )
    final = AiInteraction(
        id="r1",
        request_id="req_1",
        session_id="sess_1",
        interaction_type="aiResponse",
        app_class="IPM.SkypeTeams.Message.Copilot.BizChat",
        conversation_type="bizchat",
        locale="en-us",
        created_datetime=datetime(2025, 6, 15, 10, 2, tzinfo=UTC),
        body=InteractionBody(
            content_type="html",
            content='<attachment id="c1"></attachment>',
        ),
        attachments=[
            Attachment(
                attachmentId="c1",
                content=card,
                contentType="application/vnd.microsoft.card.adaptive",
            ),
        ],
    )
    turn = InteractionTurn("req_1", _prompt(), (final,))
    payload = turn_to_payload(turn, user=_user(), anonymize=False)
    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["output"] == "Risposta dalla card"
