from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

from copilot_sync import user_defined
from copilot_sync.config import datetime_to_timestamp_str
from copilot_sync.converter import (
    InteractionTurn,
    SkipReason,
    group_interactions,
    turn_to_payload,
)
from copilot_sync.models import (
    AiInteraction,
    Attachment,
    CopilotAuditRecord,
    CopilotUser,
    FromIdentitySet,
    InteractionBody,
    Link,
    TeamworkApplicationIdentity,
)


def _user(
    *,
    department: str | None = None,
    job_title: str | None = None,
    office_location: str | None = None,
    city: str | None = None,
    country: str | None = None,
    usage_location: str | None = None,
    company_name: str | None = None,
    employee_hire_date: datetime | None = None,
) -> CopilotUser:
    return CopilotUser(
        id="user_1",
        mail="alice@example.com",
        department=department,
        jobTitle=job_title,
        officeLocation=office_location,
        city=city,
        country=country,
        usageLocation=usage_location,
        companyName=company_name,
        employeeHireDate=employee_hire_date,
    )


def _prompt(
    request_id: str = "req_1",
    *,
    content: str = "hello",
    session_id: str = "sess_1",
    minute: int = 0,
    second: int = 0,
) -> AiInteraction:
    return AiInteraction(
        id=f"prompt_{request_id}_{minute}",
        request_id=request_id,
        session_id=session_id,
        interaction_type="userPrompt",
        app_class="IPM.SkypeTeams.Message.Copilot.Word",
        conversation_type="appchat",
        locale="en-US",
        created_datetime=datetime(2025, 6, 15, 10, minute, second, tzinfo=UTC),
        body=InteractionBody(content_type="text", content=content),
    )


def _response(
    request_id: str = "req_1",
    *,
    content: str = "hi there",
    model: str = "Microsoft 365 Chat",
    minute: int = 1,
    second: int = 0,
    session_id: str = "sess_1",
    links: list[Link] | None = None,
) -> AiInteraction:
    return AiInteraction(
        id=f"response_{request_id}_{minute}",
        request_id=request_id,
        session_id=session_id,
        interaction_type="aiResponse",
        app_class="IPM.SkypeTeams.Message.Copilot.Word",
        conversation_type="appchat",
        locale="en-US",
        created_datetime=datetime(2025, 6, 15, 10, minute, second, tzinfo=UTC),
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


def test_cross_request_id_pair_grouped() -> None:
    turns, dangling = group_interactions(
        [_prompt("req_a", minute=0), _response("req_b", minute=1)]
    )
    assert len(turns) == 1
    assert dangling == []
    assert turns[0].responses[0].request_id == "req_b"


def test_interleaved_sessions_not_merged() -> None:
    turns, dangling = group_interactions(
        [
            _prompt("req_a", session_id="sess_a", minute=0),
            _prompt("req_b", session_id="sess_b", minute=1),
            _response("req_a", session_id="sess_a", minute=2),
            _response("req_b", session_id="sess_b", minute=3),
        ]
    )
    assert len(turns) == 2
    assert dangling == []
    by_session = {t.prompt.session_id: t for t in turns}
    assert by_session["sess_a"].prompt.request_id == "req_a"
    assert by_session["sess_a"].responses[0].request_id == "req_a"
    assert by_session["sess_b"].prompt.request_id == "req_b"
    assert by_session["sess_b"].responses[0].request_id == "req_b"


def test_unanswered_prompt_midconversation_skipped() -> None:
    turns, dangling = group_interactions(
        [_prompt(minute=0), _prompt(minute=2), _response(minute=3)]
    )
    assert len(turns) == 1
    assert dangling == []
    assert turns[0].prompt.created_datetime == datetime(2025, 6, 15, 10, 2, tzinfo=UTC)


def test_trailing_unanswered_prompt_is_dangling() -> None:
    trailing = _prompt(minute=2)
    turns, dangling = group_interactions(
        [_prompt(minute=0), _response(minute=1), trailing]
    )
    assert len(turns) == 1
    assert dangling == [trailing]


def test_consecutive_empty_then_real_prompt_collapsed() -> None:
    empty_prompt = _prompt(content="", minute=0)
    real_prompt = _prompt(content="real question", minute=0)
    turns, dangling = group_interactions(
        [empty_prompt, real_prompt, _response(minute=1)]
    )
    assert len(turns) == 1
    assert dangling == []
    assert turns[0].prompt.body.content == "real question"


def test_turn_payload_shape() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(),))
    payload = turn_to_payload(turn, user=_user(), anonymize=False)

    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["conversation_id"] == "sess_1"
    assert payload["interaction"]["input"] == "hello"
    assert payload["interaction"]["output"] == "hi there"
    assert payload["interaction"]["end_user"] == "user_1"
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


def test_warmup_output_returns_skip_reason() -> None:
    turn = InteractionTurn(
        "req_1",
        _prompt(),
        (_response(content='{"IsWarmupRequest":"true"}'),),
    )
    result = turn_to_payload(turn, user=_user(), anonymize=False)
    assert result is SkipReason.WARMUP_REQUEST


def test_warmup_final_response_uses_earlier_real_output() -> None:
    real = _response(
        content="real answer",
        minute=1,
        model="Microsoft 365 Chat",
        links=[
            Link(displayName="Example", linkType="web", linkUrl="https://example.com")
        ],
    )
    turn = InteractionTurn(
        "req_1",
        _prompt(),
        (
            real,
            _response(
                content='{"IsWarmupRequest":"true"}',
                minute=2,
                model="Wrong Model",
            ),
        ),
    )
    payload = turn_to_payload(turn, user=_user(), anonymize=False)
    assert not isinstance(payload, SkipReason)
    assert payload["interaction"]["output"] == "real answer"
    assert payload["interaction"]["time_end"] == datetime_to_timestamp_str(
        real.created_datetime
    )
    assert payload["interaction"]["tags"]["copilot_app"] == "Microsoft 365 Chat"
    assert len(payload["traces"]) == 1
    assert payload["traces"][0]["source"] == "https://example.com"


def test_build_tags() -> None:
    turn = InteractionTurn(
        "req_1",
        _prompt(),
        (_response(content="step"), _response(content="final", minute=2)),
    )
    user = _user(
        department="Engineering",
        job_title="Senior Engineer",
        office_location="Building 1",
        city="Milan",
        country="IT",
        company_name="Contoso",
        employee_hire_date=datetime(2020, 3, 15, tzinfo=UTC),
    )
    tags = user_defined.build_tags(turn, user)

    assert tags["app_class"] == "IPM.SkypeTeams.Message.Copilot.Word"
    assert tags["session_id"] == "sess_1"
    assert tags["request_id"] == "req_1"
    assert tags["copilot_app"] == "Microsoft 365 Chat"
    assert tags["department"] == "Engineering"
    assert tags["job_title"] == "Senior Engineer"
    assert tags["office_location"] == "Building 1"
    assert tags["city"] == "Milan"
    assert tags["country"] == "IT"
    assert tags["company_name"] == "Contoso"
    assert tags["employee_hire_date"] == "2020-03-15"


def test_build_tags_country_falls_back_to_usage_location() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(),))
    user = _user(country=None, usage_location="US")
    tags = user_defined.build_tags(turn, user)
    assert tags["country"] == "US"


def test_build_tags_org_fields_none_when_missing() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(),))
    tags = user_defined.build_tags(turn, _user())
    for key in (
        "department",
        "job_title",
        "office_location",
        "city",
        "country",
        "company_name",
        "employee_hire_date",
    ):
        assert key not in tags


def test_build_traces_retrieval_only() -> None:
    final = _response(
        content="final answer",
        minute=2,
        links=[
            Link(displayName="Example", linkType="web", linkUrl="https://example.com")
        ],
    )
    turn = InteractionTurn("req_1", _prompt(), (_response(content="step1"), final))
    traces = user_defined.build_traces(turn)

    assert len(traces) == 1
    assert traces[0]["source"] == "https://example.com"
    assert "messages" not in traces[0]
    assert "model" not in traces[0]


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


def test_near_duplicate_turn_dropped() -> None:
    turns, dangling = group_interactions(
        [
            _prompt("req_a", minute=0, second=0),
            _response("req_a", minute=0, second=1),
            _prompt("req_b", minute=0, second=2),
            _response("req_b", minute=0, second=3),
        ]
    )
    assert len(turns) == 1
    assert dangling == []
    assert turns[0].prompt.request_id == "req_a"


def test_near_duplicate_dropped_when_trailing_response_is_warmup() -> None:
    turns, dangling = group_interactions(
        [
            _prompt("req_a", minute=0, second=0),
            _response("req_a", minute=0, second=1),
            _prompt("req_b", minute=0, second=2),
            _response("req_b", minute=0, second=3),
            _response(
                "req_b",
                content='{"IsWarmupRequest":"true"}',
                minute=0,
                second=4,
            ),
        ]
    )
    assert len(turns) == 1
    assert dangling == []


def test_outside_duplicate_window_kept() -> None:
    turns, dangling = group_interactions(
        [
            _prompt("req_a", minute=0, second=0),
            _response("req_a", minute=0, second=1),
            _prompt("req_b", minute=0, second=10),
            _response("req_b", minute=0, second=11),
        ]
    )
    assert len(turns) == 2
    assert dangling == []


def test_near_duplicate_different_session_kept() -> None:
    turns, dangling = group_interactions(
        [
            _prompt("req_a", session_id="sess_a", minute=0, second=0),
            _response("req_a", session_id="sess_a", minute=0, second=1),
            _prompt("req_b", session_id="sess_b", minute=0, second=2),
            _response("req_b", session_id="sess_b", minute=0, second=3),
        ]
    )
    assert len(turns) == 2
    assert dangling == []


def test_near_duplicate_different_output_kept() -> None:
    turns, dangling = group_interactions(
        [
            _prompt("req_a", minute=0, second=0),
            _response("req_a", content="answer one", minute=0, second=1),
            _prompt("req_b", minute=0, second=2),
            _response("req_b", content="answer two", minute=0, second=3),
        ]
    )
    assert len(turns) == 2
    assert dangling == []


def _audit_record_for_turn(turn: InteractionTurn) -> CopilotAuditRecord:
    final = turn.final_response
    assert final is not None
    return CopilotAuditRecord.model_validate(
        {
            "id": "audit-1",
            "createdDateTime": "2025-06-15T10:01:00Z",
            "auditData": {
                "CopilotEventData": {
                    "AppHost": "Teams",
                    "Messages": [
                        {"Id": turn.prompt.id, "isPrompt": True},
                        {"Id": final.id, "isPrompt": False},
                    ],
                    "ModelTransparencyDetails": [
                        {"ModelProviderName": "OpenAI", "ModelName": "gpt-test"},
                    ],
                    "AISystemPlugin": [{"Id": "BingWebSearch", "Name": "BuiltIn"}],
                    "AccessedResources": [
                        {
                            "Name": "Doc.docx",
                            "SiteUrl": "https://contoso.sharepoint.com/doc",
                        },
                    ],
                },
            },
        },
    )


def test_turn_to_payload_with_audit_adds_model_tags_and_llm_trace() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(),))
    audit = _audit_record_for_turn(turn)
    payload = turn_to_payload(turn, user=_user(), anonymize=False, audit=audit)
    assert not isinstance(payload, SkipReason)
    tags = payload["interaction"]["tags"]
    assert tags["model_name"] == "gpt-test"
    assert tags["model_provider"] == "OpenAI"
    assert tags["app_host"] == "Teams"
    assert tags["web_search"] == "true"
    traces = payload["traces"]
    assert any(t.get("model") == "gpt-test" for t in traces)
    assert any(t.get("source") == "Doc.docx" for t in traces)


def test_turn_to_payload_without_audit_unchanged_except_copilot_app_tag() -> None:
    turn = InteractionTurn("req_1", _prompt(), (_response(),))
    payload = turn_to_payload(turn, user=_user(), anonymize=False)
    assert not isinstance(payload, SkipReason)
    tags = payload["interaction"]["tags"]
    assert "model_name" not in tags
    assert tags["copilot_app"] == "Microsoft 365 Chat"
    assert not any("model" in t for t in payload["traces"])


def test_audit_retrieval_dedupes_graph_link() -> None:
    link = Link(displayName="Example", linkType="web", linkUrl="https://example.com")
    final = _response(content="answer", links=[link])
    turn = InteractionTurn("req_1", _prompt(), (final,))
    audit = CopilotAuditRecord.model_validate(
        {
            "id": "audit-dedupe",
            "createdDateTime": "2025-06-15T10:01:00Z",
            "auditData": {
                "AccessedResources": [
                    {"Name": "Example", "SiteUrl": "https://example.com"},
                ],
            },
        },
    )
    traces = user_defined.build_traces(
        turn,
        audit=audit,
        user_input="hello",
        assistant_output="answer",
    )
    link_traces = [t for t in traces if t.get("input") == "https://example.com"]
    assert len(link_traces) == 1


_COWORK_APP_CLASS = "IPM.SkypeTeams.Message.Copilot.CoworkChat"


def test_cowork_chats_validate_and_become_nebuly_payloads(
    synthetic_cowork_interactions: list[dict[str, Any]],
) -> None:
    interactions = sorted(
        [AiInteraction.model_validate(item) for item in synthetic_cowork_interactions],
        key=lambda item: item.created_datetime,
    )
    assert all(item.request_id is None for item in interactions)

    turns, dangling = group_interactions(interactions)
    assert dangling == []
    assert len(turns) == 6

    user = _user()
    sent: list[dict[str, object]] = []
    skipped: list[SkipReason] = []
    for turn in turns:
        result = turn_to_payload(turn, user=user, anonymize=False)
        if isinstance(result, SkipReason):
            skipped.append(result)
            continue
        sent.append(result)

    assert skipped == [SkipReason.EMPTY_OUTPUT]
    assert len(sent) == 5

    expected = (
        ("Hello", "Hello back from Cowork."),
        ("What can you do?", "I can help with calendars"),
        ("Help me organize my week.", "I'll review your calendar"),
        ("How do I add an MCP to Copilot Cowork?", "You add an MCP server"),
        ("```", "Created **example-connector.zip**"),
    )
    for input_prefix, output_prefix in expected:
        matches = [
            payload
            for payload in sent
            if _interaction_text(payload, "input").startswith(input_prefix)
        ]
        assert len(matches) == 1
        payload = matches[0]
        output = _interaction_text(payload, "output")
        assert output.startswith(output_prefix)
        assert "Finding an efficient solution" not in output
        assert "Addressing schema issues" not in output
        interaction = payload["interaction"]
        assert isinstance(interaction, dict)
        assert str(interaction["conversation_id"]).endswith("@thread.v2")
        tags = interaction["tags"]
        assert isinstance(tags, dict)
        assert tags["conversation_type"] == "coworkchat"
        assert tags["app_class"] == _COWORK_APP_CLASS
        assert "request_id" not in tags
        assert payload["traces"] == []


def _interaction_text(payload: dict[str, object], field: str) -> str:
    interaction = payload["interaction"]
    assert isinstance(interaction, dict)
    text = interaction[field]
    assert isinstance(text, str)
    return text
