from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from copilot_sync import user_defined
from copilot_sync.converter import InteractionTurn
from copilot_sync.models import (
    AiInteraction,
    CopilotAuditRecord,
    CopilotUser,
    InteractionBody,
)

_COWORK = "IPM.SkypeTeams.Message.Copilot.CoworkChat"
_CALL_X = "aev1.call-x"
_CALL_Y = "aev1.call-y"
_CALL_Z = "aev1.call-z"


def _interaction(
    interaction_id: str,
    kind: str,
    second: int,
    content: str,
) -> AiInteraction:
    return AiInteraction(
        id=interaction_id,
        request_id=None,
        session_id="19:synthetic@thread.v2",
        interaction_type=kind,
        app_class=_COWORK,
        conversation_type="coworkchat",
        locale="en-us",
        created_datetime=datetime(2026, 1, 15, 10, 0, second, tzinfo=UTC),
        body=InteractionBody(content_type="text", content=content),
    )


def _turn() -> InteractionTurn:
    return InteractionTurn(
        None,
        _interaction("p1", "userPrompt", 0, "first prompt"),
        (_interaction("r1", "aiResponse", 5, "final answer"),),
        (_interaction("p2", "userPrompt", 3, "second prompt"),),
    )


def _resource(call_id: str, name: str, event: str) -> dict[str, str]:
    return {"Id": call_id, "Name": name, "Type": event}


def _record(
    record_id: str,
    created: str,
    message_ids: list[str],
    *,
    plugins: list[str],
    resources: list[dict[str, str]],
    model: str | None = None,
) -> CopilotAuditRecord:
    event: dict[str, Any] = {
        "ThreadId": "19:synthetic@thread.v2",
        "Messages": [
            {"Id": mid, "isPrompt": mid.startswith("p")} for mid in message_ids
        ],
        "AISystemPlugin": [{"Id": name, "Name": name} for name in plugins],
        "AccessedResources": resources,
    }
    if model is not None:
        event["ModelTransparencyDetails"] = [
            {"ModelProviderName": "OpenAI", "ModelName": model},
        ]
    return CopilotAuditRecord.model_validate(
        {
            "id": record_id,
            "createdDateTime": created,
            "auditData": {"CopilotEventData": event},
        },
    )


def record_a() -> CopilotAuditRecord:
    return _record(
        "audit-a",
        "2026-01-15T10:00:10Z",
        ["p1"],
        plugins=[
            "Skill",
            "TaskCreate",
            "TaskUpdate",
            "core-RunScript",
            "mcp__business_data__ExecuteSql",
            "mcp__outlook_calendar__UpdateEvent",
        ],
        resources=[
            _resource(_CALL_X, "outlook_calendar/UpdateEvent", "approvalRequested"),
            _resource(_CALL_X, "outlook_calendar/UpdateEvent", "approvalGrantedOnce"),
            _resource(_CALL_Y, "m365_teams/PostMessage", "approvalRequested"),
        ],
    )


def record_b() -> CopilotAuditRecord:
    return _record(
        "audit-b",
        "2026-01-15T10:02:00Z",
        ["p2", "r1"],
        plugins=["mcp__m365_teams_deferred__PostMessage"],
        resources=[
            _resource(_CALL_Y, "m365_teams/PostMessage", "approvalDenied"),
            _resource(_CALL_Z, "m365_teams/PostMessage", "approvalRequested"),
            _resource(_CALL_Z, "m365_teams/PostMessage", "approvalGrantedOnce"),
        ],
        model="gpt-synthetic",
    )


def _merged() -> CopilotAuditRecord:
    return CopilotAuditRecord.merge([record_a(), record_b()])


def _traces(audit: CopilotAuditRecord) -> list[dict[str, Any]]:
    return user_defined.build_traces(
        _turn(),
        audit=audit,
        user_input="in",
        assistant_output="out",
    )


def _retrieval(traces: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for trace in traces:
        if "source" in trace:
            by_source.setdefault(trace["source"], []).append(trace)
    return by_source


def test_merge_concatenates_resources_and_unions_plugins() -> None:
    merged = _merged()
    assert [r.resource_id for r in merged.accessed_resources] == [
        _CALL_X,
        _CALL_X,
        _CALL_Y,
        _CALL_Y,
        _CALL_Z,
        _CALL_Z,
    ]
    assert [p.plugin_id for p in merged.ai_system_plugins] == [
        "Skill",
        "TaskCreate",
        "TaskUpdate",
        "core-RunScript",
        "mcp__business_data__ExecuteSql",
        "mcp__outlook_calendar__UpdateEvent",
        "mcp__m365_teams_deferred__PostMessage",
    ]
    assert merged.message_ids == ["p1", "p2", "r1"]
    assert merged.thread_id == "19:synthetic@thread.v2"
    assert merged.id == "audit-a"


def test_merge_dedupes_plugins_by_id() -> None:
    merged = CopilotAuditRecord.merge([record_a(), record_a()])
    ids = [p.plugin_id for p in merged.ai_system_plugins]
    assert len(ids) == len(set(ids))


def test_merge_takes_first_model_and_first_non_null_scalars() -> None:
    first = _record("a", "2026-01-15T10:00:00Z", ["p1"], plugins=[], resources=[])
    second = _record(
        "b",
        "2026-01-15T10:01:00Z",
        ["r1"],
        plugins=[],
        resources=[],
        model="gpt-synthetic",
    )
    second = second.model_copy(update={"app_host": "Teams"})
    merged = CopilotAuditRecord.merge([first, second])
    assert merged.app_host == "Teams"
    primary = merged.primary_model
    assert primary is not None
    assert primary.model_name == "gpt-synthetic"


def test_merge_ors_boolean_flags() -> None:
    flagged = CopilotAuditRecord.model_validate(
        {
            "id": "flagged",
            "createdDateTime": "2026-01-15T10:00:00Z",
            "auditData": {
                "Messages": [{"Id": "p1", "isPrompt": True, "JailbreakDetected": True}],
                "AccessedResources": [{"Name": "Doc", "XPIADetected": True}],
                "AISystemPlugin": [{"Id": "BingWebSearch", "Name": "BuiltIn"}],
            },
        },
    )
    merged = CopilotAuditRecord.merge([record_a(), flagged])
    assert merged.jailbreak_detected is True
    assert merged.xpia_detected is True
    assert merged.web_search_used is True


def test_one_call_trace_per_call_with_final_status() -> None:
    by_source = _retrieval(_traces(_merged()))
    assert by_source["outlook_calendar/UpdateEvent"] == [
        {
            "source": "outlook_calendar/UpdateEvent",
            "input": "",
            "outputs": ["approvalGrantedOnce"],
        },
    ]
    post_outputs = [t["outputs"] for t in by_source["m365_teams/PostMessage"]]
    assert post_outputs == [["approvalDenied"], ["approvalGrantedOnce"]]


def test_call_traces_do_not_produce_document_traces() -> None:
    traces = _traces(_merged())
    assert not any(t.get("input") == "m365_teams/PostMessage" for t in traces)
    assert not any(t.get("outputs") == ["outlook_calendar/UpdateEvent"] for t in traces)


def test_plugin_only_tools_appear_once_with_empty_outputs() -> None:
    by_source = _retrieval(_traces(_merged()))
    for source in ("business_data/ExecuteSql", "core/RunScript"):
        assert by_source[source] == [{"source": source, "input": "", "outputs": []}]


def test_deferred_plugin_does_not_duplicate_call_traces() -> None:
    traces = _traces(_merged())
    assert "m365_teams_deferred/PostMessage" not in _retrieval(traces)
    assert len(_retrieval(traces)["m365_teams/PostMessage"]) == 2
    assert len(_retrieval(traces)["outlook_calendar/UpdateEvent"]) == 1


def test_bookkeeping_plugins_are_not_traces_but_stay_in_plugins_tag() -> None:
    audit = _merged()
    sources = set(_retrieval(_traces(audit)))
    assert sources.isdisjoint({"Skill", "TaskCreate", "TaskUpdate", "skill/Skill"})
    assert not any(s.lower().startswith("skill") for s in sources)
    tags = user_defined.build_tags(_turn(), CopilotUser(id="u1"), audit)
    plugins = tags["plugins"].split(",")
    assert {"Skill", "TaskCreate", "TaskUpdate"} <= set(plugins)


def test_named_skill_plugin_becomes_skill_trace() -> None:
    audit = _record(
        "skills",
        "2026-01-15T10:00:00Z",
        ["p1"],
        plugins=["Skill:weekly-report", "Skill/other-skill", "Skill"],
        resources=[],
    )
    by_source = _retrieval(_traces(audit))
    assert by_source["skill/weekly-report"] == [
        {"source": "skill/weekly-report", "input": "", "outputs": []},
    ]
    assert "skill/other-skill" in by_source
    assert len([s for s in by_source if s.startswith("skill/")]) == 2


def test_tool_trace_order_and_model_trace_last() -> None:
    traces = _traces(_merged())
    sources = [t["source"] for t in traces if "source" in t]
    assert sources == [
        "outlook_calendar/UpdateEvent",
        "m365_teams/PostMessage",
        "m365_teams/PostMessage",
        "core/RunScript",
        "business_data/ExecuteSql",
    ]
    assert traces[-1]["model"] == "gpt-synthetic"


def test_non_tool_plugin_names_are_ignored() -> None:
    audit = _record(
        "builtin",
        "2026-01-15T10:00:00Z",
        ["p1"],
        plugins=["BuiltIn", "mcp__broken", "core-"],
        resources=[],
    )
    assert _retrieval(_traces(audit)) == {}


def test_tool_approval_denied_tag() -> None:
    tags = user_defined.build_tags(_turn(), CopilotUser(id="u1"), _merged())
    assert tags["tool_approval_denied"] == "true"


def test_tool_approval_denied_tag_omitted_when_final_status_not_denied() -> None:
    tags = user_defined.build_tags(_turn(), CopilotUser(id="u1"), record_a())
    assert "tool_approval_denied" not in tags


def test_tool_approval_denied_tag_uses_final_status_per_call() -> None:
    audit = _record(
        "regranted",
        "2026-01-15T10:00:00Z",
        ["p1"],
        plugins=[],
        resources=[
            _resource(_CALL_X, "m365_teams/PostMessage", "approvalDenied"),
            _resource(_CALL_X, "m365_teams/PostMessage", "approvalGrantedOnce"),
        ],
    )
    tags = user_defined.build_tags(_turn(), CopilotUser(id="u1"), audit)
    assert "tool_approval_denied" not in tags


def test_non_approval_resources_keep_document_traces() -> None:
    audit = _record(
        "docs",
        "2026-01-15T10:00:00Z",
        ["p1"],
        plugins=[],
        resources=[{"Name": "Doc.docx", "SiteUrl": "https://example.invalid/doc"}],
    )
    assert _retrieval(_traces(audit))["Doc.docx"] == [
        {
            "source": "Doc.docx",
            "input": "https://example.invalid/doc",
            "outputs": ["Doc.docx"],
        },
    ]
