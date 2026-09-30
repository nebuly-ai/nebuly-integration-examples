from __future__ import annotations

from datetime import UTC, datetime

from copilot_sync.models import CopilotAuditRecord


def test_copilot_audit_record_nested_copilot_event_data() -> None:
    thread_id = "19:lgojcxwbvhJnfU3IhUJW5M-nSX2U7tjccgSrtYAoG341@thread.v2"
    record = CopilotAuditRecord.model_validate(
        {
            "id": "99b0a960-13a0-461f-8c5c-cb2316ea273d",
            "createdDateTime": "2023-12-13T17:12:36Z",
            "userPrincipalName": "admin@contoso.com",
            "auditData": {
                "ClientRegion": "US",
                "UserId": "admin@contoso.com",
                "CopilotEventData": {
                    "AppHost": "Word",
                    "ThreadId": thread_id,
                    "Messages": [
                        {"Id": "1715187560311", "isPrompt": True},
                        {"Id": "1715187561014", "isPrompt": False},
                    ],
                    "ModelTransparencyDetails": [],
                    "AISystemPlugin": [],
                    "AccessedResources": [],
                },
            },
        }
    )

    assert (
        record.thread_id == "19:lgojcxwbvhJnfU3IhUJW5M-nSX2U7tjccgSrtYAoG341@thread.v2"
    )
    assert record.app_host == "Word"
    assert record.client_region == "US"
    assert record.message_ids == ["1715187560311", "1715187561014"]
    assert record.primary_model is None
    assert record.web_search_used is False


def test_copilot_audit_record_flat_audit_data_and_string_blob() -> None:
    audit_blob = (
        '{"ClientRegion":"IN","UserId":"admin@contoso.com",'
        '"CopilotEventData":{"AISystemPlugin":[{"Id":"BingWebSearch","Name":"BuiltIn"}],'
        '"AccessedResources":[{"Action":"Read","Name":"Document1.docx",'
        '"SensitivityLabelId":"f41ab342-8706-4188-bd11-ebb85995028c",'
        '"XPIADetected":true}],'
        '"AppHost":"Bing","Messages":[{"Id":"1715186983849","isPrompt":true},'
        '{"Id":"1715186984291","isPrompt":false,"JailbreakDetected":true}],'
        '"ModelTransparencyDetails":[{"ModelProviderName":"OpenAI","ModelName":"DEEP_LEO"}],'
        '"ThreadId":"19:Xn3uQZYgZ7f2ue0vp5w9MglEVjFyp5pza1efaC6g2U41@thread.v2"}}'
    )
    record = CopilotAuditRecord.model_validate(
        {
            "id": "537312b6-dce7-4d9b-8b12-58283204b720",
            "createdDateTime": "2023-12-14T02:11:55Z",
            "auditData": audit_blob,
        }
    )

    assert record.user_principal_name == "admin@contoso.com"
    assert record.app_host == "Bing"
    assert record.primary_model is not None
    assert record.primary_model.model_name == "DEEP_LEO"
    assert record.primary_model.model_provider_name == "OpenAI"
    assert record.web_search_used is True
    assert record.jailbreak_detected is True
    assert record.xpia_detected is True
    assert record.created_datetime == datetime(2023, 12, 14, 2, 11, 55, tzinfo=UTC)


def test_copilot_audit_record_graph_flat_event_fields() -> None:
    record = CopilotAuditRecord.model_validate(
        {
            "id": "audit-flat-1",
            "createdDateTime": "2025-06-15T10:00:00Z",
            "auditData": {
                "ThreadId": "19:flat-thread@thread.v2",
                "AppHost": "Teams",
                "AppIdentity": "Microsoft 365 Chat",
                "AgentId": "agent-1",
                "AgentName": "Researcher",
                "AgentVersion": "2",
                "Messages": [{"id": "1732148357313", "IsPrompt": True}],
                "ModelTransparencyDetails": [
                    {
                        "modelProviderName": "Anthropic",
                        "modelName": "claude-sonnet-4-6",
                    },
                ],
            },
        }
    )

    assert record.thread_id == "19:flat-thread@thread.v2"
    assert record.app_identity == "Microsoft 365 Chat"
    assert record.agent_name == "Researcher"
    assert record.message_ids == ["1732148357313"]
    assert record.primary_model is not None
    assert record.primary_model.model_name == "claude-sonnet-4-6"


def test_copilot_audit_record_merges_when_root_messages_empty() -> None:
    record = CopilotAuditRecord.model_validate(
        {
            "id": "audit-empty-root",
            "createdDateTime": "2025-06-15T10:00:00Z",
            "messages": [],
            "auditData": {
                "CopilotEventData": {
                    "Messages": [{"Id": "999", "isPrompt": True}],
                },
            },
        }
    )

    assert record.message_ids == ["999"]


def test_copilot_audit_record_accepts_singular_message_object() -> None:
    record = CopilotAuditRecord.model_validate(
        {
            "id": "audit-singular-message",
            "createdDateTime": "2025-06-15T10:00:00Z",
            "auditData": {
                "Messages": {"Id": "888", "isPrompt": False},
            },
        }
    )

    assert record.message_ids == ["888"]
