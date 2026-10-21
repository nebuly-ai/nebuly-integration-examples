from __future__ import annotations

from datetime import UTC, datetime, timedelta

from compliance_sync.models import (
    LocalSession,
    ProvenanceClientAsserted,
    ProvenanceContentUnavailable,
    ProvenanceSyntheticMarker,
    RemoteSession,
    RemoteSessionListMetadata,
    SessionMessage,
    SessionUser,
    StartedByUser,
    TextContent,
    ToolResultContent,
    ToolResultTextContent,
    ToolUseContent,
)
from compliance_sync.session_converter import (
    cut_session_interactions,
    resolve_session_end_user,
    session_interaction_to_payload,
)


def _ts(minute: int) -> datetime:
    return datetime(2025, 6, 1, 10, minute, tzinfo=UTC)


def _local_session(updated_at: datetime) -> LocalSession:
    return LocalSession(
        id="clls_1",
        organization_uuid="org_demo",
        user=SessionUser(id="user_1", email_address="u@example.com"),
        product_surface="claude_code",
        created_at=_ts(0),
        updated_at=updated_at,
    )


def test_skips_synthetic_marker_by_provenance_not_role() -> None:
    messages = [
        SessionMessage(
            id="m0",
            role="assistant",
            created_at=_ts(0),
            content=[TextContent(type="text", text="marker")],
            provenance=ProvenanceSyntheticMarker(type="synthetic_marker"),
        ),
        SessionMessage(
            id="m1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="real prompt")],
        ),
        SessionMessage(
            id="m2",
            role="assistant",
            created_at=_ts(2),
            content=[TextContent(type="text", text="answer")],
            model="claude-sonnet",
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(2),
        session_status=None,
        now=_ts(2),
        idle_minutes=30,
    )
    assert len(cuts) == 1
    assert cuts[0].input_text == "real prompt"


def test_system_reminder_only_user_message_is_injected_context() -> None:
    messages = [
        SessionMessage(
            id="m1",
            role="user",
            created_at=_ts(1),
            content=[
                TextContent(
                    type="text",
                    text="<system-reminder>rules</system-reminder>",
                )
            ],
        ),
        SessionMessage(
            id="m2",
            role="user",
            created_at=_ts(2),
            content=[TextContent(type="text", text="do work")],
        ),
        SessionMessage(
            id="m3",
            role="assistant",
            created_at=_ts(3),
            content=[TextContent(type="text", text="done")],
            model="claude-sonnet",
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(3),
        session_status=None,
        now=_ts(3),
        idle_minutes=30,
    )
    assert len(cuts) == 1
    assert cuts[0].input_text == "do work"


def test_compaction_summary_treated_as_injected_context() -> None:
    messages = [
        SessionMessage(
            id="m1",
            role="user",
            created_at=_ts(1),
            content=[
                TextContent(
                    type="text",
                    text=(
                        "This session is being continued from a previous "
                        "conversation. Summary here."
                    ),
                )
            ],
        ),
        SessionMessage(
            id="m2",
            role="user",
            created_at=_ts(2),
            content=[TextContent(type="text", text="next prompt")],
        ),
        SessionMessage(
            id="m3",
            role="assistant",
            created_at=_ts(3),
            content=[TextContent(type="text", text="ok")],
            model="claude-sonnet",
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(3),
        session_status=None,
        now=_ts(3),
        idle_minutes=30,
    )
    assert len(cuts) == 1
    assert cuts[0].input_text == "next prompt"


def test_tool_rounds_and_retrieval_traces_in_payload() -> None:
    messages = [
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="search")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(2),
            content=[
                ToolUseContent(
                    type="tool_use", id="tu1", name="web_search", input='{"q":"x"}'
                ),
            ],
            model="claude-sonnet",
        ),
        SessionMessage(
            id="r1",
            role="user",
            created_at=_ts(3),
            content=[
                ToolResultContent(
                    type="tool_result",
                    tool_use_id="tu1",
                    is_error=False,
                    name="web_search",
                    content=[ToolResultTextContent(type="text", text="result")],
                )
            ],
        ),
        SessionMessage(
            id="a2",
            role="assistant",
            created_at=_ts(4),
            content=[TextContent(type="text", text="final")],
            model="claude-sonnet",
        ),
    ]
    session = _local_session(_ts(4))
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(4),
        session_status=None,
        now=_ts(4),
        idle_minutes=30,
    )
    payload = session_interaction_to_payload(
        cuts[0],
        session,
        source="local_session",
        list_metadata=None,
        anonymize=False,
    )
    assert payload is not None
    retrieval = [t for t in payload["traces"] if "source" in t]
    assert retrieval[0]["source"] == "web_search"


def test_client_asserted_not_used_as_output() -> None:
    messages = [
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="go")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(2),
            content=[TextContent(type="text", text="verified")],
            model="claude-sonnet",
        ),
        SessionMessage(
            id="a2",
            role="assistant",
            created_at=_ts(3),
            content=[TextContent(type="text", text="unverified")],
            provenance=ProvenanceClientAsserted(type="client_asserted"),
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(3),
        session_status=None,
        now=_ts(3),
        idle_minutes=30,
    )
    assert cuts[0].output_text == "verified"


def test_content_unavailable_reason_tagged() -> None:
    messages = [
        SessionMessage(
            id="ret",
            role="user",
            created_at=_ts(0),
            content=[],
            provenance=ProvenanceContentUnavailable(
                type="content_unavailable", reason="retention_elapsed"
            ),
        ),
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="prompt")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(2),
            content=[],
            provenance=ProvenanceContentUnavailable(
                type="content_unavailable", reason="client_aborted"
            ),
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(2),
        session_status=None,
        now=_ts(2),
        idle_minutes=30,
    )
    assert "content_unavailable=client_aborted" in cuts[0].content_unavailable_tags


def test_back_to_back_prompts_joined() -> None:
    messages = [
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="first")],
        ),
        SessionMessage(
            id="p2",
            role="user",
            created_at=_ts(2),
            content=[TextContent(type="text", text="second")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(3),
            content=[TextContent(type="text", text="reply")],
            model="claude-sonnet",
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(3),
        session_status=None,
        now=_ts(3),
        idle_minutes=30,
    )
    assert cuts[0].input_text == "first\nsecond"


def test_open_interaction_closed_when_idle() -> None:
    messages = [
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="prompt")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(2),
            content=[TextContent(type="text", text="partial")],
            model="claude-sonnet",
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(2),
        session_status=None,
        now=_ts(2) + timedelta(minutes=31),
        idle_minutes=30,
    )
    assert cuts[0].closed is True


def test_open_interaction_stays_open_when_not_idle() -> None:
    messages = [
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="prompt")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(2),
            content=[TextContent(type="text", text="partial")],
            model="claude-sonnet",
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=False,
        session_updated_at=_ts(2),
        session_status=None,
        now=_ts(2) + timedelta(minutes=5),
        idle_minutes=30,
    )
    assert cuts[0].closed is False


def test_remote_content_unavailable_flag() -> None:
    messages = [
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="go")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(2),
            content=[],
            content_unavailable=True,
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=True,
        session_updated_at=_ts(2),
        session_status="active",
        now=_ts(2) + timedelta(minutes=31),
        idle_minutes=30,
    )
    assert "content_unavailable" in cuts[0].content_unavailable_tags


def test_remote_end_user_from_started_by_user_metadata() -> None:
    session = RemoteSession(
        id="cse_1",
        organization_uuid="org_demo",
        user=None,
        agent_id="agent_1",
        started_by_user=None,
        status="archived",
        created_at=_ts(0),
        updated_at=_ts(5),
        product_surface="cowork_remote",
    )
    metadata = RemoteSessionListMetadata(started_by_user=StartedByUser(id="owner_1"))
    assert resolve_session_end_user(session, metadata) == "owner_1"


def test_time_end_is_max_timestamp_in_interaction() -> None:
    messages = [
        SessionMessage(
            id="p1",
            role="user",
            created_at=_ts(1),
            content=[TextContent(type="text", text="go")],
        ),
        SessionMessage(
            id="a1",
            role="assistant",
            created_at=_ts(3),
            content=[
                ToolUseContent(type="tool_use", id="tu1", name="bash", input="{}"),
            ],
            model="claude-sonnet",
        ),
        SessionMessage(
            id="r1",
            role="user",
            created_at=_ts(2),
            content=[
                ToolResultContent(
                    type="tool_result",
                    tool_use_id="tu1",
                    is_error=False,
                    name="bash",
                    content=[ToolResultTextContent(type="text", text="out")],
                )
            ],
        ),
        SessionMessage(
            id="a2",
            role="assistant",
            created_at=_ts(4),
            content=[TextContent(type="text", text="done")],
            model="claude-sonnet",
        ),
    ]
    cuts = cut_session_interactions(
        messages,
        remote=True,
        session_updated_at=_ts(4),
        session_status="archived",
        now=_ts(4),
        idle_minutes=30,
    )
    assert cuts[0].time_end == _ts(4)
