from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest
from compliance_sync.config import Config
from compliance_sync.converter import Interaction, pair_to_payload
from compliance_sync.entra import (
    EntraDirectory,
    EntraUser,
    fetch_entra_directory,
    load_entra_extract,
)
from compliance_sync.models import (
    ChatMessage,
    ChatMessagesResponse,
    ChatUser,
    LocalSession,
    SessionMessage,
    SessionUser,
    TextContent,
)
from compliance_sync.session_converter import (
    SessionCutInteraction,
    session_interaction_to_payload,
)
from compliance_sync.sources import (
    ChatSource,
    ConversationRef,
    FetchRequest,
    LocalSessionSource,
)

from tests.test_converter import _chat, _msg
from tests.test_session_converter import _local_session

if TYPE_CHECKING:
    from pathlib import Path

_USER = {
    "id": "entra-1",
    "mail": "Ada@Example.com",
    "userPrincipalName": "ada@example.com",
    "department": "Engineering",
    "jobTitle": "Staff Engineer",
    "officeLocation": "Milan",
    "city": "Milan",
    "country": "",
    "usageLocation": "IT",
    "companyName": "Example",
    "employeeHireDate": "2020-03-16T01:30:00+02:00",
}


def _directory() -> EntraDirectory:
    return EntraDirectory([EntraUser.model_validate(_USER)])


def test_directory_tags_match_copilot_shape() -> None:
    tags = _directory().tags_for_email("  ADA@example.com ")
    assert tags == {
        "department": "Engineering",
        "job_title": "Staff Engineer",
        "office_location": "Milan",
        "city": "Milan",
        "country": "IT",
        "company_name": "Example",
        "employee_hire_date": "2020-03-15",
    }


def test_blank_directory_fields_are_omitted() -> None:
    user = EntraUser.model_validate({"mail": "a@example.com", "department": "  "})
    assert EntraDirectory([user]).tags_for_email("a@example.com") == {}


def test_first_duplicate_email_wins() -> None:
    first = EntraUser.model_validate({"mail": "a@example.com", "department": "First"})
    second = EntraUser.model_validate(
        {"userPrincipalName": "A@example.com", "department": "Second"}
    )
    tags = EntraDirectory([first, second]).tags_for_email("a@example.com")
    assert tags["department"] == "First"


def test_load_object_and_bare_array(tmp_path: Path) -> None:
    obj = tmp_path / "object.json"
    obj.write_text(
        '{"users": [{"mail": "a@example.com", "department": "Engineering"}]}',
        encoding="utf-8",
    )
    array = tmp_path / "array.json"
    array.write_text(
        '[{"mail": "b@example.com", "jobTitle": "Engineer"}]',
        encoding="utf-8",
    )
    assert (
        load_entra_extract(obj).tags_for_email("a@example.com")["department"]
        == "Engineering"
    )
    assert load_entra_extract(array).tags_for_email("b@example.com")["job_title"] == (
        "Engineer"
    )


def test_missing_extract_raises(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="not found"):
        load_entra_extract(tmp_path / "missing.json")


def test_chat_payload_keeps_claude_user_and_adds_tags() -> None:
    chat = _chat()
    chat.user = ChatUser(id="user_01", email_address="ada@example.com")
    pair = Interaction(
        user_message=_msg("u1", "user", "hello"),
        assistant_message=_msg("a1", "assistant", "hi"),
        chat=chat,
    )
    payload = pair_to_payload(pair, anonymize=False, directory=_directory())
    assert payload is not None
    interaction = payload["interaction"]
    assert interaction["end_user"] == "user_01"
    assert interaction["team"] == "Engineering"
    assert interaction["market"] == "IT"
    assert interaction["tenure"] == "2020-03-15"
    assert interaction["tags"]["claude source"] == "chat"
    assert interaction["tags"]["department"] == "Engineering"
    assert interaction["tags"]["employee_hire_date"] == "2020-03-15"

    plain = pair_to_payload(pair, anonymize=False)
    assert plain is not None
    assert "department" not in plain["interaction"]["tags"]
    assert "team" not in plain["interaction"]
    assert "market" not in plain["interaction"]
    assert "tenure" not in plain["interaction"]


def test_unmatched_email_adds_no_directory_tags() -> None:
    chat = _chat()
    assert chat.user is not None
    chat.user = ChatUser(id="user_01", email_address="other@example.com")
    pair = Interaction(
        user_message=_msg("u1", "user", "hello"),
        assistant_message=_msg("a1", "assistant", "hi"),
        chat=chat,
    )
    payload = pair_to_payload(pair, anonymize=False, directory=_directory())
    assert payload is not None
    assert payload["interaction"]["end_user"] == "user_01"
    assert "department" not in payload["interaction"]["tags"]
    assert "team" not in payload["interaction"]
    assert "market" not in payload["interaction"]
    assert "tenure" not in payload["interaction"]


def test_session_uses_listing_email_when_message_email_is_null() -> None:
    session = _local_session(datetime(2025, 6, 1, 10, 4, tzinfo=UTC))
    assert session.user is not None
    session.user = SessionUser(id="user_1", email_address=None)
    cut = SessionCutInteraction(
        key="prompt_01",
        input_text="Fix the test.",
        output_text="Done.",
        time_start=datetime(2025, 6, 1, 10, 1, tzinfo=UTC),
        time_end=datetime(2025, 6, 1, 10, 4, tzinfo=UTC),
        closed=True,
    )
    payload = session_interaction_to_payload(
        cut,
        session,
        source="local_session",
        list_metadata=None,
        anonymize=False,
        directory=_directory(),
        match_email="ada@example.com",
    )
    assert payload is not None
    assert payload["interaction"]["end_user"] == "user_1"
    assert payload["interaction"]["team"] == "Engineering"
    assert payload["interaction"]["market"] == "IT"
    assert payload["interaction"]["tenure"] == "2020-03-15"
    assert payload["interaction"]["tags"]["job_title"] == "Staff Engineer"
    assert payload["interaction"]["tags"]["product surface"] == "claude_code"


class _ChatClient:
    def list_chat_messages(
        self,
        chat_id: str,
        created_at_gte: str | None = None,
    ) -> ChatMessagesResponse:
        now = datetime(2025, 2, 1, 10, 0, tzinfo=UTC)
        return ChatMessagesResponse(
            id=chat_id,
            name="Test chat",
            created_at=now,
            updated_at=now,
            href="https://claude.ai/chat/test",
            model="claude-opus-4-8",
            organization_uuid="org_demo",
            user=ChatUser(id="user_01", email_address="ada@example.com"),
            chat_messages=[
                ChatMessage(
                    id="u1",
                    role="user",
                    created_at=now,
                    content=[TextContent(type="text", text="hello")],
                ),
                ChatMessage(
                    id="a1",
                    role="assistant",
                    created_at=now,
                    content=[TextContent(type="text", text="hi")],
                ),
            ],
            has_more=False,
        )


def test_chat_fetch_attaches_directory_before_the_payload_is_built() -> None:
    now = datetime(2025, 2, 1, 10, 0, tzinfo=UTC)
    source = ChatSource(cast(Any, _ChatClient()), "org_demo")
    result = source.fetch(
        FetchRequest(
            ref=ConversationRef(
                source="chats",
                conversation_id="chat_01",
                updated_at=now,
            ),
            from_date=None,
            now=now,
            idle_minutes=30,
            anonymize=False,
            directory=_directory(),
        )
    )
    payload = result.interactions[0].payload
    assert payload is not None
    assert payload["interaction"]["end_user"] == "user_01"
    assert payload["interaction"]["team"] == "Engineering"
    assert payload["interaction"]["market"] == "IT"
    assert payload["interaction"]["tenure"] == "2020-03-15"
    assert payload["interaction"]["tags"]["department"] == "Engineering"


class _LocalClient:
    def list_session_messages(
        self,
        kind: str,
        session_id: str,
        *,
        limit: int = 1000,
    ) -> tuple[LocalSession, list[SessionMessage]]:
        now = datetime(2025, 6, 1, 10, 4, tzinfo=UTC)
        session = LocalSession(
            id=session_id,
            organization_uuid="org_demo",
            user=SessionUser(id="user_1", email_address=None),
            product_surface="claude_code",
            created_at=now,
            updated_at=now,
        )
        messages = [
            SessionMessage(
                id="p1",
                role="user",
                created_at=now,
                content=[TextContent(type="text", text="go")],
            ),
            SessionMessage(
                id="a1",
                role="assistant",
                created_at=now,
                content=[TextContent(type="text", text="done")],
                model="claude-sonnet",
            ),
        ]
        return session, messages


def test_local_fetch_matches_the_listing_email() -> None:
    now = datetime(2025, 6, 1, 10, 4, tzinfo=UTC)
    listed = LocalSession(
        id="clls_1",
        organization_uuid="org_demo",
        user=SessionUser(id="user_1", email_address="ada@example.com"),
        product_surface="claude_code",
        created_at=now,
        updated_at=now,
    )
    source = LocalSessionSource(cast(Any, _LocalClient()), "org_demo")
    result = source.fetch(
        FetchRequest(
            ref=ConversationRef(
                source="local_sessions",
                conversation_id="clls_1",
                updated_at=now,
                local_session=listed,
            ),
            from_date=None,
            now=now,
            idle_minutes=30,
            anonymize=False,
            directory=_directory(),
        )
    )
    payload = result.interactions[0].payload
    assert payload is not None
    assert payload["interaction"]["end_user"] == "user_1"
    assert payload["interaction"]["team"] == "Engineering"
    assert payload["interaction"]["tags"]["company_name"] == "Example"


def _graph_transport(user: dict[str, Any]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "token"})
        if request.url.host == "graph.microsoft.com":
            return httpx.Response(200, json={"value": [user], "@odata.nextLink": None})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def test_fetch_entra_directory_from_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("entra_export")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    client = httpx.Client(transport=_graph_transport(_USER))
    directory = fetch_entra_directory(client)
    tags = directory.tags_for_email("ada@example.com")
    assert tags["department"] == "Engineering"
    assert tags["job_title"] == "Staff Engineer"


def test_config_loads_entra_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("COMPLIANCE_API_KEY", "key")
    monkeypatch.setenv("ORGANIZATION_UUID", "org_demo")
    monkeypatch.setenv("ENTRA_FETCH", "true")
    monkeypatch.setenv("ENTRA_EXTRACT_PATH", "")

    def _fake_fetch() -> EntraDirectory:
        return _directory()

    monkeypatch.setattr("compliance_sync.config.fetch_entra_directory", _fake_fetch)
    config = Config.from_env_and_args([])
    assert config.entra_directory is not None
    assert (
        config.entra_directory.tags_for_email("ada@example.com")["department"]
        == "Engineering"
    )


def test_config_rejects_entra_fetch_and_extract_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "entra.json"
    path.write_text('{"users": []}', encoding="utf-8")
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("COMPLIANCE_API_KEY", "key")
    monkeypatch.setenv("ORGANIZATION_UUID", "org_demo")
    monkeypatch.setenv("ENTRA_FETCH", "true")
    monkeypatch.setenv("ENTRA_EXTRACT_PATH", str(path))
    with pytest.raises(RuntimeError, match="not both"):
        Config.from_env_and_args([])


def test_config_entra_fetch_requires_azure_env(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("entra_export")
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("COMPLIANCE_API_KEY", "key")
    monkeypatch.setenv("ORGANIZATION_UUID", "org_demo")
    monkeypatch.setenv("ENTRA_FETCH", "true")
    monkeypatch.setenv("ENTRA_EXTRACT_PATH", "")
    monkeypatch.setenv("AZURE_TENANT_ID", "")
    monkeypatch.setenv("AZURE_CLIENT_ID", "")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "")
    with pytest.raises(RuntimeError, match="AZURE_TENANT_ID"):
        Config.from_env_and_args([])


def test_fetch_entra_directory_without_entra_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "entra_export.export", None)
    with pytest.raises(RuntimeError, match="poetry install --with entra"):
        fetch_entra_directory()


def test_config_loads_entra_extract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "entra.json"
    path.write_text(
        '{"users": [{"mail": "ada@example.com", "department": "Engineering"}]}',
        encoding="utf-8",
    )
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("COMPLIANCE_API_KEY", "key")
    monkeypatch.setenv("ORGANIZATION_UUID", "org_demo")
    monkeypatch.setenv("ENTRA_EXTRACT_PATH", str(path))
    config = Config.from_env_and_args([])
    assert config.entra_directory is not None
    assert (
        config.entra_directory.tags_for_email("ADA@example.com")["department"]
        == "Engineering"
    )
