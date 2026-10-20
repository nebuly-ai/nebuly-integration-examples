from __future__ import annotations

import json
from typing import TYPE_CHECKING

import httpx
import pytest

if TYPE_CHECKING:
    from pathlib import Path

from entra_export.config import Config
from entra_export.export import run_export, write_extract
from entra_export.graph import USER_FIELDS, list_users


def _config(tmp_path: Path, *, dry_run: bool = False) -> Config:
    return Config(
        azure_tenant_id="tenant",
        azure_client_id="client",
        azure_client_secret="secret",
        output=tmp_path / "entra.json",
        dry_run=dry_run,
        verbose=False,
    )


def _user(index: int) -> dict[str, object]:
    return {
        "id": f"id-{index}",
        "mail": f"user{index}@example.com",
        "userPrincipalName": f"user{index}@example.com",
        "department": "Engineering",
        "jobTitle": "Engineer",
        "officeLocation": "Milan",
        "city": "Milan",
        "country": "Italy",
        "usageLocation": "IT",
        "companyName": "Example",
        "employeeHireDate": "2020-03-15T00:00:00Z",
        "displayName": "should be dropped",
    }


def _transport(pages: list[dict[str, object]]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "token"})
        if request.url.host != "graph.microsoft.com":
            return httpx.Response(404)
        page = request.url.params.get("page", "1")
        body = pages[int(page) - 1]
        return httpx.Response(200, json=body)

    return httpx.MockTransport(handler)


def test_list_users_follows_next_link_and_keeps_graph_fields() -> None:
    pages = [
        {
            "value": [_user(1), {"id": "  ", "mail": "blank@example.com"}],
            "@odata.nextLink": ("https://graph.microsoft.com/v1.0/users?page=2"),
        },
        {"value": [_user(2)], "@odata.nextLink": None},
    ]
    client = httpx.Client(transport=_transport(pages))

    users = list_users(client, "token")

    assert [user["id"] for user in users] == ["id-1", "id-2"]
    assert set(users[0]) == set(USER_FIELDS)
    assert "displayName" not in users[0]
    assert users[0]["employeeHireDate"] == "2020-03-15T00:00:00Z"
    assert users[0]["jobTitle"] == "Engineer"


def test_run_export_writes_users_array(tmp_path: Path) -> None:
    pages = [{"value": [_user(1)], "@odata.nextLink": None}]
    client = httpx.Client(transport=_transport(pages))

    count = run_export(_config(tmp_path), client)

    written = json.loads((tmp_path / "entra.json").read_text(encoding="utf-8"))
    assert count == 1
    assert written["users"][0]["mail"] == "user1@example.com"
    assert written["users"][0]["userPrincipalName"] == "user1@example.com"


def test_export_uses_configured_base_urls(tmp_path: Path) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        if request.url.path.endswith("/token"):
            return httpx.Response(200, json={"access_token": "token"})
        return httpx.Response(200, json={"value": [_user(1)]})

    config = Config(
        azure_tenant_id="tenant",
        azure_client_id="client",
        azure_client_secret="secret",
        output=tmp_path / "entra.json",
        dry_run=True,
        verbose=False,
        login_base_url="http://entra.test",
        graph_base_url="http://entra.test",
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))

    count = run_export(config, client)

    assert count == 1
    assert seen[0].startswith("http://entra.test/tenant/oauth2/v2.0/token")
    assert seen[1].startswith("http://entra.test/v1.0/users")


def test_dry_run_does_not_write_a_file(tmp_path: Path) -> None:
    pages = [{"value": [_user(1)], "@odata.nextLink": None}]
    client = httpx.Client(transport=_transport(pages))

    count = run_export(_config(tmp_path, dry_run=True), client)

    assert count == 1
    assert not (tmp_path / "entra.json").exists()


def test_graph_403_names_the_required_permission() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"error": {"message": "Insufficient privileges"}},
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))

    with pytest.raises(RuntimeError, match=r"User\.Read\.All"):
        list_users(client, "token")


def test_write_extract_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "entra.json"
    write_extract(path, [_user(1)])
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["users"][0]["department"] == "Engineering"


def test_missing_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_TENANT_ID", "")
    monkeypatch.setenv("AZURE_CLIENT_ID", "")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "")

    with pytest.raises(RuntimeError, match="AZURE_TENANT_ID"):
        Config.from_env_and_args([])
