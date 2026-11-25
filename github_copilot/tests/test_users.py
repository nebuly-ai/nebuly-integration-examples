from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import httpx
from github_copilot_sync.store import Store
from github_copilot_sync.users import UserResolver

from tests.test_sources import _config


def test_user_resolver_login_and_email(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/user/42":
            return httpx.Response(200, json={"login": "octo"})
        if request.url.path == "/graphql":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "enterprise": {
                            "ownerInfo": {
                                "samlIdentityProvider": {
                                    "externalIdentities": {
                                        "pageInfo": {"hasNextPage": False},
                                        "nodes": [
                                            {
                                                "user": {
                                                    "databaseId": 42,
                                                    "login": "octo",
                                                },
                                                "scimIdentity": {
                                                    "emails": [
                                                        {
                                                            "value": "octo@example.com",
                                                            "primary": True,
                                                        }
                                                    ]
                                                },
                                            }
                                        ],
                                    }
                                }
                            }
                        }
                    }
                },
            )
        return httpx.Response(404)

    config = replace(_config(resolve_users=True), cache_dir=tmp_path)
    store = Store(tmp_path / "db.sqlite")
    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    resolver = UserResolver(config, client, store)
    assert resolver.end_user(42) == "octo@example.com"
    assert resolver.end_user(42) == "octo@example.com"

    config_off = replace(config, resolve_users=False)
    resolver_off = UserResolver(config_off, client, store)
    assert resolver_off.end_user(42) == "github-user-42"
