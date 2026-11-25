from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
from github_copilot_sync.store import Store
from github_copilot_sync.users import ResolvedUser, UserResolver

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
    expected = ResolvedUser("octo@example.com", "octo@example.com")
    assert resolver.resolve(42) == expected
    assert resolver.resolve(42) == expected

    config_off = replace(config, resolve_users=False)
    resolver_off = UserResolver(config_off, client, store)
    assert resolver_off.resolve(42) == ResolvedUser("github-user-42", None)


def _warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.levelno == logging.WARNING]


def _resolver(
    tmp_path: Path, handler: Callable[[httpx.Request], httpx.Response]
) -> UserResolver:
    config = replace(_config(resolve_users=True), cache_dir=tmp_path)
    client = httpx.Client(
        base_url="https://api.github.com",
        transport=httpx.MockTransport(handler),
    )
    return UserResolver(config, client, Store(tmp_path / "db.sqlite"))


def test_graphql_forbidden_falls_back_to_login_and_warns_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/graphql":
            return httpx.Response(403, request=request)
        return httpx.Response(200, json={"login": f"user{request.url.path[-1]}"})

    resolver = _resolver(tmp_path, handler)

    assert resolver.resolve(1) == ResolvedUser("user1", None)
    assert resolver.resolve(2) == ResolvedUser("user2", None)
    assert calls.count("/graphql") == 1
    assert len(_warnings(caplog)) == 1


def test_login_forbidden_falls_back_to_user_id_and_stops_looking_up(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(403, request=request)

    resolver = _resolver(tmp_path, handler)

    assert resolver.resolve(1) == ResolvedUser("github-user-1", None)
    assert resolver.resolve(2) == ResolvedUser("github-user-2", None)
    assert calls == ["/user/1"]
    assert len(_warnings(caplog)) == 1


def test_missing_login_is_not_refetched(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(404, request=request)

    resolver = _resolver(tmp_path, handler)

    for _ in range(3):
        assert resolver.resolve(7) == ResolvedUser("github-user-7", None)
    assert calls == ["/user/7"]


def test_transient_login_error_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(UserResolver._fetch_login.retry, "sleep", lambda _s: None)
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/graphql":
            return httpx.Response(200, json={"data": {}})
        attempts["n"] += 1
        if attempts["n"] < 3:
            return httpx.Response(503, request=request)
        return httpx.Response(200, json={"login": "octo"})

    resolver = _resolver(tmp_path, handler)

    assert resolver.resolve(5) == ResolvedUser("octo", None)
    assert attempts["n"] == 3
