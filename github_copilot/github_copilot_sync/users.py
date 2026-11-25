"""Resolve GitHub user_id to login (REST) and email (enterprise GraphQL)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx
from httpx import HTTPStatusError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

if TYPE_CHECKING:
    from .config import Config
    from .store import Store

logger = logging.getLogger(__name__)

_EXTERNAL_IDENTITIES_QUERY = """
query($slug: String!, $cursor: String) {
  enterprise(slug: $slug) {
    ownerInfo {
      samlIdentityProvider {
        externalIdentities(first: 100, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          nodes {
            samlIdentity { nameId }
            scimIdentity {
              username
              emails { value primary }
            }
            user { databaseId login }
          }
        }
      }
    }
  }
}
"""


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, HTTPStatusError):
        return False
    status = exc.response.status_code
    return status in {408, 429} or status >= 500


def _email_from_node(node: dict[str, Any]) -> str | None:
    scim = node.get("scimIdentity")
    if isinstance(scim, dict):
        emails = scim.get("emails")
        if isinstance(emails, list):
            for item in emails:
                if not isinstance(item, dict):
                    continue
                if item.get("primary"):
                    value = item.get("value")
                    if isinstance(value, str) and "@" in value:
                        return value.strip()
            for item in emails:
                if isinstance(item, dict):
                    value = item.get("value")
                    if isinstance(value, str) and "@" in value:
                        return value.strip()
        username = scim.get("username")
        if isinstance(username, str) and "@" in username:
            return username.strip()
    saml = node.get("samlIdentity")
    if isinstance(saml, dict):
        name_id = saml.get("nameId")
        if isinstance(name_id, str) and "@" in name_id:
            return name_id.strip()
    return None


class UserResolver:
    def __init__(self, config: Config, client: httpx.Client, store: Store) -> None:
        self._config = config
        self._client = client
        self._store = store
        self._email_by_login: dict[str, str] = {}
        self._identities_loaded = False

    def end_user(self, user_id: int) -> str:
        if not self._config.resolve_users:
            return f"github-user-{user_id}"
        login = self.login_for_user(user_id)
        email = self.email_for_login(login) if login else None
        if email:
            return email
        if login:
            return login
        return f"github-user-{user_id}"

    def email_for_end_user_resolution(self, user_id: int) -> str | None:
        if not self._config.resolve_users:
            return None
        login = self.login_for_user(user_id)
        return self.email_for_login(login) if login else None

    def login_for_user(self, user_id: int) -> str | None:
        cached = self._store.get_user_login(user_id)
        if cached is not None:
            return cached
        login = self._fetch_login(user_id)
        if login:
            self._store.set_user_login(user_id, login)
        return login

    def email_for_login(self, login: str | None) -> str | None:
        if not login or not self._config.resolve_users:
            return None
        if not self._identities_loaded:
            self._load_external_identities()
        return self._email_by_login.get(login.casefold())

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    def _fetch_login(self, user_id: int) -> str | None:
        response = self._client.get(f"/user/{user_id}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        login = payload.get("login")
        return login.strip() if isinstance(login, str) and login.strip() else None

    def _load_external_identities(self) -> None:
        self._identities_loaded = True
        cursor: str | None = None
        slug = self._config.github_enterprise
        while True:
            payload = self._graphql(slug, cursor)
            enterprise = (
                payload.get("enterprise") if isinstance(payload, dict) else None
            )
            owner_info = (
                enterprise.get("ownerInfo") if isinstance(enterprise, dict) else None
            )
            provider = (
                owner_info.get("samlIdentityProvider")
                if isinstance(owner_info, dict)
                else None
            )
            identities = (
                provider.get("externalIdentities")
                if isinstance(provider, dict)
                else None
            )
            if not isinstance(identities, dict):
                logger.warning(
                    "Enterprise %s returned no externalIdentities; emails unavailable",
                    slug,
                )
                return
            nodes = identities.get("nodes")
            if isinstance(nodes, list):
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    user = node.get("user")
                    login = user.get("login") if isinstance(user, dict) else None
                    email = _email_from_node(node)
                    if isinstance(login, str) and email:
                        self._email_by_login.setdefault(login.casefold(), email)
            page = identities.get("pageInfo")
            if not isinstance(page, dict) or not page.get("hasNextPage"):
                return
            next_cursor = page.get("endCursor")
            cursor = next_cursor if isinstance(next_cursor, str) else None
            if cursor is None:
                return

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        reraise=True,
    )
    def _graphql(self, slug: str, cursor: str | None) -> dict[str, Any]:
        response = self._client.post(
            "/graphql",
            json={
                "query": _EXTERNAL_IDENTITIES_QUERY,
                "variables": {"slug": slug, "cursor": cursor},
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return {}
        errors = payload.get("errors")
        if errors:
            logger.warning("GraphQL externalIdentities errors: %s", errors)
        data = payload.get("data")
        return data if isinstance(data, dict) else {}
