"""Resolve GitHub user_id to login (REST) and email (enterprise GraphQL)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, NamedTuple

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


def _valid_email(value: object) -> str | None:
    if isinstance(value, str) and "@" in value:
        return value.strip()
    return None


def _email_from_emails(emails: list[Any], *, primary_only: bool) -> str | None:
    for item in emails:
        if not isinstance(item, dict):
            continue
        if primary_only and not item.get("primary"):
            continue
        email = _valid_email(item.get("value"))
        if email is not None:
            return email
    return None


def _email_from_node(node: dict[str, Any]) -> str | None:
    scim = node.get("scimIdentity")
    if isinstance(scim, dict):
        emails = scim.get("emails")
        if isinstance(emails, list):
            email = _email_from_emails(emails, primary_only=True) or _email_from_emails(
                emails, primary_only=False
            )
            if email is not None:
                return email
        username = _valid_email(scim.get("username"))
        if username is not None:
            return username
    saml = node.get("samlIdentity")
    if isinstance(saml, dict):
        return _valid_email(saml.get("nameId"))
    return None


def _external_identities(payload: object) -> dict[str, Any] | None:
    enterprise = payload.get("enterprise") if isinstance(payload, dict) else None
    owner_info = enterprise.get("ownerInfo") if isinstance(enterprise, dict) else None
    provider = (
        owner_info.get("samlIdentityProvider") if isinstance(owner_info, dict) else None
    )
    identities = (
        provider.get("externalIdentities") if isinstance(provider, dict) else None
    )
    return identities if isinstance(identities, dict) else None


def _next_cursor(page: object) -> str | None:
    if not isinstance(page, dict) or not page.get("hasNextPage"):
        return None
    end_cursor = page.get("endCursor")
    return end_cursor if isinstance(end_cursor, str) else None


class ResolvedUser(NamedTuple):
    end_user: str
    email: str | None


class UserResolver:
    """Falls back from email to login to `github-user-<id>`.

    A non-retryable GitHub error (e.g. 403 for a token without enterprise owner
    access) disables that lookup for the rest of the run after one warning.
    """

    def __init__(self, config: Config, client: httpx.Client, store: Store) -> None:
        self._config = config
        self._client = client
        self._store = store
        self._email_by_login: dict[str, str] = {}
        self._identities_loaded = False
        self._login_lookup_disabled = False
        self._missing_user_ids: set[int] = set()

    def resolve(self, user_id: int) -> ResolvedUser:
        fallback = f"github-user-{user_id}"
        if not self._config.resolve_users:
            return ResolvedUser(fallback, None)
        login = self.login_for_user(user_id)
        email = self.email_for_login(login)
        return ResolvedUser(email or login or fallback, email)

    def login_for_user(self, user_id: int) -> str | None:
        cached = self._store.get_user_login(user_id)
        if cached is not None:
            return cached
        if self._login_lookup_disabled or user_id in self._missing_user_ids:
            return None
        try:
            login = self._fetch_login(user_id)
        except HTTPStatusError as exc:
            if _should_retry(exc):
                raise
            logger.warning(
                "GitHub login lookup failed (HTTP %d); falling back to "
                "github-user-<id> for the rest of this run",
                exc.response.status_code,
            )
            self._login_lookup_disabled = True
            return None
        if login:
            self._store.set_user_login(user_id, login)
        else:
            self._missing_user_ids.add(user_id)
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
            try:
                payload = self._graphql(slug, cursor)
            except HTTPStatusError as exc:
                if _should_retry(exc):
                    raise
                logger.warning(
                    "Enterprise %s external identities lookup failed (HTTP %d); "
                    "emails unavailable for the rest of this run",
                    slug,
                    exc.response.status_code,
                )
                return
            identities = _external_identities(payload)
            if not isinstance(identities, dict):
                logger.warning(
                    "Enterprise %s returned no externalIdentities; emails unavailable",
                    slug,
                )
                return
            self._absorb_identity_nodes(identities.get("nodes"))
            cursor = _next_cursor(identities.get("pageInfo"))
            if cursor is None:
                return

    def _absorb_identity_nodes(self, nodes: object) -> None:
        if not isinstance(nodes, list):
            return
        for node in nodes:
            if not isinstance(node, dict):
                continue
            user = node.get("user")
            login = user.get("login") if isinstance(user, dict) else None
            email = _email_from_node(node)
            if isinstance(login, str) and email:
                self._email_by_login.setdefault(login.casefold(), email)

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
