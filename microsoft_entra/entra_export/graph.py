from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger(__name__)

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
PAGE_SIZE = 999

# Graph names the Claude Compliance join expects. See claude_compliance/EXTRACTS.md.
USER_FIELDS: tuple[str, ...] = (
    "id",
    "mail",
    "userPrincipalName",
    "department",
    "jobTitle",
    "officeLocation",
    "city",
    "country",
    "usageLocation",
    "companyName",
    "employeeHireDate",
)


def fetch_token(
    client: httpx.Client,
    *,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    login_base_url: str = "https://login.microsoftonline.com",
) -> str:
    response = client.post(
        f"{login_base_url.rstrip('/')}/{tenant_id}/oauth2/v2.0/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": GRAPH_SCOPE,
            "grant_type": "client_credentials",
        },
    )
    if response.status_code != 200:
        message = (
            "Microsoft Entra token request failed "
            f"({response.status_code}): {_error_text(response)}"
        )
        raise RuntimeError(message)
    token = response.json().get("access_token")
    if not isinstance(token, str) or not token:
        message = "Microsoft Entra token response did not include access_token"
        raise RuntimeError(message)
    return token


def list_users(
    client: httpx.Client,
    token: str,
    *,
    graph_base_url: str = "https://graph.microsoft.com",
) -> list[dict[str, Any]]:
    """Page through Graph users and keep only the directory fields we join on."""
    headers = {"Authorization": f"Bearer {token}"}
    params: dict[str, str] | None = {
        "$select": ",".join(USER_FIELDS),
        "$top": str(PAGE_SIZE),
    }
    url: str | None = f"{graph_base_url.rstrip('/')}/v1.0/users"
    users: list[dict[str, Any]] = []
    while url is not None:
        response = client.get(url, headers=headers, params=params)
        # nextLink already carries the query string.
        params = None
        if response.status_code == 403:
            message = (
                "Microsoft Graph refused the user list (403). "
                "The app registration needs the application permission "
                "User.Read.All, and an admin must grant consent for the tenant. "
                f"{_error_text(response)}"
            )
            raise RuntimeError(message)
        if response.status_code != 200:
            message = (
                f"Microsoft Graph user list failed ({response.status_code}): "
                f"{_error_text(response)}"
            )
            raise RuntimeError(message)
        body = response.json()
        for raw in body.get("value") or []:
            projected = _project_user(raw)
            if projected is not None:
                users.append(projected)
        next_link = body.get("@odata.nextLink")
        url = next_link if isinstance(next_link, str) and next_link else None
    logger.info("Listed %d Entra users", len(users))
    return users


def _project_user(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    user_id = raw.get("id")
    if not isinstance(user_id, str) or not user_id.strip():
        return None
    return {field: raw.get(field) for field in USER_FIELDS}


def _error_text(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:500]
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        description = error.get("error_description") or error.get("message")
        if isinstance(description, str) and description:
            return description
    return response.text[:500]
