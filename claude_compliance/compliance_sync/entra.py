from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from pathlib import Path


class EntraUser(BaseModel):
    """One directory user, using the Microsoft Graph field names."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    id: str | None = None
    mail: str | None = None
    user_principal_name: str | None = Field(None, alias="userPrincipalName")
    department: str | None = None
    job_title: str | None = Field(None, alias="jobTitle")
    office_location: str | None = Field(None, alias="officeLocation")
    city: str | None = None
    country: str | None = None
    usage_location: str | None = Field(None, alias="usageLocation")
    company_name: str | None = Field(None, alias="companyName")
    employee_hire_date: datetime | None = Field(None, alias="employeeHireDate")


class EntraExtract(BaseModel):
    model_config = ConfigDict(extra="ignore")

    users: list[EntraUser]


def _normalize_email(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().casefold()
    return normalized or None


def _non_empty(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _format_hire_date(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).date().isoformat()


def directory_tags(user: EntraUser) -> dict[str, str]:
    """Copilot-aligned directory tags. Empty values are left out."""
    raw = {
        "department": _non_empty(user.department),
        "job_title": _non_empty(user.job_title),
        "office_location": _non_empty(user.office_location),
        "city": _non_empty(user.city),
        "country": _non_empty(user.country) or _non_empty(user.usage_location),
        "company_name": _non_empty(user.company_name),
        "employee_hire_date": _format_hire_date(user.employee_hire_date),
    }
    return {key: value for key, value in raw.items() if value is not None}


class EntraDirectory:
    """Email index of an Entra extract. The first row for an email wins."""

    def __init__(self, users: list[EntraUser]) -> None:
        self.user_count = len(users)
        self._by_email: dict[str, EntraUser] = {}
        for user in users:
            for email in (user.mail, user.user_principal_name):
                key = _normalize_email(email)
                if key is not None and key not in self._by_email:
                    self._by_email[key] = user

    def tags_for_email(self, email: str | None) -> dict[str, str]:
        key = _normalize_email(email)
        if key is None:
            return {}
        user = self._by_email.get(key)
        if user is None:
            return {}
        return directory_tags(user)


def apply_directory_tags(
    tags: dict[str, str],
    directory: EntraDirectory | None,
    email: str | None,
) -> dict[str, str]:
    if directory is None:
        return tags
    merged = dict(tags)
    merged.update(directory.tags_for_email(email))
    return merged


def load_entra_extract(path: Path) -> EntraDirectory:
    """Load `{ "users": [...] }` or a bare array of Graph user objects."""
    if not path.is_file():
        raise RuntimeError(f"Entra extract not found: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Entra extract is not valid JSON: {path}") from exc
    if isinstance(raw, list):
        users = [EntraUser.model_validate(item) for item in raw]
    elif isinstance(raw, dict):
        users = EntraExtract.model_validate(raw).users
    else:
        raise TypeError(
            f"Entra extract must be an object with a users array, or an array: {path}"
        )
    return EntraDirectory(users)
