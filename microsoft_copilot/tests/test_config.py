from __future__ import annotations

import pytest
from copilot_sync.config import Config


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant_1")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client_1")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret_1")
    monkeypatch.setenv("NEBULY_API_KEY", "nebuly_key")


def test_from_date_after_to_date_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)

    with pytest.raises(RuntimeError, match="cannot be after"):
        Config.from_env_and_args(
            [
                "--from-date",
                "2025-06-15T12:00:00Z",
                "--to-date",
                "2025-06-15T08:00:00Z",
            ],
        )


def test_ordered_dates_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)

    config = Config.from_env_and_args(
        [
            "--from-date",
            "2025-06-15T08:00:00Z",
            "--to-date",
            "2025-06-15T12:00:00Z",
        ],
    )

    assert config.from_date is not None
    assert config.to_date is not None
    assert config.from_date < config.to_date


def test_audit_enrichment_env_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("AUDIT_ENRICHMENT", "true")
    monkeypatch.setenv("AUDIT_SETTLE_LAG_SECONDS", "3600")

    config = Config.from_env_and_args(
        ["--from-date", "2025-06-15T08:00:00Z"],
    )

    assert config.audit_enrichment is True
    assert config.audit_settle_lag_seconds == 3600
