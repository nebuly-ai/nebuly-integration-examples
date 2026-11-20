from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from copilot_sync.config import Config


@pytest.fixture(autouse=True)
def _no_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("copilot_sync.config.load_dotenv", lambda *_a, **_k: False)


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


def test_csv_export_does_not_require_nebuly_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("NEBULY_API_KEY")
    monkeypatch.setenv("CSV_EXPORT", "true")
    monkeypatch.setenv("CSV_EXPORT_PATH", "exports/out.csv")

    config = Config.from_env_and_args(["--from-date", "2025-06-15T08:00:00Z"])

    assert config.csv_export is True
    assert str(config.csv_export_path) == "exports/out.csv"


def test_missing_nebuly_api_key_raises_without_csv_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("NEBULY_API_KEY")
    monkeypatch.delenv("CSV_EXPORT", raising=False)

    with pytest.raises(RuntimeError, match="NEBULY_API_KEY"):
        Config.from_env_and_args(["--from-date", "2025-06-15T08:00:00Z"])


def _config(monkeypatch: pytest.MonkeyPatch) -> Config:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("CSV_EXPORT", raising=False)
    return Config.from_env_and_args(["--from-date", "2025-06-15T08:00:00Z"])


def test_cache_path_is_keyed_by_cache_key(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(monkeypatch)
    assert config.cache_path == config.cache_dir / config.cache_key / "sync_state.db"
    assert config.cache_key == _config(monkeypatch).cache_key


@pytest.mark.parametrize(
    "changes",
    [
        {"azure_tenant_id": "tenant_2"},
        {"azure_client_id": "client_2"},
        {"nebuly_api_key": "other_key"},
        {"nebuly_endpoint": "https://other.example.com/events"},
        {"csv_export": True},
    ],
)
def test_cache_key_changes_with_source_or_destination(
    monkeypatch: pytest.MonkeyPatch, changes: dict[str, object]
) -> None:
    config = _config(monkeypatch)
    assert replace(config, **cast(Any, changes)).cache_key != config.cache_key


def test_csv_cache_key_follows_export_path_not_nebuly_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(_config(monkeypatch), csv_export=True)
    assert replace(config, nebuly_api_key="other_key").cache_key == config.cache_key
    assert (
        replace(config, csv_export_path=Path("other.csv")).cache_key != config.cache_key
    )
