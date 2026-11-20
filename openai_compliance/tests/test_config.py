from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pytest
from openai_compliance_sync.config import Config

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _no_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "openai_compliance_sync.config.load_dotenv", lambda *_a, **_k: False
    )


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPLIANCE_API_KEY", "compliance_key")
    monkeypatch.setenv("COMPLIANCE_WORKSPACE_ID", "ws-1")
    monkeypatch.setenv("NEBULY_API_KEY", "nebuly_key")
    monkeypatch.delenv("CSV_EXPORT", raising=False)
    monkeypatch.delenv("CSV_EXPORT_PATH", raising=False)


def test_csv_export_defaults_off(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)

    config = Config.from_env_and_args([])

    assert config.csv_export is False
    assert str(config.csv_export_path) == "interactions.csv"


def test_csv_export_does_not_require_nebuly_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("NEBULY_API_KEY")
    monkeypatch.setenv("CSV_EXPORT", "true")
    monkeypatch.setenv("CSV_EXPORT_PATH", "exports/out.csv")

    config = Config.from_env_and_args([])

    assert config.csv_export is True
    assert str(config.csv_export_path) == "exports/out.csv"


def test_missing_nebuly_api_key_raises_without_csv_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("NEBULY_API_KEY")

    with pytest.raises(RuntimeError, match="NEBULY_API_KEY"):
        Config.from_env_and_args([])


def _keyed_config(tmp_path: Path) -> Config:
    return Config(
        nebuly_api_key="nebuly-key",
        nebuly_endpoint="https://nebuly.example/trace",
        compliance_api_key="compliance-key",
        compliance_base_url="https://api.chatgpt.com/v1/compliance",
        compliance_workspace_id="ws-demo",
        compliance_max_requests_per_minute=600,
        anonymize=False,
        from_date=None,
        to_date=None,
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
        csv_export_path=tmp_path / "out.csv",
    )


def test_store_path_is_keyed_by_cache_key(tmp_path: Path) -> None:
    config = _keyed_config(tmp_path)
    assert config.store_path == tmp_path / config.cache_key / "compliance_store.db"
    assert config.cache_key == _keyed_config(tmp_path).cache_key


@pytest.mark.parametrize(
    "changes",
    [
        {"compliance_workspace_id": "ws-other"},
        {"compliance_base_url": "https://other.example/v1/compliance"},
        {"nebuly_api_key": "other-key"},
        {"nebuly_endpoint": "https://other.example/trace"},
        {"csv_export": True},
    ],
)
def test_cache_key_changes_with_source_or_destination(
    tmp_path: Path, changes: dict[str, object]
) -> None:
    config = _keyed_config(tmp_path)
    assert replace(config, **cast(Any, changes)).cache_key != config.cache_key


def test_cache_key_ignores_compliance_api_key_rotation(tmp_path: Path) -> None:
    config = _keyed_config(tmp_path)
    assert replace(config, compliance_api_key="rotated").cache_key == config.cache_key


def test_csv_cache_key_follows_export_path_not_nebuly_key(tmp_path: Path) -> None:
    config = replace(_keyed_config(tmp_path), csv_export=True)
    assert replace(config, nebuly_api_key="other-key").cache_key == config.cache_key
    assert (
        replace(config, csv_export_path=tmp_path / "other.csv").cache_key
        != config.cache_key
    )
