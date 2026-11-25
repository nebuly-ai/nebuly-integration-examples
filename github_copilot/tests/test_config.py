from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pytest
from github_copilot_sync.config import Config

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _no_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "github_copilot_sync.config.load_dotenv", lambda *_a, **_k: False
    )


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "gh_token")
    monkeypatch.setenv("GITHUB_ENTERPRISE", "acme-corp")
    monkeypatch.setenv("NEBULY_API_KEY", "nebuly_key")
    monkeypatch.delenv("CSV_EXPORT", raising=False)


def test_missing_github_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("GITHUB_TOKEN")

    with pytest.raises(RuntimeError, match="GITHUB_TOKEN"):
        Config.from_env_and_args([])


def test_csv_export_skips_nebuly_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("NEBULY_API_KEY")
    monkeypatch.setenv("CSV_EXPORT", "true")

    config = Config.from_env_and_args([])

    assert config.csv_export is True


def _keyed_config(tmp_path: Path) -> Config:
    return Config(
        github_token="gh",
        github_enterprise="ent",
        github_api_url="https://api.github.com",
        github_api_version="2026-03-10",
        nebuly_api_key="nebuly",
        nebuly_endpoint="https://nebuly.example/trace",
        sources=("rest",),
        pair_wait_minutes=15,
        conversation_gap_hours=8,
        interaction_idle_minutes=30,
        rest_overlap_seconds=60,
        resolve_users=True,
        s3_bucket="",
        s3_prefix="",
        s3_endpoint_url=None,
        azure_storage_connection_string=None,
        azure_blob_account_url=None,
        azure_blob_container="",
        azure_blob_prefix="",
        azure_blob_sas_token=None,
        anonymize=False,
        from_date=None,
        to_date=None,
        cache_dir=tmp_path,
        dry_run=False,
        verbose=False,
    )


def test_store_path_under_cache_key(tmp_path: Path) -> None:
    config = _keyed_config(tmp_path)
    assert config.store_path == tmp_path / config.cache_key / "copilot_store.db"


@pytest.mark.parametrize(
    "changes",
    [
        {"github_enterprise": "other"},
        {"csv_export": True},
    ],
)
def test_cache_key_follows_enterprise_and_destination(
    tmp_path: Path, changes: dict[str, object]
) -> None:
    config = _keyed_config(tmp_path)
    assert replace(config, **cast(Any, changes)).cache_key != config.cache_key


def test_cache_key_ignores_sources(tmp_path: Path) -> None:
    config = _keyed_config(tmp_path)
    widened = replace(config, sources=("rest", "s3", "azure_blob"))
    assert widened.cache_key == config.cache_key
    assert widened.store_path == config.store_path


def test_sources_default_to_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)

    assert Config.from_env_and_args([]).sources == ("rest",)
    assert Config.from_env_and_args(["--sources", "rest,s3"]).sources == (
        "rest",
        "s3",
    )


def test_object_lag_seconds_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    assert Config.from_env_and_args([]).object_lag_seconds == 3600
    monkeypatch.setenv("OBJECT_LAG_SECONDS", "120")
    assert Config.from_env_and_args([]).object_lag_seconds == 120
