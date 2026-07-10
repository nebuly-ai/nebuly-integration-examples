from __future__ import annotations

from unittest.mock import patch

import pytest
from gemini_enterprise_sync.config import Config


@patch("gemini_enterprise_sync.config.load_dotenv")
def test_bigquery_source_without_table_raises(
    load_dotenv: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("GCP_PROJECT_ID", "p")
    monkeypatch.setenv("GCP_LOCATION", "eu")
    monkeypatch.setenv("GCP_ENGINE_ID", "engine")
    monkeypatch.setenv("GCP_LOG_SOURCE", "bigquery")
    monkeypatch.delenv("GCP_BIGQUERY_TABLE", raising=False)

    with pytest.raises(RuntimeError, match="GCP_BIGQUERY_TABLE is required"):
        Config.from_env_and_args([])


@patch("gemini_enterprise_sync.config.load_dotenv")
def test_missing_user_hash_secret_raises(
    load_dotenv: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("GCP_PROJECT_ID", "p")
    monkeypatch.setenv("GCP_LOCATION", "eu")
    monkeypatch.setenv("GCP_ENGINE_ID", "engine")
    monkeypatch.delenv("USER_HASH_SECRET", raising=False)
    monkeypatch.delenv("SEND_PLAIN_END_USER", raising=False)

    with pytest.raises(RuntimeError, match="USER_HASH_SECRET is required"):
        Config.from_env_and_args([])
