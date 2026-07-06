from __future__ import annotations

import pytest
from gemini_enterprise_sync.config import Config


def test_bigquery_source_without_table_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEBULY_API_KEY", "key")
    monkeypatch.setenv("GCP_PROJECT_ID", "p")
    monkeypatch.setenv("GCP_LOCATION", "eu")
    monkeypatch.setenv("GCP_ENGINE_ID", "engine")
    monkeypatch.setenv("GCP_LOG_SOURCE", "bigquery")
    monkeypatch.delenv("GCP_BIGQUERY_TABLE", raising=False)

    with pytest.raises(RuntimeError, match="GCP_BIGQUERY_TABLE is required"):
        Config.from_env_and_args([])
