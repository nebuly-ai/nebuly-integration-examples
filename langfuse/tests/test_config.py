from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from langfuse_sync.config import Config


def test_run_until_applies_settle_lag() -> None:
    fixed_now = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)
    config = Config(
        langfuse_public_key="p",
        langfuse_secret_key="s",
        langfuse_base_url="https://example.com",
        nebuly_api_key="n",
        nebuly_endpoint="https://example.com/events",
        anonymize=False,
        settle_lag_seconds=900,
        from_date=None,
        to_date=None,
        cache_dir=Path(),
        dry_run=False,
        verbose=False,
        force=False,
    )
    with patch("langfuse_sync.config.datetime") as dt_mod:
        dt_mod.now.return_value = fixed_now
        dt_mod.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        dt_mod.UTC = UTC
        dt_mod.timedelta = timedelta
        assert config.run_until() == fixed_now - timedelta(seconds=900)
