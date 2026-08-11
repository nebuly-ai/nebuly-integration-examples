"""Environment configuration for the Langfuse → Nebuly export script."""

from __future__ import annotations

import datetime
import os

from dotenv import load_dotenv

load_dotenv()

secret_key = os.getenv("LANGFUSE_SECRET_KEY")
public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
nebuly_api_key = os.getenv("NEBULY_API_KEY")

langfuse_base_url = os.getenv("LANGFUSE_BASE_URL", "https://cloud.langfuse.com").rstrip(
    "/"
)
nebuly_url = os.getenv(
    "NEBULY_ENDPOINT",
    "https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction",
).rstrip("/")
anonymize = os.getenv("ANONYMIZE", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

start_date_str = os.getenv("START_DATE", "2026-01-01")
end_date_str = os.getenv("END_DATE", "2026-12-31")

start_date = datetime.datetime.fromisoformat(start_date_str).replace(
    tzinfo=datetime.UTC
)
# END_DATE is inclusive; send the next day as the exclusive upper bound.
end_date = datetime.datetime.fromisoformat(end_date_str).replace(
    tzinfo=datetime.UTC
) + datetime.timedelta(days=1)


def require_env() -> None:
    missing = [
        name
        for name, val in [
            ("LANGFUSE_PUBLIC_KEY", public_key),
            ("LANGFUSE_SECRET_KEY", secret_key),
            ("NEBULY_API_KEY", nebuly_api_key),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
