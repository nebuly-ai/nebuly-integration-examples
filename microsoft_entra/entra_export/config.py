from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    azure_tenant_id: str
    azure_client_id: str
    azure_client_secret: str
    output: Path
    dry_run: bool
    verbose: bool
    login_base_url: str = "https://login.microsoftonline.com"
    graph_base_url: str = "https://graph.microsoft.com"

    @classmethod
    def from_env_and_args(cls, argv: list[str] | None = None) -> Config:
        load_dotenv()
        parser = argparse.ArgumentParser(
            description=(
                "Export Microsoft Entra users to the JSON file "
                "Claude Compliance reads via ENTRA_EXTRACT_PATH"
            ),
        )
        parser.add_argument(
            "--output",
            type=str,
            default="entra.json",
            help="Path of the JSON extract to write (default: entra.json)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List users and print the count without writing a file",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable debug logging",
        )
        args = parser.parse_args(argv)

        azure_tenant_id = os.environ.get("AZURE_TENANT_ID", "").strip()
        azure_client_id = os.environ.get("AZURE_CLIENT_ID", "").strip()
        azure_client_secret = os.environ.get("AZURE_CLIENT_SECRET", "").strip()
        missing = [
            name
            for name, value in (
                ("AZURE_TENANT_ID", azure_tenant_id),
                ("AZURE_CLIENT_ID", azure_client_id),
                ("AZURE_CLIENT_SECRET", azure_client_secret),
            )
            if not value
        ]
        if missing:
            message = f"Missing required env vars: {', '.join(missing)}"
            raise RuntimeError(message)

        return cls(
            azure_tenant_id=cast("str", azure_tenant_id),
            azure_client_id=cast("str", azure_client_id),
            azure_client_secret=cast("str", azure_client_secret),
            output=Path(args.output),
            dry_run=args.dry_run,
            verbose=args.verbose,
            login_base_url=_base_url(
                "AZURE_LOGIN_BASE_URL",
                "https://login.microsoftonline.com",
            ),
            graph_base_url=_base_url(
                "GRAPH_BASE_URL",
                "https://graph.microsoft.com",
            ),
        )


def _base_url(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return (value or default).rstrip("/")
