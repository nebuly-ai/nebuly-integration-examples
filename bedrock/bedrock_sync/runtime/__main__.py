"""Run the runtime connector: ``python -m bedrock_sync.runtime``."""

from __future__ import annotations

from bedrock_sync.shared.cli import run_command

from .config import Config
from .sync import run_sync


def main(argv: list[str] | None = None) -> int:
    return run_command(
        lambda: Config.from_env_and_args(argv),
        run_sync,
        incomplete=lambda summary: summary.incomplete,
    )


if __name__ == "__main__":
    raise SystemExit(main())
