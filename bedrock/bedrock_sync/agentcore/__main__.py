"""Run the AgentCore connector: ``python -m bedrock_sync.agentcore``."""

from __future__ import annotations

from bedrock_sync.shared.cli import run_command

from .config import AgentCoreConfig
from .sync import run_sync


def main(argv: list[str] | None = None) -> int:
    return run_command(
        lambda: AgentCoreConfig.from_env_and_args(argv),
        run_sync,
        incomplete=lambda report: report.incomplete,
    )


if __name__ == "__main__":
    raise SystemExit(main())
