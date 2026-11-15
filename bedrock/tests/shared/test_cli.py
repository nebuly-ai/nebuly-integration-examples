"""Both commands share one boundary: JSON summary, exit codes, safe errors."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from bedrock_sync.agentcore import __main__ as agentcore_main
from bedrock_sync.agentcore.sync import SyncReport
from bedrock_sync.runtime import __main__ as runtime_main
from bedrock_sync.runtime.sync import SyncSummary
from bedrock_sync.shared.cli import run_command
from bedrock_sync.shared.delivery import DeliveryReport
from bedrock_sync.shared.errors import ConfigError


@dataclass(frozen=True)
class _Settings:
    verbose: bool = False


@dataclass(frozen=True)
class _Report:
    sent: int = 0
    failed: int = 0


def _run(report: _Report | None = None, error: BaseException | None = None) -> int:
    def sync(_: _Settings) -> _Report:
        if error is not None:
            raise error
        return report or _Report()

    return run_command(_Settings, sync, incomplete=lambda r: bool(r.failed))


def test_summary_is_printed_as_json(capsys: pytest.CaptureFixture[str]) -> None:
    assert _run(_Report(sent=2)) == 0
    assert json.loads(capsys.readouterr().out) == {"sent": 2, "failed": 0}


def test_incomplete_delivery_exits_with_one(capsys: pytest.CaptureFixture[str]) -> None:
    assert _run(_Report(failed=1)) == 1
    assert json.loads(capsys.readouterr().out)["failed"] == 1


def test_connector_errors_show_their_message(caplog: pytest.LogCaptureFixture) -> None:
    assert _run(error=ConfigError("BEDROCK_REGION is required")) == 1
    assert "BEDROCK_REGION is required" in caplog.text


def test_other_errors_show_only_their_type(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run(error=ValueError("private-token-and-prompt")) == 1
    assert "private-token-and-prompt" not in caplog.text
    assert "ValueError" in caplog.text
    assert not capsys.readouterr().out
    assert logging.getLogger("httpx").level == logging.WARNING


def test_interruption_exits_with_130() -> None:
    assert _run(error=KeyboardInterrupt()) == 130


@pytest.mark.parametrize(
    ("module", "report"),
    [
        (runtime_main, SyncSummary(delivery=DeliveryReport(uncertain=1))),
        (agentcore_main, SyncReport(rejected=1)),
    ],
)
def test_both_commands_use_the_shared_boundary(
    module: object, report: object, capsys: pytest.CaptureFixture[str]
) -> None:
    config_class = (
        "bedrock_sync.runtime.__main__.Config"
        if module is runtime_main
        else "bedrock_sync.agentcore.__main__.AgentCoreConfig"
    )
    with (
        patch(f"{config_class}.from_env_and_args"),
        patch.object(module, "run_sync", return_value=report),
    ):
        assert module.main([]) == 1  # type: ignore[attr-defined]
    assert json.loads(capsys.readouterr().out)


def test_package_command_points_to_the_connectors() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "bedrock_sync"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "bedrock_sync.runtime" in result.stderr
    assert "bedrock_sync.agentcore" in result.stderr
