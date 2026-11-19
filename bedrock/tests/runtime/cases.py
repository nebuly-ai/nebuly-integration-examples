"""Helpers over the anonymized runtime cases in ``fixtures/``."""

from __future__ import annotations

import gzip
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bedrock_sync.runtime.config import Config
from bedrock_sync.runtime.s3_client import LocalObjectStore
from bedrock_sync.runtime.sync import sync

if TYPE_CHECKING:
    from bedrock_sync.runtime.sync import SyncSummary

FIXTURES = Path(__file__).with_name("fixtures")
CASES = sorted(path.stem for path in FIXTURES.glob("*.json"))


def load(name: str) -> dict[str, Any]:
    case: dict[str, Any] = json.loads((FIXTURES / f"{name}.json").read_text())
    return case


def write_bucket(case: dict[str, Any], root: Path) -> None:
    """Lay the case's log files and external bodies out like the bucket."""
    for obj in case["objects"]:
        lines = "".join(json.dumps(record) + "\n" for record in obj["records"])
        _write(root / obj["key"], lines)
    for body in case["bodies"]:
        _write(root / body["key"], json.dumps(body["body"]))


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(text.encode()))


def config(case: dict[str, Any], state_dir: Path) -> Config:
    source = case["source"]
    return Config(
        account_id=source["account_id"],
        region=source["region"],
        aws_role_arn=None,
        aws_external_id=None,
        nebuly_api_key="nb-key",
        nebuly_endpoint="https://backend.example.test/trace_interaction",
        user_hash_secret="",
        pseudonymize_end_user=False,
        anonymize=False,
        from_date=datetime.fromisoformat(case["run"]["from_date"]),
        to_date=None,
        state_db=state_dir / "state.db",
        content_retention_seconds=604800,
        dry_run=False,
        verbose=False,
        s3_bucket=source["bucket"],
        s3_prefix="",
    )


def run(case: dict[str, Any], tmp_path: Path) -> tuple[SyncSummary, list[Any]]:
    """One connector run over the case; returns its summary and payloads."""
    write_bucket(case, tmp_path / "bucket")
    payloads: list[Any] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return httpx.Response(200)

    summary = sync(
        config(case, tmp_path),
        LocalObjectStore(tmp_path / "bucket"),
        transport=httpx.MockTransport(handler),
        now=datetime.fromisoformat(case["run"]["now"]),
    )
    return summary, payloads


def counters(summary: SyncSummary) -> dict[str, Any]:
    """The nonzero grouping counters and the skip reasons of a run."""
    return {
        "grouping": {k: v for k, v in asdict(summary.grouping).items() if v},
        "records_skipped": summary.totals.records_skipped,
        "interactions_skipped": summary.totals.interactions_skipped,
        "open_interactions": summary.totals.open_interactions,
    }


def tokens_by_model(payload: dict[str, Any]) -> dict[str, dict[str, int]]:
    return {
        trace["model"]: {
            key: trace[key]
            for key in (
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_write_input_tokens",
            )
        }
        for trace in payload["traces"]
        if "model" in trace
    }


def summary_of(payload: dict[str, Any]) -> dict[str, Any]:
    """The fields of a payload the fixtures pin down."""
    interaction = payload["interaction"]
    return {
        "conversation_id": interaction["conversation_id"],
        "end_user": interaction["end_user"],
        "input": interaction["input"],
        "output": interaction["output"],
        "tokens": tokens_by_model(payload),
        "tools": sorted(
            trace["source"] for trace in payload["traces"] if "source" in trace
        ),
    }
