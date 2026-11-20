from __future__ import annotations

import csv
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

CSV_COLUMNS: tuple[str, ...] = (
    "conversation_id",
    "end_user",
    "team",
    "market",
    "tenure",
    "time_start",
    "time_end",
    "input",
    "output",
    "hide_content",
    "tags",
    "traces",
    "user_feedback",
    "anonymize",
)


class CsvInteractionWriter:
    """Appends Nebuly interaction payloads to a CSV file instead of POSTing them."""

    def __init__(self, path: Path, *, dry_run: bool = False) -> None:
        self._path = path
        self._dry_run = dry_run

    def send_interaction(self, payload: dict[str, Any]) -> None:
        if self._dry_run:
            return

        interaction = payload["interaction"]
        row = {
            "conversation_id": interaction["conversation_id"],
            "end_user": interaction["end_user"],
            "team": interaction.get("team", ""),
            "market": interaction.get("market", ""),
            "tenure": interaction.get("tenure", ""),
            "time_start": interaction["time_start"],
            "time_end": interaction["time_end"],
            "input": interaction["input"],
            "output": interaction["output"],
            "hide_content": interaction.get("hide_content", False),
            "tags": json.dumps(interaction.get("tags", {}), ensure_ascii=False),
            "traces": json.dumps(payload.get("traces", []), ensure_ascii=False),
            "user_feedback": json.dumps(
                payload.get("user_feedback", []), ensure_ascii=False
            ),
            "anonymize": payload.get("anonymize", False),
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self._path.exists() or self._path.stat().st_size == 0
        with self._path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
