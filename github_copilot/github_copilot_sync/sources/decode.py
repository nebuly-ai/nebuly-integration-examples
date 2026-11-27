from __future__ import annotations

import gzip
import json
import logging
import zlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from github_copilot_sync.models import JsonDict

logger = logging.getLogger(__name__)


@dataclass
class DecodeProblems:
    unreadable_files: int = 0
    unreadable_lines: int = 0


def decode_ndjson(raw: bytes, *, source: str) -> tuple[list[JsonDict], DecodeProblems]:
    """Decode gzip-or-plain NDJSON object payloads (streaming export format)."""
    problems = DecodeProblems()
    try:
        payload = gzip.decompress(raw)
    except gzip.BadGzipFile:
        payload = raw
    except (EOFError, zlib.error):
        logger.warning("Corrupt log file %s skipped, its records are lost", source)
        problems.unreadable_files += 1
        return [], problems
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning(
            "Log file %s is not UTF-8 and was skipped, its records are lost",
            source,
        )
        problems.unreadable_files += 1
        return [], problems

    records: list[JsonDict] = []
    for line_no, line in enumerate(text.split("\n"), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            logger.warning(
                "Line %s of %s is not JSON and was skipped",
                line_no,
                source,
            )
            problems.unreadable_lines += 1
            continue
        if isinstance(value, dict):
            records.append(value)
    return records, problems
