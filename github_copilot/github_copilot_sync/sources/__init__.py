from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from github_copilot_sync.models import PipelineCounters, UsageRecord

if TYPE_CHECKING:
    from collections.abc import Iterator

    from github_copilot_sync.config import Config


@dataclass
class SourceStats:
    non_usage_events: int = 0
    unreadable_files: int = 0
    unreadable_lines: int = 0

    def absorb_decode(self, *, files: int, lines: int) -> None:
        self.unreadable_files += files
        self.unreadable_lines += lines

    def merge_counters(self, counters: PipelineCounters) -> None:
        counters.non_usage_events += self.non_usage_events


class RecordSource(Protocol):
    stats: SourceStats

    def iter_records(self, cursor: str | None) -> Iterator[tuple[UsageRecord, str]]: ...


def build_sources(config: Config) -> dict[str, RecordSource]:
    from .azure_blob import AzureBlobUsageSource
    from .rest import RestUsageSource
    from .s3 import S3UsageSource

    sources: dict[str, RecordSource] = {}
    if "rest" in config.sources:
        sources["rest"] = RestUsageSource(config)
    if "s3" in config.sources:
        sources["s3"] = S3UsageSource(config)
    if "azure_blob" in config.sources:
        sources["azure_blob"] = AzureBlobUsageSource(config)
    return sources
