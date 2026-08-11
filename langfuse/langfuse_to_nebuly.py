"""Fetch Langfuse traces and send them to Nebuly (Interaction API v3)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from langfuse_sync import config
from langfuse_sync.converter import interaction_from_langfuse_trace
from langfuse_sync.langfuse_client import get_observations_by_trace_id, get_traces
from langfuse_sync.nebuly_client import send_interactions

if TYPE_CHECKING:
    from langfuse_sync.models import Interaction

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config.require_env()

    traces = get_traces(config.start_date, config.end_date)
    logger.info(
        "Fetched %s Langfuse traces from %s (%s → %s)",
        len(traces),
        config.langfuse_base_url,
        config.start_date_str,
        config.end_date_str,
    )

    observations_by_trace = get_observations_by_trace_id(
        config.start_date, config.end_date
    )
    logger.info(
        "Fetched observations for %s traces",
        len(observations_by_trace),
    )

    interactions: list[Interaction] = []
    skipped = 0
    for trace in traces:
        interaction = interaction_from_langfuse_trace(
            trace, observations_by_trace.get(trace["id"], [])
        )
        if interaction is None:
            skipped += 1
            continue
        interactions.append(interaction)

    if skipped:
        logger.info("Skipped %s traces with empty input and output", skipped)

    send_interactions(interactions)
    logger.info("Sent %s interactions to Nebuly", len(interactions))


if __name__ == "__main__":
    main()
