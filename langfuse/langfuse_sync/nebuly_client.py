"""Nebuly Interaction API client."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests
from langfuse_sync import config

if TYPE_CHECKING:
    from langfuse_sync.models import Interaction, NebulyRequestPayload

logger = logging.getLogger(__name__)


def send_interactions(interactions: list[Interaction]) -> None:
    skipped_too_large = 0
    for interaction in interactions:
        payload: NebulyRequestPayload = {
            "interaction": interaction.to_interaction_dict(),
            "traces": [trace.to_dict() for trace in interaction.traces],
            "user_feedback": [],
            "anonymize": config.anonymize,
        }
        response = requests.post(
            config.nebuly_url,
            headers={
                "Authorization": f"Bearer {config.nebuly_api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )
        if response.status_code == 413:
            skipped_too_large += 1
            logger.warning(
                "Skipping interaction conversation_id=%s: payload too large (413)",
                interaction.conversation_id,
            )
            continue
        if not response.ok:
            raise RuntimeError(
                f"Nebuly POST failed: status={response.status_code} "
                f"body={response.text!r}"
            ) from None
    if skipped_too_large:
        logger.warning(
            "Skipped %s interactions due to 413 Request Entity Too Large",
            skipped_too_large,
        )
