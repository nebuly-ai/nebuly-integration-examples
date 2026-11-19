"""Model ids as Nebuly stores them, the same for both connectors."""

from __future__ import annotations

import pytest

from bedrock_sync.shared.model_names import observed_model_name


@pytest.mark.parametrize(
    "prefix", ["", "global.", "us.", "us-gov.", "eu.", "apac.", "jp.", "au.", "ca."]
)
def test_every_cross_region_prefix_is_removed(prefix: str) -> None:
    model = f"{prefix}anthropic.claude-sonnet-4-5-20250929-v1:0"
    assert observed_model_name(model) == "claude-sonnet-4-5"


def test_unknown_ids_are_kept_as_observed() -> None:
    assert observed_model_name("my-custom-model") == "my-custom-model"
    assert observed_model_name("xx.anthropic.claude-x") == "xx.anthropic.claude-x"
