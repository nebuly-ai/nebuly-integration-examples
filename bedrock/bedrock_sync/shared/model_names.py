"""Bedrock model naming adapters; no pricing lookup or model guessing."""

import re

_PROVIDERS = (
    "anthropic",
    "openai",
    "amazon",
    "meta",
    "mistral",
    "cohere",
    "ai21",
    "deepseek",
    "qwen",
    "writer",
    "google",
    "moonshotai",
    "minimax",
    "nvidia",
    "zai",
)
_PROVIDER_RE = re.compile(rf"^({'|'.join(_PROVIDERS)})\.")
_NAME_WITHOUT_PROVIDER = frozenset({"deepseek"})
"""Providers whose Bedrock ids drop the family name: ``deepseek.r1-v1:0``."""
_BEDROCK_VERSION_RE = re.compile(r"(?:-v\d+(?::\d+)?|-\d+:\d+|:\d+)(?::\d+k|:mm)?$")
_DATE_RE = re.compile(r"-(?:\d{8}|\d{4}-\d{2}-\d{2})$")
_LEGACY_NAMES = {
    "anthropic.claude-v2": "claude-2.0",
    "anthropic.claude-v2:1": "claude-2.1",
    "anthropic.claude-instant-v1": "claude-instant-1.2",
}
"""Old ids whose ``-vN`` is the model version, not the Bedrock one."""


def nebuly_model_name(model: str) -> str:
    """Bedrock model id -> the model name as Nebuly stores it: no provider,
    no Bedrock version, no release date.
    ``anthropic.claude-haiku-4-5-20251001-v1:0`` -> ``claude-haiku-4-5``,
    ``openai.gpt-oss-120b-1:0`` -> ``gpt-oss-120b``,
    ``deepseek.r1-v1:0`` -> ``deepseek-r1``; the context-window suffix of
    provisioned models (``:200k``) goes too. Expects the id already stripped
    of ARN and geo prefix (``Invocation.model``).

    An application inference profile logs only its own ARN, so the name is
    the profile id at its end (``abc123``): the model is not in the log.
    """
    if model in _LEGACY_NAMES:
        return _LEGACY_NAMES[model]
    provider_match = _PROVIDER_RE.match(model)
    name = _BEDROCK_VERSION_RE.sub("", _PROVIDER_RE.sub("", model))
    name = _DATE_RE.sub("", name)
    if provider_match and provider_match.group(1) in _NAME_WITHOUT_PROVIDER:
        return f"{provider_match.group(1)}-{name}"
    return name


def observed_model_name(model: str) -> str:
    """Normalize known provider IDs, preserving unknown observed IDs exactly."""
    candidate = re.sub(r"^(global|us|eu|apac)\.", "", model)
    if not _PROVIDER_RE.match(candidate):
        return model
    return nebuly_model_name(candidate)
