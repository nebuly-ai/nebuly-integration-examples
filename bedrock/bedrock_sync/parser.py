"""Turn a raw invocation log record into an ``Invocation``.

The parser reads the envelope common to every record, picks the adapter from
the operation and the body shape, and merges token counts. It does no I/O:
bodies stored in ``data/`` are inlined beforehand by the S3 client.

Every skipped record is logged: a failed call (``errorCode``, for example a
throttled request the client retried) at DEBUG, since it is not a sync
problem; a record we could not read at WARNING, with the request id and the
S3 key so it can be looked up.
"""

from __future__ import annotations

import json
import logging
import re

from .config import timestamp_str_to_datetime
from .formats import anthropic, converse, openai
from .formats.base import BodyFormatError, ParsedBody, as_dict, as_int
from .models import (
    ApiFormat,
    ClaudeCodeMetadata,
    Invocation,
    JsonDict,
    Skipped,
    SkipReason,
    TokenUsage,
)

logger = logging.getLogger(__name__)

_CONVERSE_OPERATIONS = frozenset({"Converse", "ConverseStream"})
_GEO_PREFIX_RE = re.compile(r"^(global|us|us-gov|eu|apac|jp|au|ca)\.")


def normalize_model_id(model_id: str) -> str:
    """``arn:...:inference-profile/global.openai.gpt-x`` -> ``openai.gpt-x``.

    Converse logs the id the caller passed, InvokeModel the full profile ARN;
    the geographic prefix of cross-region profiles is routing, not the model.
    """
    return _GEO_PREFIX_RE.sub("", model_id.rsplit("/", 1)[-1])


def _detect_format(
    operation: str, input_body: JsonDict, output_body: object
) -> ApiFormat | None:
    if operation in _CONVERSE_OPERATIONS or converse.matches(output_body):
        return ApiFormat.CONVERSE
    if anthropic.matches(input_body, output_body):
        return ApiFormat.ANTHROPIC
    if openai.matches(output_body):
        return ApiFormat.OPENAI
    return None


def _parse_body(
    api_format: ApiFormat, input_body: JsonDict, output_body: object
) -> ParsedBody:
    if api_format is ApiFormat.CONVERSE:
        return converse.parse(input_body, output_body)
    if api_format is ApiFormat.ANTHROPIC:
        return anthropic.parse(input_body, output_body)
    return openai.parse(input_body, output_body)


def _envelope_usage(
    input_section: JsonDict, output_section: JsonDict, body: TokenUsage | None
) -> TokenUsage:
    """Envelope counters are Bedrock's own and win; the body fills the gaps."""
    body = body or TokenUsage()

    def pick(section: JsonDict, key: str, fallback: int) -> int:
        value = as_int(section.get(key))
        return fallback if value is None else value

    return TokenUsage(
        input_tokens=pick(input_section, "inputTokenCount", body.input_tokens),
        output_tokens=pick(output_section, "outputTokenCount", body.output_tokens),
        cache_read_tokens=pick(
            input_section, "cacheReadInputTokenCount", body.cache_read_tokens
        ),
        cache_write_tokens=pick(
            input_section, "cacheWriteInputTokenCount", body.cache_write_tokens
        ),
    )


def _claude_code_metadata(input_body: JsonDict) -> ClaudeCodeMetadata | None:
    raw = as_dict(input_body.get("metadata")).get("user_id")
    if not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return ClaudeCodeMetadata(
        session_id=parsed.get("session_id") or None,
        device_id=parsed.get("device_id") or None,
        account_uuid=parsed.get("account_uuid") or None,
    )


def _log_skip(skipped: Skipped, source_key: str) -> None:
    level = (
        logging.DEBUG if skipped.reason is SkipReason.ERROR_RECORD else logging.WARNING
    )
    logger.log(
        level,
        "Skipping request %s (%s%s) in %s",
        skipped.request_id or "<no id>",
        skipped.reason.value,
        f": {skipped.detail}" if skipped.detail else "",
        source_key or "<unknown key>",
    )


def parse_record(record: JsonDict, *, source_key: str = "") -> Invocation | Skipped:
    request_id = str(record.get("requestId") or "")
    try:
        result = _parse(record, request_id, source_key)
    except Exception as exc:
        # An adapter bug must cost one record, not the whole run.
        logger.exception(
            "Skipping request %s (unexpected error) in %s", request_id, source_key
        )
        return Skipped(
            request_id, SkipReason.INVALID_BODY, f"{type(exc).__name__}: {exc}"
        )
    if isinstance(result, Skipped):
        _log_skip(result, source_key)
    return result


def _parse(record: JsonDict, request_id: str, source_key: str) -> Invocation | Skipped:
    if record.get("errorCode"):
        return Skipped(request_id, SkipReason.ERROR_RECORD, str(record["errorCode"]))
    if not request_id:
        # Bedrock always logs one. Without it a call cannot be deduplicated
        # nor read again from S3 after a checkpoint.
        return Skipped(request_id, SkipReason.INVALID_BODY, "no requestId")

    input_section = as_dict(record.get("input"))
    output_section = as_dict(record.get("output"))
    input_body = input_section.get("inputBodyJson")
    output_body = output_section.get("outputBodyJson")
    if not isinstance(input_body, dict) or output_body is None:
        missing = []
        if not isinstance(input_body, dict):
            missing.append("input")
        if output_body is None:
            missing.append("output")
        return Skipped(request_id, SkipReason.MISSING_BODY, ", ".join(missing))

    operation = str(record.get("operation") or "")
    api_format = _detect_format(operation, input_body, output_body)
    if api_format is None:
        return Skipped(request_id, SkipReason.UNSUPPORTED_FORMAT, operation)

    try:
        body = _parse_body(api_format, input_body, output_body)
        timestamp = timestamp_str_to_datetime(str(record.get("timestamp") or ""))
    except (BodyFormatError, ValueError) as exc:
        return Skipped(request_id, SkipReason.INVALID_BODY, str(exc))

    model_id = str(record.get("modelId") or "")
    metadata = {
        str(k): str(v) for k, v in as_dict(record.get("requestMetadata")).items()
    }
    return Invocation(
        request_id=request_id,
        timestamp=timestamp,
        latency_ms=body.latency_ms,
        operation=operation,
        api_format=api_format,
        model=normalize_model_id(model_id),
        model_id=model_id,
        region=str(record.get("region") or ""),
        inference_region=str(record.get("inferenceRegion") or ""),
        account_id=str(record.get("accountId") or ""),
        identity_arn=str(as_dict(record.get("identity")).get("arn") or ""),
        request_metadata=metadata,
        system=body.system,
        messages=body.messages,
        output=body.output,
        stop_reason=body.stop_reason,
        usage=_envelope_usage(input_section, output_section, body.usage),
        max_tokens=body.max_tokens,
        claude_code=_claude_code_metadata(input_body),
        source_key=source_key,
    )
