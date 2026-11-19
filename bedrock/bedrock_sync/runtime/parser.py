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
from typing import TYPE_CHECKING

from bedrock_sync.shared.cli import format_exception_stack
from bedrock_sync.shared.model_names import GEO_PREFIX_RE
from bedrock_sync.shared.time_utils import timestamp_str_to_datetime

from .formats import anthropic, converse, openai
from .formats.base import BodyFormatError, ParsedBody, as_dict, as_int
from .models import Invocation, JsonDict, RecordSkip, Skipped, TokenUsage

if TYPE_CHECKING:
    from collections.abc import Callable

    type Adapter = Callable[[JsonDict, object], ParsedBody]

logger = logging.getLogger(__name__)

_CONVERSE_OPERATIONS = frozenset({"Converse", "ConverseStream"})


def normalize_model_id(model_id: str) -> str:
    """``arn:...:inference-profile/global.openai.gpt-x`` -> ``openai.gpt-x``.

    Converse logs the id the caller passed, InvokeModel the full profile ARN;
    the geographic prefix of cross-region profiles is routing, not the model.
    """
    return GEO_PREFIX_RE.sub("", model_id.rsplit("/", 1)[-1])


def _adapter(
    operation: str, input_body: JsonDict, output_body: object
) -> Adapter | None:
    """The adapter of the body format: Converse, Anthropic or OpenAI."""
    if operation in _CONVERSE_OPERATIONS or converse.matches(output_body):
        return converse.parse
    if anthropic.matches(input_body, output_body):
        return anthropic.parse
    if openai.matches(output_body):
        return openai.parse
    return None


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


def _request_metadata(value: object) -> dict[str, str]:
    """The caller's ``requestMetadata``: a map for Converse; for InvokeModel the
    JSON string of the ``X-Amzn-Bedrock-Request-Metadata`` header, should the
    log keep it as a string."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {str(k): str(v) for k, v in as_dict(value).items() if v is not None}


def _log_skip(skipped: Skipped, source_key: str) -> None:
    level = (
        logging.DEBUG if skipped.reason is RecordSkip.ERROR_RECORD else logging.WARNING
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
    except Exception as exc:  # noqa: BLE001 - isolate adapter errors to one record
        # An adapter bug must cost one record, not the whole run.
        logger.error(  # noqa: TRY400 - the exception may contain the log body
            "Skipping request %s (unexpected %s) in %s",
            request_id,
            type(exc).__name__,
            source_key,
        )
        logger.debug(
            "Adapter error of request %s:\n%s",
            request_id,
            format_exception_stack(exc),
        )
        return Skipped(request_id, RecordSkip.INVALID_BODY, type(exc).__name__)
    if isinstance(result, Skipped):
        _log_skip(result, source_key)
    return result


def _parse(record: JsonDict, request_id: str, source_key: str) -> Invocation | Skipped:
    if record.get("errorCode"):
        return Skipped(request_id, RecordSkip.ERROR_RECORD, str(record["errorCode"]))
    if not request_id:
        # Bedrock always logs one. Without it a call cannot be deduplicated
        # nor read again from S3 after a checkpoint.
        return Skipped(request_id, RecordSkip.INVALID_BODY, "no requestId")
    try:
        # Python accepts non-JSON NaN/Infinity and escaped lone surrogates.
        # Skip these here: SQLite and the UTF-8 delivery would fail on every run.
        json.dumps(record, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except ValueError:
        return Skipped(request_id, RecordSkip.INVALID_BODY, "invalid JSON text")

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
        return Skipped(request_id, RecordSkip.MISSING_BODY, ", ".join(missing))

    operation = str(record.get("operation") or "")
    adapter = _adapter(operation, input_body, output_body)
    if adapter is None:
        return Skipped(request_id, RecordSkip.UNSUPPORTED_FORMAT, operation)

    try:
        body = adapter(input_body, output_body)
    except BodyFormatError:
        return Skipped(request_id, RecordSkip.INVALID_BODY, "invalid model body")
    try:
        timestamp = timestamp_str_to_datetime(str(record.get("timestamp") or ""))
    except ValueError:
        return Skipped(request_id, RecordSkip.INVALID_BODY, "invalid timestamp")

    return Invocation(
        request_id=request_id,
        timestamp=timestamp,
        latency_ms=body.latency_ms,
        model=normalize_model_id(str(record.get("modelId") or "")),
        account_id=str(record.get("accountId") or ""),
        request_metadata=_request_metadata(record.get("requestMetadata")),
        system=body.system,
        messages=body.messages,
        output=body.output,
        stop_reason=body.stop_reason,
        usage=_envelope_usage(input_section, output_section, body.usage),
        source_key=source_key,
    )
