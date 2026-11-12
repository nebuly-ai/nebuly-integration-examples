# Standalone CLI meant for AWS CloudShell (Python 3.9): prints its report and
# cannot use datetime.UTC (3.11+).
# ruff: noqa: T201, INP001, UP017, ANN401

"""Generate a representative set of Bedrock invocations for fixture collection.

Run it where AWS credentials are already available (e.g. AWS CloudShell):

    python3 generate_test_invocations.py --model global.openai.gpt-6.1-sol
    python3 generate_test_invocations.py --model global.anthropic.claude-haiku-5-5
    python3 generate_test_invocations.py --model <id> --only tool_use,native_stream
    python3 generate_test_invocations.py --model <id> --list

Scenarios are grouped in three sets:

* Converse API (model agnostic): single, multi_turn, system_prompt, tool_use,
  stream, regenerate, large_payload, plus reasoning for Anthropic models.
* Native API (body depends on the provider family): native, native_stream.
* Claude Code-like traffic on the Anthropic native API (as seen in real logs):
  anthropic_tools, claude_code_session, single_token_probe.

Scenarios that do not apply to the model family are skipped. Each scenario is
independent: a failure is reported and the script moves on. The printed request
ids match the `requestId` field of the invocation log records.

Only depends on boto3 (preinstalled in CloudShell). Python >= 3.9.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3

DEFAULT_REGION = "eu-north-1"
MAX_TOKENS = 512
ANTHROPIC_VERSION = "bedrock-2023-05-31"

RUN_ID = uuid.uuid4().hex[:8]

results: list[dict[str, Any]] = []


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def model_family(model_id: str) -> str:
    """Provider family from a model id, inference profile id or ARN."""
    name = model_id.rsplit("/", 1)[-1].lower()
    for family, marker in (
        ("anthropic", "anthropic."),
        ("openai", "openai."),
        ("meta", "meta."),
        ("nova", "amazon.nova"),
        ("mistral", "mistral."),
    ):
        if marker in name:
            return family
    return "unknown"


def _metadata(scenario: str, session_id: str, user_id: str) -> dict[str, str]:
    # Converse requestMetadata: we want to see whether/how it lands in the log.
    return {
        "nebuly_test_run": RUN_ID,
        "scenario": scenario,
        "session_id": session_id,
        "user_id": user_id,
    }


def _ok(
    scenario: str,
    request_id: str,
    stop: str = "",
    tokens_in: object = "?",
    tokens_out: object = "?",
    note: str = "",
) -> None:
    results.append(
        {
            "scenario": scenario,
            "time": _now(),
            "request_id": request_id or "?",
            "stop_reason": stop or "",
            "tokens": f"{tokens_in}/{tokens_out}",
            "note": note,
        }
    )
    print(f"  ok  request_id={request_id} stop={stop}")


def _record_converse(scenario: str, response: dict[str, Any], note: str = "") -> None:
    usage = response.get("usage", {})
    _ok(
        scenario,
        response.get("ResponseMetadata", {}).get("RequestId", "?"),
        response.get("stopReason", ""),
        usage.get("inputTokens", "?"),
        usage.get("outputTokens", "?"),
        note,
    )


def _fail(scenario: str, exc: Exception) -> None:
    msg = str(exc)
    hint = ""
    if "on-demand throughput" in msg or "inference profile" in msg:
        hint = " (hint: use the inference profile id, e.g. eu.<model-id>)"
    results.append(
        {
            "scenario": scenario,
            "time": _now(),
            "request_id": "-",
            "stop_reason": "ERROR",
            "tokens": "-",
            "note": msg[:200] + hint,
        }
    )
    print(f"  ERR {msg[:300]}{hint}")


def _user(text: str) -> dict[str, Any]:
    return {"role": "user", "content": [{"text": text}]}


# --------------------------------------------------------------------------- #
# Converse scenarios (model agnostic)
# --------------------------------------------------------------------------- #


def scenario_single(client: Any, model: str) -> None:
    response = client.converse(
        modelId=model,
        messages=[_user("In one sentence, what is Amazon Bedrock?")],
        inferenceConfig={"maxTokens": MAX_TOKENS},
        requestMetadata=_metadata("single", f"single-{RUN_ID}", "alice@example.com"),
    )
    _record_converse("single", response)


def scenario_multi_turn(client: Any, model: str) -> None:
    session_id = f"multi-{RUN_ID}"
    questions = [
        "I'm planning a 3-day trip to Stockholm. Suggest one thing per day.",
        "Which of those is best if it rains?",
        "Great, and where should I eat near there?",
    ]
    messages: list[dict[str, Any]] = []
    for turn, question in enumerate(questions, start=1):
        messages.append(_user(question))
        response = client.converse(
            modelId=model,
            messages=messages,
            inferenceConfig={"maxTokens": MAX_TOKENS},
            requestMetadata=_metadata(
                f"multi_turn_{turn}", session_id, "bob@example.com"
            ),
        )
        messages.append(response["output"]["message"])
        _record_converse(
            f"multi_turn_{turn}", response, note=f"history={len(messages) - 2}"
        )


def scenario_system_prompt(client: Any, model: str) -> None:
    response = client.converse(
        modelId=model,
        system=[
            {
                "text": "You are the internal HR assistant of ACME Corp. "
                "Answer briefly and always in Italian."
            }
        ],
        messages=[_user("How many vacation days do I have per year?")],
        inferenceConfig={"maxTokens": MAX_TOKENS},
        requestMetadata=_metadata(
            "system_prompt", f"system-{RUN_ID}", "carol@example.com"
        ),
    )
    _record_converse("system_prompt", response)


CONVERSE_TOOL_CONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "get_weather",
                "description": "Get the current weather for a city.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "city": {"type": "string", "description": "City name"}
                        },
                        "required": ["city"],
                    }
                },
            }
        }
    ]
}


def scenario_tool_use(client: Any, model: str) -> None:
    session_id = f"tools-{RUN_ID}"
    messages: list[dict[str, Any]] = [
        _user("What's the weather like in Stockholm right now?")
    ]
    for step in range(1, 4):
        response = client.converse(
            modelId=model,
            messages=messages,
            toolConfig=CONVERSE_TOOL_CONFIG,
            inferenceConfig={"maxTokens": MAX_TOKENS},
            requestMetadata=_metadata(
                f"tool_use_{step}", session_id, "dave@example.com"
            ),
        )
        assistant = response["output"]["message"]
        messages.append(assistant)
        _record_converse(f"tool_use_{step}", response)
        if response.get("stopReason") != "tool_use":
            return
        tool_results = []
        for block in assistant["content"]:
            if "toolUse" not in block:
                continue
            tool_use = block["toolUse"]
            city = tool_use.get("input", {}).get("city", "unknown")
            tool_results.append(
                {
                    "toolResult": {
                        "toolUseId": tool_use["toolUseId"],
                        "content": [
                            {"json": {"city": city, "temp_c": 4, "sky": "cloudy"}}
                        ],
                    }
                }
            )
        messages.append({"role": "user", "content": tool_results})


def scenario_stream(client: Any, model: str) -> None:
    response = client.converse_stream(
        modelId=model,
        messages=[_user("Write a haiku about log files.")],
        inferenceConfig={"maxTokens": MAX_TOKENS},
        requestMetadata=_metadata("stream", f"stream-{RUN_ID}", "erin@example.com"),
    )
    stop_reason = ""
    usage: dict[str, Any] = {}
    for event in response["stream"]:
        if "messageStop" in event:
            stop_reason = event["messageStop"].get("stopReason", "")
        if "metadata" in event:
            usage = event["metadata"].get("usage", {})
    _ok(
        "stream",
        response.get("ResponseMetadata", {}).get("RequestId", "?"),
        stop_reason,
        usage.get("inputTokens", "?"),
        usage.get("outputTokens", "?"),
    )


def scenario_regenerate(client: Any, model: str) -> None:
    # Same input sent twice, like a user hitting "regenerate": grouping must not
    # merge them into one conversation turn by mistake.
    session_id = f"regen-{RUN_ID}"
    for attempt in (1, 2):
        response = client.converse(
            modelId=model,
            messages=[_user("Give me a creative name for a coffee shop.")],
            # No temperature: some models (e.g. OpenAI) reject the field.
            inferenceConfig={"maxTokens": 64},
            requestMetadata=_metadata(
                f"regenerate_{attempt}", session_id, "frank@example.com"
            ),
        )
        _record_converse(f"regenerate_{attempt}", response, note="identical input")


def scenario_large_payload(client: Any, model: str) -> None:
    # ~150 KB prompt: big bodies are offloaded by Bedrock to separate
    # `data/<requestId>_input.json.gz` objects referenced via inputBodyS3Path.
    paragraph = (
        "Bedrock invocation logging writes one JSON record per model call. "
        "This sentence is repeated only to make the request body large. "
    )
    filler = paragraph * (150_000 // len(paragraph))
    response = client.converse(
        modelId=model,
        messages=[
            _user(
                "Here is a long document:\n\n"
                + filler
                + "\n\nIn one sentence, what is the document about?"
            )
        ],
        inferenceConfig={"maxTokens": 64},
        requestMetadata=_metadata(
            "large_payload", f"large-{RUN_ID}", "grace@example.com"
        ),
    )
    _record_converse("large_payload", response, note=f"prompt={len(filler)} chars")


def scenario_reasoning(client: Any, model: str) -> None:
    # Extended thinking: produces reasoningContent blocks in the output.
    # Newer Claude models only accept adaptive thinking; older ones only the
    # budget-based form. Try adaptive first, fall back on a validation error.
    variants = [
        (
            "adaptive",
            {"thinking": {"type": "adaptive"}, "output_config": {"effort": "low"}},
        ),
        ("enabled", {"thinking": {"type": "enabled", "budget_tokens": 1024}}),
    ]
    last_exc: Exception | None = None
    for label, fields in variants:
        try:
            response = client.converse(
                modelId=model,
                messages=[_user("Is 1001 a prime number? Explain briefly.")],
                inferenceConfig={"maxTokens": 2048},
                additionalModelRequestFields=fields,
                requestMetadata=_metadata(
                    "reasoning", f"reasoning-{RUN_ID}", "heidi@example.com"
                ),
            )
        except client.exceptions.ValidationException as exc:
            last_exc = exc
            continue
        _record_converse("reasoning", response, note=f"thinking={label}")
        return
    raise last_exc  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# Native API scenarios (provider-specific bodies)
# --------------------------------------------------------------------------- #

SYSTEM = "You are a concise assistant."
QUESTION = "Name three Nordic capitals."


def native_body(family: str) -> dict[str, Any]:
    if family == "anthropic":
        return {
            "anthropic_version": ANTHROPIC_VERSION,
            "max_tokens": MAX_TOKENS,
            "system": SYSTEM,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": QUESTION}]}
            ],
        }
    if family == "openai":
        return {
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": QUESTION},
            ],
            "max_completion_tokens": MAX_TOKENS,
        }
    if family == "nova":
        return {
            "schemaVersion": "messages-v1",
            "system": [{"text": SYSTEM}],
            "messages": [{"role": "user", "content": [{"text": QUESTION}]}],
            "inferenceConfig": {"maxTokens": MAX_TOKENS},
        }
    if family == "meta":
        return {
            "prompt": (
                "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n"
                f"{SYSTEM}<|eot_id|><|start_header_id|>user<|end_header_id|>\n\n"
                f"{QUESTION}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
            ),
            "max_gen_len": MAX_TOKENS,
        }
    if family == "mistral":
        return {
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": QUESTION},
            ],
            "max_tokens": MAX_TOKENS,
        }
    raise ValueError(f"no native body for family {family!r}")


def _invoke(
    client: Any, model: str, body: dict[str, Any]
) -> tuple[str, dict[str, Any]]:
    response = client.invoke_model(
        modelId=model,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    request_id = response.get("ResponseMetadata", {}).get("RequestId", "?")
    return request_id, json.loads(response["body"].read())


def _native_stop_and_usage(payload: dict[str, Any]) -> tuple[str, object, object]:
    usage = payload.get("usage", {})
    if "choices" in payload:  # OpenAI / Mistral chat shape
        stop = (payload.get("choices") or [{}])[0].get("finish_reason", "")
        return (
            stop,
            usage.get("prompt_tokens", "?"),
            usage.get("completion_tokens", "?"),
        )
    if "stopReason" in payload:  # Nova
        return (
            payload["stopReason"],
            usage.get("inputTokens", "?"),
            usage.get("outputTokens", "?"),
        )
    if "stop_reason" in payload and "generation" in payload:  # Meta
        return (
            payload["stop_reason"],
            payload.get("prompt_token_count", "?"),
            payload.get("generation_token_count", "?"),
        )
    return (
        payload.get("stop_reason", ""),
        usage.get("input_tokens", "?"),
        usage.get("output_tokens", "?"),
    )


def scenario_native(client: Any, model: str) -> None:
    family = model_family(model)
    request_id, payload = _invoke(client, model, native_body(family))
    stop, tin, tout = _native_stop_and_usage(payload)
    _ok("native", request_id, stop, tin, tout, note=f"InvokeModel {family} body")


def scenario_native_stream(client: Any, model: str) -> None:
    family = model_family(model)
    body = native_body(family)
    if family in {"openai", "mistral"}:
        body["stream"] = True
    response = client.invoke_model_with_response_stream(
        modelId=model,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    request_id = response.get("ResponseMetadata", {}).get("RequestId", "?")
    events = 0
    metrics: dict[str, Any] = {}
    for event in response["body"]:
        chunk = event.get("chunk")
        if not chunk:
            continue
        events += 1
        data = json.loads(chunk["bytes"])
        metrics = data.get("amazon-bedrock-invocationMetrics", metrics)
    _ok(
        "native_stream",
        request_id,
        "",
        metrics.get("inputTokenCount", "?"),
        metrics.get("outputTokenCount", "?"),
        note=f"InvokeModelWithResponseStream {family}, {events} chunks",
    )


# --------------------------------------------------------------------------- #
# Claude Code-like scenarios (Anthropic native API)
# --------------------------------------------------------------------------- #

ANTHROPIC_TOOLS = [
    {
        "name": "read_file",
        "description": "Read a file from the repository.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    }
]


def _claude_code_user_id(session_id: str) -> str:
    # Claude Code sends metadata.user_id as a JSON *string*; the colleague's
    # prototype relies on session_id/device_id/account_uuid parsed from it.
    return json.dumps(
        {
            "session_id": session_id,
            "device_id": f"device-{RUN_ID}",
            "account_uuid": "",
        }
    )


def scenario_anthropic_tools(client: Any, model: str) -> None:
    session_id = str(uuid.uuid4())
    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": [{"type": "text", "text": "What does README.md say?"}],
        }
    ]
    for step in range(1, 4):
        body = {
            "anthropic_version": ANTHROPIC_VERSION,
            "max_tokens": MAX_TOKENS,
            "tools": ANTHROPIC_TOOLS,
            "messages": messages,
            "metadata": {"user_id": _claude_code_user_id(session_id)},
        }
        request_id, payload = _invoke(client, model, body)
        stop, tin, tout = _native_stop_and_usage(payload)
        _ok(f"anthropic_tools_{step}", request_id, stop, tin, tout)
        messages.append({"role": "assistant", "content": payload.get("content", [])})
        if stop != "tool_use":
            return
        tool_results = [
            {
                "type": "tool_result",
                "tool_use_id": block["id"],
                "content": "# Demo repo\n\nThis repository is used for tests.",
            }
            for block in payload.get("content", [])
            if block.get("type") == "tool_use"
        ]
        messages.append({"role": "user", "content": tool_results})


def scenario_claude_code_session(client: Any, model: str) -> None:
    # Two turns with prompt caching (cache_control) and <system-reminder> text
    # inside the user message, like real Claude Code traffic.
    session_id = str(uuid.uuid4())
    system = [
        {
            "type": "text",
            "text": "You are Claude Code, a coding assistant. " * 300,
            "cache_control": {"type": "ephemeral"},
        }
    ]
    messages: list[dict[str, Any]] = []
    turns = [
        "<system-reminder>The user is in /repo.</system-reminder>\n"
        "How do I list files in Python?",
        "And recursively?",
    ]
    for turn, text in enumerate(turns, start=1):
        messages.append({"role": "user", "content": [{"type": "text", "text": text}]})
        body = {
            "anthropic_version": ANTHROPIC_VERSION,
            "max_tokens": MAX_TOKENS,
            "system": system,
            "messages": messages,
            "metadata": {"user_id": _claude_code_user_id(session_id)},
        }
        request_id, payload = _invoke(client, model, body)
        stop, tin, tout = _native_stop_and_usage(payload)
        usage = payload.get("usage", {})
        _ok(
            f"claude_code_session_{turn}",
            request_id,
            stop,
            tin,
            tout,
            note=(
                f"cache_write={usage.get('cache_creation_input_tokens', 0)} "
                f"cache_read={usage.get('cache_read_input_tokens', 0)}"
            ),
        )
        messages.append({"role": "assistant", "content": payload.get("content", [])})


def scenario_single_token_probe(client: Any, model: str) -> None:
    # Claude Code fires max_tokens=1 probes; they must be filtered out.
    body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": 1,
        "messages": [{"role": "user", "content": [{"type": "text", "text": "ping"}]}],
        "metadata": {"user_id": _claude_code_user_id(str(uuid.uuid4()))},
    }
    request_id, payload = _invoke(client, model, body)
    stop, tin, tout = _native_stop_and_usage(payload)
    _ok("single_token_probe", request_id, stop, tin, tout, note="max_tokens=1")


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

ALL = None  # applies to every family
NATIVE_FAMILIES = {"anthropic", "openai", "nova", "meta", "mistral"}

SCENARIOS = {
    # name: (function, families it applies to)
    "single": (scenario_single, ALL),
    "multi_turn": (scenario_multi_turn, ALL),
    "system_prompt": (scenario_system_prompt, ALL),
    "tool_use": (scenario_tool_use, ALL),
    "stream": (scenario_stream, ALL),
    "regenerate": (scenario_regenerate, ALL),
    "large_payload": (scenario_large_payload, ALL),
    "reasoning": (scenario_reasoning, {"anthropic"}),
    "native": (scenario_native, NATIVE_FAMILIES),
    "native_stream": (scenario_native_stream, NATIVE_FAMILIES),
    "anthropic_tools": (scenario_anthropic_tools, {"anthropic"}),
    "claude_code_session": (scenario_claude_code_session, {"anthropic"}),
    "single_token_probe": (scenario_single_token_probe, {"anthropic"}),
}


def applicable(family: str) -> list[str]:
    return [
        name
        for name, (_, families) in SCENARIOS.items()
        if families is ALL or family in families
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", required=True, help="Model / inference profile id")
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument(
        "--only", default="", help=f"Comma-separated subset of: {', '.join(SCENARIOS)}"
    )
    parser.add_argument(
        "--list", action="store_true", help="Print the scenarios that would run"
    )
    args = parser.parse_args()

    family = model_family(args.model)
    selected = [s.strip() for s in args.only.split(",") if s.strip()]
    unknown = [s for s in selected if s not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenarios: {', '.join(unknown)}")
    if not selected:
        selected = applicable(family)

    print(f"run_id={RUN_ID} model={args.model} family={family} region={args.region}")
    print(f"scenarios: {', '.join(selected)}")
    if args.list:
        return 0
    print(f"start={_now()}")

    client = boto3.client("bedrock-runtime", region_name=args.region)
    for name in selected:
        print(f"[{name}]")
        try:
            SCENARIOS[name][0](client, args.model)
        except Exception as exc:  # noqa: BLE001 - keep going on any failure
            _fail(name, exc)

    print(f"\nend={_now()}  (S3 delivery usually takes about a minute)\n")
    print(
        f"{'scenario':<24} {'time':<26} {'request_id':<38} "
        f"{'stop':<14} {'in/out':<12} note"
    )
    for r in results:
        print(
            f"{r['scenario']:<24} {r['time']:<26} {r['request_id']:<38} "
            f"{r['stop_reason']:<14} {r['tokens']:<12} {r['note']}"
        )
    return 0 if all(r["stop_reason"] != "ERROR" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
