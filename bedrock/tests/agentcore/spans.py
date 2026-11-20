"""Small Strands traces as AgentCore exports them to CloudWatch Logs.

Each span is a CloudWatch event whose ``message`` is the OTEL JSON record.
Spans are written when they end, children before parents, as AWS exports them.
"""

from __future__ import annotations

import json
from collections import Counter
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

ACCOUNT = "123456789012"
REGION = "eu-north-1"
SOURCE_ID = (
    f"arn:aws:bedrock-agentcore:{REGION}:{ACCOUNT}:"
    "runtime/synthetic-agent/runtime-endpoint/DEFAULT:DEFAULT"
)
LOG_GROUP = "/aws/bedrock-agentcore/runtimes/synthetic-agent-DEFAULT"
STRANDS = "strands.telemetry.tracer"
HAIKU = "global.anthropic.claude-haiku-5-5"
T0_MS = 1_735_689_600_000
STEP_MS = 100


def _blocks(text: str) -> str:
    return json.dumps([{"text": text}])


class Trace:
    """One turn. Context managers nest spans; leaves are plain calls."""

    def __init__(
        self,
        n: int = 1,
        *,
        session: str = "session-1",
        actor: str = "actor-1",
        start_ms: int | None = None,
    ) -> None:
        self.n, self.session, self.actor = n, session, actor
        self.trace_id = f"{n:032x}"
        self.now_ms = start_ms if start_ms is not None else T0_MS + n * 60_000
        self.events: list[dict[str, Any]] = []
        self.expected: dict[str, Any] = {
            "trace_id": self.trace_id,
            "actor_id": actor,
            "session_id": session,
            "canonical_model_span_ids": [],
            "usage_by_model": {},
            "tools": [],
        }
        self._stack: list[str] = []
        self._agents = 0
        self._ids = 0

    # -- low level -----------------------------------------------------------

    def _tick(self) -> int:
        self.now_ms += STEP_MS
        return self.now_ms * 1_000_000

    def _id(self) -> str:
        self._ids += 1
        return f"{self.n:08x}{self._ids:08x}"

    def _emit(self, record: dict[str, Any], timestamp_ms: int) -> None:
        record = {
            "resource": {
                "attributes": {
                    "cloud.region": REGION,
                    "cloud.resource_id": SOURCE_ID,
                    "service.name": "synthetic-agent.DEFAULT",
                }
            },
            "traceId": self.trace_id,
            **record,
        }
        self.events.append(
            {
                "logGroupName": LOG_GROUP,
                "logStreamName": "otel-rt-logs",
                "eventId": f"t{self.n}-e{len(self.events) + 1:03d}",
                "timestamp": timestamp_ms,
                "ingestionTime": timestamp_ms + 1000,
                "message": json.dumps(record, separators=(",", ":")),
            }
        )

    @contextmanager
    def span(
        self,
        name: str,
        *,
        scope: str = STRANDS,
        attributes: dict[str, Any] | None = None,
        events: list[tuple[str, dict[str, Any]]] = (),  # type: ignore[assignment]
        status: str = "OK",
    ) -> Iterator[str]:
        """A span around whatever the body emits; yields its span id."""
        span_id, start = self._id(), self._tick()
        parent = self._stack[-1] if self._stack else f"{self.n:08x}ffffffff"
        self._stack.append(span_id)
        try:
            yield span_id
        finally:
            self._stack.pop()
        end = self._tick()
        record: dict[str, Any] = {
            "scope": {"name": scope},
            "spanId": span_id,
            "parentSpanId": parent,
            "name": name,
            "startTimeUnixNano": start,
            "endTimeUnixNano": end,
            "durationNano": end - start,
            "attributes": {"session.id": self.session, **(attributes or {})},
            "events": [
                {"timeUnixNano": end, "name": event, "attributes": attrs}
                for event, attrs in events
            ],
            "status": {"code": status},
        }
        self._emit(record, end // 1_000_000)

    # -- Strands shapes ------------------------------------------------------

    @contextmanager
    def http(self) -> Iterator[str]:
        """POST /invocations: its parent is the AgentCore service, never exported."""
        with self.span(
            "POST /invocations",
            scope="opentelemetry.instrumentation.starlette",
            status="UNSET",
        ) as span_id:
            yield span_id

    @contextmanager
    def agent(
        self,
        input: str,  # noqa: A002
        output: str | None,
        *,
        name: str = "Strands Agents",
        error: bool = False,
    ) -> Iterator[str]:
        """invoke_agent <name>; the outermost one is the turn."""
        root = self._agents == 0
        self._agents += 1
        events = [
            ("gen_ai.system.message", {"content": _blocks("system prompt")}),
            ("gen_ai.user.message", {"content": _blocks(input)}),
        ]
        if output is not None:
            events.append(("gen_ai.choice", {"message": output}))
        if error:
            events.append(("exception", {"exception.message": "synthetic"}))
        with self.span(
            f"invoke_agent {name}",
            attributes={"gen_ai.agent.name": name},
            events=events,
            status="ERROR" if error else "OK",
        ) as span_id:
            if root:
                self.expected.update(
                    root_span_id=span_id,
                    input=input,
                    output=output,
                    terminal_status="ERROR" if error else "OK",
                )
            yield span_id
        self._agents -= 1

    @contextmanager
    def cycle(self, *, error: bool = False) -> Iterator[str]:
        events = [("exception", {"exception.message": "synthetic"})] if error else []
        with self.span(
            "execute_event_loop_cycle",
            events=events,
            status="ERROR" if error else "OK",
        ) as span_id:
            yield span_id

    def memory(self, actor: str | None = None) -> None:
        """The Memory client log that names the actor: an OTEL log, not a span."""
        time = self._tick()
        message = (
            f"Retrieved 0 memories from namespace: /actors/{actor or self.actor}/facts/"
        )
        record = {
            "scope": {"name": "bedrock_agentcore.memory.client"},
            "timeUnixNano": time,
            "body": json.dumps({"message": message}),
            "spanId": self._stack[-1],
        }
        self._emit(record, time // 1_000_000)

    def chat(
        self,
        output: str = "answer",
        *,
        tokens: tuple[int, int] = (10, 5),
        cache: tuple[int, int] = (0, 0),
        model: str = HAIKU,
        uses: tuple[str, ...] = (),
    ) -> str:
        """A model call. ``tokens`` is (non-cached input, output), ``cache`` is
        (read, write); Strands reports input including the cache, with aliases.
        ``uses`` names the tools the model asks for. The botocore span below it
        is the duplicate the assembler must not count."""
        fresh, out = tokens
        read, write = cache
        total = fresh + read + write
        blocks: list[dict[str, Any]] = [
            {"toolUse": {"toolUseId": f"call-{tool}", "name": tool, "input": {}}}
            for tool in uses
        ] or [{"text": output}]
        attributes = {
            "gen_ai.request.model": model,
            "gen_ai.usage.input_tokens": total,
            "gen_ai.usage.prompt_tokens": total,
            "gen_ai.usage.output_tokens": out,
            "gen_ai.usage.completion_tokens": out,
            "gen_ai.usage.cache_read_input_tokens": read,
            "gen_ai.usage.cache_read.input_tokens": read,
            "gen_ai.usage.cache_write_input_tokens": write,
            "gen_ai.usage.cache_creation.input_tokens": write,
        }
        with (
            self.span(
                "chat",
                attributes=attributes,
                events=[("gen_ai.choice", {"message": json.dumps(blocks)})],
            ) as span_id,
            self.span(
                f"chat {model}",
                scope="opentelemetry.instrumentation.botocore.bedrock-runtime",
                attributes={
                    "gen_ai.request.model": model,
                    "gen_ai.usage.input_tokens": fresh,
                    "gen_ai.usage.output_tokens": out,
                },
                status="UNSET",
            ),
        ):
            pass
        self.expected["canonical_model_span_ids"].append(span_id)
        self.expected["usage_by_model"].setdefault(model, Counter()).update(
            input_tokens=fresh,
            output_tokens=out,
            cache_read_input_tokens=read,
            cache_write_input_tokens=write,
        )
        return span_id

    @contextmanager
    def tool_span(
        self,
        name: str,
        input: dict[str, Any],  # noqa: A002
        output: str,
        *,
        error: bool = False,
    ) -> Iterator[str]:
        """execute_tool <name>; a sub-agent runs inside the tool that calls it."""
        call_id = f"call-{name}"
        events = [
            ("gen_ai.tool.message", {"content": json.dumps(input), "id": call_id})
        ]
        if not error:
            events.append(
                ("gen_ai.choice", {"message": _blocks(output), "id": call_id})
            )
        internal = self._agents > 1
        with self.span(
            f"execute_tool {name}",
            attributes={"gen_ai.tool.call.id": call_id, "gen_ai.tool.name": name},
            events=events,
            status="ERROR" if error else "OK",
        ) as span_id:
            yield span_id
        self.expected["tools"].append(
            {
                "span_id": span_id,
                "call_id": call_id,
                "name": name,
                "input": input,
                "output": None if error else [{"text": output}],
                "internal_to_subagent": internal,
            }
        )

    def tool(self, name: str, input: dict[str, Any], output: str, **kw: Any) -> str:  # noqa: A002, ANN401
        with self.tool_span(name, input, output, **kw) as span_id:
            return span_id

    def case(self) -> dict[str, Any]:
        """The shape the tests read: source, CloudWatch events, expected result."""
        expected = {
            **self.expected,
            "usage_by_model": {
                model: dict(counts)
                for model, counts in self.expected["usage_by_model"].items()
            },
            # Expected tools in start order, as the assembler reports them.
            "tools": sorted(self.expected["tools"], key=lambda tool: tool["span_id"]),
        }
        return {
            "source": {"account_id": ACCOUNT, "region": REGION, "source_id": SOURCE_ID},
            "records": self.events,
            "expected": expected,
        }


# -- the scenarios the connector must handle ---------------------------------


def text_turn(
    n: int = 1,
    *,
    tokens: tuple[int, int] = (4, 14),
    cache: tuple[int, int] = (8994, 45),
    model: str = HAIKU,
    **identity: Any,  # noqa: ANN401
) -> dict[str, Any]:
    t = Trace(n, **identity)
    with t.http(), t.agent(f"question {n}", f"answer {n}"):
        t.memory()
        with t.cycle():
            t.chat(f"answer {n}", tokens=tokens, cache=cache, model=model)
    return t.case()


def principal_tools(n: int = 4, **identity: Any) -> dict[str, Any]:  # noqa: ANN401
    t = Trace(n, **identity)
    with t.http(), t.agent("write and run it", "done"):
        t.memory()
        with t.cycle():
            t.chat(tokens=(4, 120), cache=(9000, 300), uses=("write", "shell"))
            t.tool("write", {"path": "a.py", "content": "print(1)"}, "written")
            t.tool("shell", {"command": "python a.py"}, "1")
        with t.cycle():
            t.chat(tokens=(2, 100), cache=(9100, 134), uses=("read",))
            t.tool("read", {"path": "a.py"}, "print(1)")
        with t.cycle():
            t.chat("done", tokens=(2, 142), cache=(9186, 100))
    return t.case()


def subagent_usage(n: int = 5, **identity: Any) -> dict[str, Any]:  # noqa: ANN401
    t = Trace(n, **identity)
    with t.http(), t.agent("delegate it", "delegated answer"):
        t.memory()
        with t.cycle():
            t.chat(tokens=(4, 279), cache=(9431, 208), uses=("subagent",))
            task = {"task": "inspect", "context": "repo"}
            with (
                t.tool_span("subagent", task, "sub answer"),
                t.agent("inspect", "sub answer", name="subagent"),
            ):
                with t.cycle():
                    t.chat(tokens=(4, 162), cache=(0, 1874), uses=("shell",))
                    t.tool("shell", {"command": "ls"}, "a.py")
                with t.cycle():
                    t.chat("sub answer", tokens=(2, 89), cache=(1874, 197))
        with t.cycle():
            t.chat("delegated answer", tokens=(2, 363), cache=(9639, 374))
    return t.case()


def terminal_error(n: int = 6, **identity: Any) -> dict[str, Any]:  # noqa: ANN401
    t = Trace(n, **identity)
    with t.http(), t.agent("run it", None, error=True):
        t.memory()
        with t.cycle():
            t.chat(tokens=(4, 53), cache=(8900, 90), uses=("shell",))
            t.tool("shell", {"command": "make"}, "ok")
        with t.cycle(error=True):
            pass
    return t.case()


SCENARIOS: dict[str, Callable[..., dict[str, Any]]] = {
    "text_cache_write": lambda **kw: text_turn(
        1, tokens=(4, 22), cache=(0, 8994), **kw
    ),
    "text_cache_read": lambda **kw: text_turn(2, **kw),
    "text_second_model": lambda **kw: text_turn(
        3, tokens=(2, 15), cache=(0, 3768), model="global.openai.gpt-6.1-terra", **kw
    ),
    "principal_tools": principal_tools,
    "subagent_usage": subagent_usage,
    "terminal_error": terminal_error,
}


def load(name: str = "text_cache_read", **identity: Any) -> dict[str, Any]:  # noqa: ANN401
    """Drop-in for the JSON loader: a fresh case each call, safe to mutate."""
    return SCENARIOS[name](**identity)
