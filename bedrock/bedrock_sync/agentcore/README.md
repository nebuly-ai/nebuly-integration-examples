# Bedrock AgentCore → Nebuly

Reads the **AgentCore observability** logs on CloudWatch (OpenTelemetry spans and logs of each agent invocation) and POSTs each invocation to Nebuly as one interaction. Setup, shared settings, state and delivery rules are in the [main README](../../README.md).

```
CloudWatch log groups (spans and logs of the runtime endpoint)
        │
        ▼
State (records of every page read, coverage per log group)
        │
        ▼
Assembler (spans of one trace → one interaction: input, answer, model calls, tools)
        │
        ▼
Nebuly trace_interaction
```

One agent invocation is one trace: the agent span, its model calls, its tools and its sub-agents. The connector sends a trace as soon as its agent span has ended, with what has been read so far. Until then the trace waits in the state, also across runs.

## Prerequisites

- An agent on **Bedrock AgentCore** with observability enabled, so its spans and logs reach CloudWatch. Tested with the Strands agent harness.
- The actor (end user) of each invocation recorded in the telemetry (see [Actor](#actor)).
- AWS credentials that can read the log groups (see [AWS access](#aws-access)).

## Actor

The end user of an invocation is its AgentCore actor id. The connector reads it from, in order:

1. **A span attribute (recommended):** `actor.id` on the agent span, set for each invocation with the actor of that request. On an AgentCore harness, add `actor.id` to the harness environment variable `OTEL_BAGGAGE_SPAN_ATTRIBUTE_KEYS` (keep the variables and keys already set: `UpdateHarness` replaces the whole map) and pass `baggage="actor.id=<actorId>"` on every `InvokeHarness` call; both are needed. With your own Strands agent code, use the agent's [`trace_attributes`](https://strandsagents.com/docs/user-guide/sdk/observability-evaluation/traces/#custom-attribute-tracking). Use `--actor-id-attribute` if you use another attribute name. Never set one fixed value for every user. With this attribute, AgentCore Memory is not needed.
2. **AgentCore Memory logs (fallback):** when the agent retrieves memories, the Memory client logs the namespace it read, for example `/actors/user-123/facts/`. By default the connector recognizes the layout an AgentCore harness creates (`/actors/{actorId}/`) and the AWS defaults of the built-in strategies (`/strategy/{memoryStrategyId}/actors/{actorId}/` and, for summaries, `/strategy/{memoryStrategyId}/actor/{actorId}/`). If your strategies use another namespace, pass it with `--actor-namespace-template`, for example `/facts/{actorId}/`; repeat the flag for each layout. The values you pass replace the defaults. It only reads these logs: it never calls the Memory APIs.

A trace with no actor, or with two different actors in the Memory logs, is not sent (`missing_actor`, `ambiguous_actor`).

**Several actors in a session.** Nebuly identifies a conversation by its id together with the end user, so traces of several actors in one session are all sent and become one conversation per actor. It is unusual, so the run logs a warning once per session and counts it in `sessions_with_several_actors` (sessions seen with several actors in the same run).

## AgentCore settings

Besides the [shared settings](../../README.md#settings-shared-by-both-connectors):

| Flag | Required | Default | Description |
| ---- | -------- | ------- | ----------- |
| `--log-group` | yes | none | CloudWatch log group to read; repeat it for each group. The runtime endpoint writes to `/aws/bedrock-agentcore/runtimes/<runtime-id>-<endpoint>`; add `aws/spans` if your spans go there |
| `--source-id` | yes | none | The runtime endpoint ARN, as in the `cloud.resource_id` resource attribute of its spans. Records of other sources are ignored. Never change it between runs: the state is bound to it |
| `--overlap-seconds` | no | `300` | Read again this much before where the last run stopped, for late events |
| `--reconciliation-seconds` | no | `86400` | How far back to read before the previous reconciliation ended (`0`: disables reconciliation reads) |
| `--reconciliation-interval-seconds` | no | `86400` | Run a reconciliation at most this often (`0`: every run) |
| `--root-wait-seconds` | no | `1200` | How long an agent span whose parents are not all read yet waits before it is sent as a turn (see [How reading works](#how-reading-works)) |
| `--actor-id-attribute` | no | `actor.id` | Span attribute with the actor id |
| `--actor-namespace-template` | no | harness and AWS default layouts | Memory namespace with the actor id, for the fallback; repeat for each layout |
| `--state-db` | no | `.cache/agentcore/state.db` | State file |

Run one connector per runtime endpoint, each with its own `--state-db`.

### AWS access

The identity needs `logs:FilterLogEvents` on each configured log group. Nothing else is called: no Memory, no AgentCore and no model APIs.

## How reading works

- The first run reads from `--from-date`. Each later run reads from where the previous one stopped, minus `--overlap-seconds`. The connector skips records explicitly marked as belonging to another source or region before storing their contents. A window counts as read only when every log group was read in full.
- Once a day by default, a **reconciliation** reads from the end of the previous reconciliation minus `--reconciliation-seconds`, within the covered range. The first run establishes the initial reference point. This catches late events even when intermediate runs have already advanced the regular checkpoint. Use `--from-date` for events before that window.
- A long trace may have started before the window read. For each trace it sees, the connector reads back that trace's earlier spans with a filter on its trace id, without counting it as coverage.
- A turn is sent once its agent span has ended and its parents up to `POST /invocations` were read. A sub-agent ends, and reaches CloudWatch, before the agent that called it: a run in between sees it without its caller and would send it as a turn of its own. So an agent span whose parents are not all read waits (`waiting_for_parent` in the summary), until they arrive or for `--root-wait-seconds` (20 minutes: a synchronous AgentCore request lasts at most 15). After the wait it is sent with `missing_parent`, so an agent without `POST /invocations` is only delayed. If the caller arrives even later (an asynchronous job), the turn is sent too, with a warning (`sub_agent_sent_as_turn`): Nebuly then also shows the sub-agent as an interaction.
- While the trace content is retained, a late model or tool span can produce a corrected POST that updates the Nebuly interaction (`updated_after_delivery`). A correction waits at least a minute after the previous delivery. The trace stays available while a correction is in progress, until it either completes or expires. For a completed trace, content retention runs from its latest finalized delivery. After any POST attempt, the connector keeps the earlier payload if the input, end user or start time changes (`late_delivery_discrepancy`). If automatic delivery was given up, use the shared [recovery command](../../README.md#delivery) to retry the retained payload.

### Warnings and blocked traces

Nebuly needs a user and some content, so only these stop a trace: no actor or two actors (`missing_actor`, `ambiguous_actor`), no input and no output (`empty_interaction`), and no single agent span (`missing_root`, `ambiguous_roots`). Everything else is a warning: the trace is sent without the part that could not be read, never with invented values.

| Warning (examples) | What is sent |
| ------------------ | ------------ |
| `unsupported_content` (image, file, audio in a message) | The text only |
| `missing_usage`, `invalid_usage`, `missing_model` | The interaction without that model call's tokens |
| `incomplete_tool`, `invalid_tool_input`, `missing_tool_span` | The interaction without that tool |
| `missing_parent`, `disconnected_span`, `unfinished_descendant`, `unread_trace_history`, `conflicting_span`, `malformed_record` | What could be connected to the agent span; a late span updates it |
| `missing_session` | The trace id as `conversation_id` |
| `missing_input`, `missing_output` | That field empty |
| `missing_terminal_status` | `agentcore.status` set to `unknown` |

Each trace sent with warnings logs one line with its codes, and `degraded` in the summary counts them by code. Warnings do not change the exit code.

A blocked trace waits in the state and is checked again on every run. If it is still blocked `--reconciliation-seconds` plus `--content-retention-seconds` after it started, it is dropped and counted as `expired_trace`. The run logs the reasons (`expired_reasons` in the summary) and exits with code `1`. Fixing the cause before then (for example `--actor-namespace-template`) still sends it. Routine infrastructure traces without an agent span, Strands evidence, or actor retrieval are counted as `ignored_rootless_trace` on expiry and leave the exit code unchanged.

## Field mapping

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | The AgentCore session id (`session.id`), unchanged |
| `interaction.input` | The user message of the agent span |
| `interaction.output` | The final answer of the agent span (may be empty for an invocation that ended in error) |
| `interaction.time_start` / `time_end` | Start and end of the agent span |
| `interaction.end_user` | The actor id, or its pseudonym ([End user](../../README.md#end-user)) |
| `interaction.tags` | `agentcore.status` (`ok` or `error`), `tools used` |
| LLM traces | One per model, with the tokens of every model call (sub-agents included), counted once. `input_tokens` excludes cached prompt tokens, sent as `cache_read_input_tokens` and `cache_write_input_tokens` |
| Tool traces | One per tool call of the main agent, with its input and result. Tools a sub-agent calls stay inside the tool call that launched it |

The model is the one recorded on the model call spans, not the agent configuration. Texts (input, output, model outputs, tool inputs and results) are cut at 8000 characters (`MAX_CONTENT_CHARS` in `user_defined.py`).

## Customizing the payload

Edit [`user_defined.py`](user_defined.py) in this folder:

| Function | Purpose |
| -------- | ------- |
| `load_context` | Load your own mappings once per run |
| `end_user_id` | The end user (default: the actor id), before any pseudonymization |
| `build_tags` | Interaction tags (`agentcore.status` is always added after it) |
| `build_segments` | `team`, `market`, `tenure`, only if you provide them |
| `customize_traces` | Traces |
| `build_user_feedback` | Feedback (the telemetry has none) |

If a hook raises an error or returns data the connector cannot encode as JSON, that trace is not sent. The run logs its trace id and the error type (`--verbose` adds where it happened), counts it as `conversion_failed` in `diagnostics` and exits with code 1. The other traces go on as usual. The trace is built again on every run, so it is sent as soon as the hook is fixed, or it expires like any blocked trace.

## Known limitations

- Tested with the Strands agent harness, with any agent name. Other frameworks need the same span names and events (`invoke_agent <name>`, `chat`, `execute_tool <name>`, `gen_ai.*` events).
- Without an actor in the telemetry nothing is sent.
- Late events beyond the reconciliation window need a manual backfill.
- A sub-agent of a job running longer than `--root-wait-seconds` after it may also appear in Nebuly as an interaction of its own (`sub_agent_sent_as_turn`).
- Until a late span arrives, an interaction may have fewer tokens or tools than the agent used. Late corrections require retained content and a stable deduplication key (see [How reading works](#how-reading-works)).
- No user feedback in the telemetry.
