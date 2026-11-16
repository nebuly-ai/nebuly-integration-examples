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

One agent invocation is one trace: the agent span, its model calls, its tools and its sub-agents. The connector sends a trace once its agent span has ended and every part it needs has been read. Until then the trace waits in the state, also across runs.

## Prerequisites

- An agent on **Bedrock AgentCore** with observability enabled, so its spans and logs reach CloudWatch. Tested with the Strands agent harness.
- The actor (end user) of each invocation recorded in the telemetry (see [Actor](#actor)).
- AWS credentials that can read the log groups (see [AWS access](#aws-access)).

## Actor

The end user of an invocation is its AgentCore actor id. The connector reads it from, in order:

1. **A span attribute (recommended):** `actor.id` on the agent span, set for each invocation with the actor of that request. On an AgentCore harness, add `actor.id` to the harness environment variable `OTEL_BAGGAGE_SPAN_ATTRIBUTE_KEYS` (keep the variables and keys already set: `UpdateHarness` replaces the whole map) and pass `baggage="actor.id=<actorId>"` on every `InvokeHarness` call; both are needed. With your own Strands agent code, use the agent's [`trace_attributes`](https://strandsagents.com/docs/user-guide/sdk/observability-evaluation/traces/#custom-attribute-tracking). Use `--actor-id-attribute` if you use another attribute name. Never set one fixed value for every user. With this attribute, AgentCore Memory is not needed.
2. **AgentCore Memory logs (fallback):** when the agent retrieves memories, the Memory client logs the namespace it read, for example `/actors/user-123/facts/`. The connector takes the actor from it with `--actor-namespace-template` (default `/actors/{actorId}/`; for example `/facts/{actorId}/`). It only reads these logs: it never calls the Memory APIs.

A trace with no actor, or with two different actors in the Memory logs, is not sent (`missing_actor`, `ambiguous_actor`).

**One session belongs to one actor.** The session goes to the actor of its first trace sent (or staged to be sent); a later trace of the same session with another actor is dropped (`conversation_user_conflict`). Traces ready in the same run are sent oldest first, so the actor who started the session keeps it. If none of the owner's traces is sent or still staged (all rejected, or expired before they were sent), the session goes to the next actor.

## AgentCore settings

Besides the [shared settings](../../README.md#settings-shared-by-both-connectors):

| Flag | Required | Default | Description |
| ---- | -------- | ------- | ----------- |
| `--log-group` | yes | none | CloudWatch log group to read; repeat it for each group. The runtime endpoint writes to `/aws/bedrock-agentcore/runtimes/<runtime-id>-<endpoint>`; add `aws/spans` if your spans go there |
| `--source-id` | yes | none | The runtime endpoint ARN, as in the `cloud.resource_id` resource attribute of its spans. Records of other sources are ignored. Never change it between runs: the state is bound to it |
| `--overlap-seconds` | no | `300` | Read again this much before where the last run stopped, for late events |
| `--reconciliation-seconds` | no | `86400` | How far back a reconciliation reads again |
| `--reconciliation-interval-seconds` | no | `86400` | Run a reconciliation at most this often (`0`: every run) |
| `--actor-id-attribute` | no | `actor.id` | Span attribute with the actor id |
| `--actor-namespace-template` | no | `/actors/{actorId}/` | Memory namespace with the actor id, for the fallback |
| `--state-db` | no | `.cache/agentcore/state.db` | State file |

Run one connector per runtime endpoint, each with its own `--state-db`.

### AWS access

The identity needs `logs:FilterLogEvents` on each configured log group. Nothing else is called: no Memory, no AgentCore and no model APIs.

## How reading works

- The first run reads from `--from-date`. Each later run reads from where the previous one stopped, minus `--overlap-seconds`, and stores every record before moving on. A window counts as read only when every log group was read in full.
- Once a day (by default) a **reconciliation** reads the last day again, to find events CloudWatch delivered late. It is bounded: an event later than `--reconciliation-seconds` is not found unless you backfill with `--from-date`.
- A long trace may have started before the window read. For each trace it sees, the connector reads back that trace's earlier spans with a filter on its trace id, without counting it as coverage.
- A trace that cannot be sent yet (for example a missing span, or no actor) waits in the state and is checked again on every run. If it is still not complete `--reconciliation-seconds` plus `--content-retention-seconds` after it started, it is dropped and counted as `expired_trace`: fixing the cause before then (for example `--actor-namespace-template`) still sends it.

## Field mapping

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | The AgentCore session id (`session.id`), unchanged |
| `interaction.input` | The user message of the agent span |
| `interaction.output` | The final answer of the agent span (may be empty for an invocation that ended in error) |
| `interaction.time_start` / `time_end` | Start and end of the agent span |
| `interaction.end_user` | The actor id, pseudonymized by default ([End user](../../README.md#end-user)) |
| `interaction.tags` | `agentcore.status` (`ok` or `error`), `tools used` |
| LLM traces | One per model, with the tokens of every model call (sub-agents included), counted once. `input_tokens` excludes cached prompt tokens, sent as `cache_read_input_tokens` and `cache_write_input_tokens` |
| Tool traces | One per tool call of the main agent, with its input and result. Tools a sub-agent calls stay inside the tool call that launched it |

The model is the one recorded on the model call spans, not the agent configuration.

## Customizing the payload

Edit [`user_defined.py`](user_defined.py) in this folder:

| Function | Purpose |
| -------- | ------- |
| `load_context` | Load your own mappings once per run |
| `end_user_id` | The end user (default: the actor id), before pseudonymization |
| `build_tags` | Interaction tags (`agentcore.status` is always added after it) |
| `build_segments` | `team`, `market`, `tenure`, only if you provide them |
| `customize_traces` | Traces |
| `build_user_feedback` | Feedback (the telemetry has none) |

## Known limitations

- Tested with the Strands agent harness, with any agent name. Other frameworks need the same span names and events (`invoke_agent <name>`, `chat`, `execute_tool <name>`, `gen_ai.*` events).
- Without an actor in the telemetry nothing is sent.
- Late events beyond the reconciliation window need a manual backfill.
- A trace is sent as soon as its agent span has ended and the parts it needs were read. A model call or tool span that CloudWatch delivers after that is not added: the interaction stays as sent, with fewer tokens or tools, and the run counts `late_delivery_discrepancy`.
- No user feedback in the telemetry.
