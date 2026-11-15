# Bedrock runtime → Nebuly

Reads the **Amazon Bedrock model invocation logs** delivered to S3 and POSTs each interaction to Nebuly. Setup, shared settings, state and delivery rules are in the [main README](../../README.md).

```
S3 (hourly .json.gz files, one record per model call)
        │
        ▼
Parser (Converse, Anthropic native, OpenAI native, streams included)
        │
        ▼
Grouper (calls with the same interaction_id → one interaction)
        │
        ▼
Nebuly trace_interaction
```

Bedrock logs one record per model call, with no conversation, turn or user of its own. The connector does not guess them from the content: your application sends three ids with every call, and the connector groups by them.

## Prerequisites

- **Model invocation logging** enabled in the AWS account and region, with **S3** as destination and text data delivery on. The setting is per account and region: run one connector per account and region, each with its own `--state-db`.
- Your application sends the three ids below on every call.
- AWS credentials that can read the log bucket (see [AWS access](#aws-access)).

## Send the three ids

Put them in `requestMetadata` on every model call:

| Key | Value |
| --- | ----- |
| `interaction_id` | One user turn: a new value for each user message, and the **same value on every model call needed to answer it**, tool calls and sub-agents included |
| `conversation_id` | The conversation (chat, thread, session) the turn belongs to |
| `user_id` | Who asked. A conversation has one user: calls of a conversation with another `user_id` are dropped |
| `agent_role` | Optional: `subagent` on calls made by a sub-agent, so they are never taken as the final answer |

Any other key you add becomes an interaction tag.

**Converse / ConverseStream:**

```python
response = bedrock.converse(
    modelId="global.anthropic.claude-haiku-4-5-20251001-v1:0",
    messages=messages,
    requestMetadata={
        "interaction_id": turn_id,
        "conversation_id": chat_id,
        "user_id": user_id,
    },
)
```

**InvokeModel / InvokeModelWithResponseStream:** the same keys as a JSON string (sent as the `X-Amzn-Bedrock-Request-Metadata` header):

```python
response = bedrock.invoke_model(
    modelId=model_id,
    body=json.dumps(body),
    requestMetadata=json.dumps(
        {"interaction_id": turn_id, "conversation_id": chat_id, "user_id": user_id}
    ),
)
```

A call without all three ids is skipped and counted as `missing_triplet`.

## Runtime settings

Besides the [shared settings](../../README.md#settings-shared-by-both-connectors):

| Variable | Required | Description |
| -------- | -------- | ----------- |
| `BEDROCK_S3_BUCKET` | yes | Bucket set as destination in the logging settings |
| `BEDROCK_S3_PREFIX` | no | Key prefix set in the logging settings |

Logs are read under `[<BEDROCK_S3_PREFIX>/]AWSLogs/<BEDROCK_ACCOUNT_ID>/BedrockModelInvocationLogs/<BEDROCK_REGION>/`.

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--interaction-idle-minutes` | `30` | Close an interaction after this long without calls |
| `--delivery-margin-seconds` | `300` | How long to wait for late S3 files before closing an interaction |
| `--settle-lag-seconds` | `300` | Leave files delivered in the last N seconds to the next run |
| `--yes` / `--force` | off | Confirm a run that leaves a gap in the synced range |
| `--state-db` | `.cache/runtime/state.db` | State file |

### AWS access

The identity needs:

- `s3:ListBucket` on the bucket, for the log prefix
- `s3:GetObject` on the log prefix (large request bodies are stored under `data/` in the same prefix)
- `kms:Decrypt` on the key, if the bucket uses SSE-KMS

## How interactions close

No log record says that a turn is over, so an interaction closes on the first of:

1. **The next turn:** a call with another `interaction_id` in the same conversation, once the files delivered up to its start plus `--delivery-margin-seconds` have been read. S3 delivers files out of order, so the last call of a turn can arrive after the first call of the next one.
2. **Inactivity:** `--interaction-idle-minutes` without calls, plus the same margin.

Time is the delivery time of the files read, never the clock, so a backfill behaves like live traffic. A call that starts after the interaction was idle, or arrives after it closed, is dropped (`calls_for_closed_interaction`): what was sent is never changed. With the margin, the result depends on when the calls ran, not on when the connector runs.

While an interaction is open, the state keeps only where to read its calls again on S3 (key and request id), not their content.

### Conversation owner

The conversation belongs to the user of its **oldest** call, since S3 may deliver a newer call first. While none of its interactions is closed, an older call of another user takes the conversation over and the open interactions of the other user are dropped. Once an interaction closes, the owner is final, also for later runs. Calls of another user are counted in `conversation_user_conflicts`. An `interaction_id` reused in another conversation is dropped too (`interaction_id_conflicts`).

## Field mapping

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | `conversation_id` |
| `interaction.input` | Text of the user message that opened the turn, in the first call not marked as sub-agent |
| `interaction.output` | Text answer of the final call: the last call not marked as sub-agent |
| `interaction.time_start` / `time_end` | Start of the first call / end of the last call |
| `interaction.end_user` | `user_id`, pseudonymized by default ([End user](../../README.md#end-user)) |
| `interaction.tags` | `model` (of the answer), `models` (if more than one), `aws account`, `tools used`, and your extra `requestMetadata` keys |
| LLM traces | One per model, with the tokens of every call (sub-agents included). `input_tokens` excludes cached prompt tokens, sent as `cache_read_input_tokens` and `cache_write_input_tokens` |
| Retrieval traces | One per answered tool call of the turn (sub-agents launched through a tool included) |

Texts are cut at 8000 characters. An interaction with neither input nor output is skipped (`empty_interaction`), as is one where every call is marked as sub-agent (`no_main_call`).

## Run summary

Besides delivery (`delivery`, `outbox`), the JSON summary counts:

| Key | Meaning |
| --- | ------- |
| `totals.records_skipped` | Records the parser could not use: `error_record` (failed call), `missing_body`, `unsupported_format`, `invalid_body` |
| `grouping.missing_triplet` | Calls without the three ids |
| `grouping.interaction_id_conflicts`, `grouping.conversation_user_conflicts` | Calls dropped by the [owner rules](#conversation-owner) |
| `grouping.calls_for_closed_interaction` | Calls that arrived too late, see [How interactions close](#how-interactions-close) |
| `grouping.closed_next_interaction`, `grouping.closed_idle` | Interactions closed by each rule |
| `totals.interactions_skipped` | Closed interactions not sent: `empty_interaction`, `no_main_call` |
| `totals.conversion_errors` | Closed interactions whose payload could not be built (for example an error in a `user_defined.py` hook); they are not retried |
| `totals.open_interactions` | Interactions still open, finished by a later run |

## Customizing the payload

Edit [`user_defined.py`](user_defined.py) in this folder:

| Function | Purpose |
| -------- | ------- |
| `load_context` | Load your own mappings once per run |
| `end_user_id` | The end user (default: `user_id`), before pseudonymization |
| `build_tags` | Interaction tags |
| `build_segments` | `team`, `market`, `tenure`, only if you provide them |
| `trace_cost` | Cost of the calls of one model in micro-dollars, or `None` to let Nebuly compute it |
| `build_retrieval_traces` / `customize_traces` | Traces |
| `build_user_feedback` | Feedback (Bedrock logs have none) |

## Known limitations

- Only S3 is read; a CloudWatch-only logging destination is not supported.
- Calls without the three ids are skipped: older traffic cannot be imported.
- `InvokeModel` without streaming has no latency in the log, so its start and end times are the same.
- Failed calls (`errorCode`) are skipped.
- No user feedback in the logs.
