# Bedrock runtime → Nebuly

Reads the **Amazon Bedrock model invocation logs** delivered to S3 and POSTs each interaction to Nebuly. Setup, shared settings, state and delivery rules are in the [main README](../../README.md).

```
S3 (hourly .json.gz files, one record per model call)
        │
        ▼
Parser (Converse, Anthropic native, OpenAI native, streams included)
        │
        ▼
Grouper (calls with the same conversation_id and interaction_id → one interaction)
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
| `interaction_id` | One user turn: a new value for each user message in the conversation, and the **same value on every model call needed to answer it**, tool calls and sub-agents included. A turn is identified together with `conversation_id`, so the same value in two conversations is two turns |
| `conversation_id` | The conversation (chat, thread, session) the turn belongs to |
| `user_id` | Who asked. Calls with the same `conversation_id` and different users are grouped separately |
| `agent_role` | Optional: `subagent` on calls made by a sub-agent, so they are never taken as the final answer |

Any other key you add becomes an interaction tag.

If the user can write while the agent is still answering, choose what the new message is: part of the same request (the agent reads it and gives one answer) keeps the same `interaction_id` and becomes one interaction; a new request gets a new `interaction_id` and becomes its own interaction, with its own answer.

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
| `--delivery-margin-seconds` | `300` | How long to wait for late S3 files before closing an interaction, and how far incremental runs re-list before the last checkpoint |
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

1. **The next turn:** a call with another `interaction_id` in the same conversation, once this turn has had no calls for `--delivery-margin-seconds` too, counted from both the start of the next turn and the last call of this one, on the files read. S3 delivers files out of order, and the agent may still work on a turn after the user sent the next message.
2. **Inactivity:** `--interaction-idle-minutes` without calls, plus the same margin. If your agent can wait longer for the user (for example for an approval), raise it.

Time is the delivery time of the files read, never the clock, so a backfill behaves like live traffic. A call that starts after the interaction was idle, or arrives after it closed, is dropped (`calls_for_closed_interaction`): what was sent is never changed. With the margin, the result depends on when the calls ran, not on when the connector runs.

Incremental runs re-list the delivery margin before the last checkpoint to find newly visible keys. The state remembers which files in that interval were processed, so the overlapping listing does not fetch their contents again. Files that appear outside the margin require a backfill, and calls for already closed turns still cannot be added.

While an interaction is open, the state keeps its calls' S3 keys and request IDs, along with an opaque turn key. If a previously read call cannot be restored, the connector holds that turn and preserves its references while continuing to process other turns. Restore the original log file or external body, then run the connector again.

Nebuly matches interactions on input, end user, and start time. Identical requests from the same user with the same logged start time can update the same interaction even when their conversation IDs differ.

### Several users in a conversation

Nebuly identifies a conversation by `conversation_id` together with the end user, so the connector groups the same way: each `user_id` of a `conversation_id` has their own turns, and only their own next turn closes one. Nothing is dropped, but it is unusual, so the run logs a warning once per conversation and counts it in `conversations_with_several_users`. The same `interaction_id` with calls of several users becomes one interaction per user, with its own warning and `interactions_with_several_users`.

## Field mapping

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | `conversation_id` |
| `interaction.input` | Text of the user message that opened the turn, in the first call not marked as sub-agent |
| `interaction.output` | Text answer of the final call: the last call not marked as sub-agent |
| `interaction.time_start` / `time_end` | Start of the first call / end of the last call |
| `interaction.end_user` | `user_id`, or its pseudonym ([End user](../../README.md#end-user)) |
| `interaction.tags` | `model` (of the answer), `models` (if more than one), `aws account`, `tools used`, and your extra `requestMetadata` keys |
| LLM traces | One per model, with the tokens of every call (sub-agents included). `input_tokens` excludes cached prompt tokens, sent as `cache_read_input_tokens` and `cache_write_input_tokens` |
| Retrieval traces | One per answered tool call of the turn (sub-agents launched through a tool included) |

Texts are cut at 8000 characters (`MAX_CONTENT_CHARS` in `user_defined.py`). An interaction with neither input nor output is skipped (`empty_interaction`), as is one where every call is marked as sub-agent (`no_main_call`).

## Run summary

Besides delivery (`delivery`, `outbox`), the JSON summary counts:

| Key | Meaning |
| --- | ------- |
| `totals.records_skipped` | Records the parser could not use: `error_record` (failed call), `missing_body`, `unsupported_format`, `invalid_body` |
| `grouping.missing_triplet` | Calls without the three ids |
| `grouping.conversations_with_several_users`, `grouping.interactions_with_several_users` | Conversations and turns with calls of several users in this run, each sent as one per user; see [Several users in a conversation](#several-users-in-a-conversation) |
| `grouping.calls_for_closed_interaction` | Calls that arrived too late, see [How interactions close](#how-interactions-close) |
| `grouping.closed_next_interaction`, `grouping.closed_idle` | Interactions closed by each rule |
| `totals.interactions_skipped` | Closed interactions not sent: `empty_interaction`, `no_main_call` |
| `totals.conversion_errors` | Closed interactions whose payload could not be built (for example an error in a `user_defined.py` hook); see [Customizing the payload](#customizing-the-payload) |
| `totals.unreadable_files` | Log files skipped because they are corrupt or not UTF-8: their calls are lost. The run logs the S3 key and exits with code 1 |
| `totals.unreadable_lines` | Log lines skipped because they are not JSON: their calls are lost. The run logs the S3 key and line and exits with code 1 |
| `totals.restore_missing_calls` | Previously read calls that could not be restored for open turns. Affected turns are held, and the run exits with code 1 |
| `totals.open_interactions` | Interactions still open, finished by a later run |

## Customizing the payload

Edit [`user_defined.py`](user_defined.py) in this folder:

| Function | Purpose |
| -------- | ------- |
| `load_context` | Load your own mappings once per run |
| `end_user_id` | The end user (default: `user_id`), before any pseudonymization |
| `model_name` | The model name sent to Nebuly (default: the normalized Bedrock id); map application inference profiles here |
| `build_tags` | Interaction tags |
| `build_segments` | `team`, `market`, `tenure`, only if you provide them |
| `trace_cost` | Cost of the calls of one model in micro-dollars, or `None` to let Nebuly compute it |
| `build_retrieval_traces` / `customize_traces` | Traces |
| `build_user_feedback` | Feedback (Bedrock logs have none) |

If a hook raises an error, that interaction is not sent: the run logs its request id and the error type (`--verbose` adds where it happened), counts it in `totals.conversion_errors` and exits with code 1. The other interactions go on as usual. Fix the hook, then backfill with `--from-date` from before the interaction. The backfill must include the original first call's S3 log to reopen the turn. Calls that arrive after it closed are dropped, as for any closed interaction.

## Known limitations

- Only S3 is read; a CloudWatch-only logging destination is not supported.
- Calls without the three ids are skipped: older traffic cannot be imported.
- `InvokeModel` without streaming has no latency in the log, so its start and end times are the same.
- Failed calls (`errorCode`) are skipped.
- A call that arrives after its interaction closed is dropped and counted in `calls_for_closed_interaction`: what was sent is not updated. This happens when the agent goes on with a turn after `--interaction-idle-minutes` without calls (for example after waiting for a human approval), or when its S3 file is delivered more than `--delivery-margin-seconds` late. If your agent can wait that long, raise `--interaction-idle-minutes`.
- An **application inference profile** is logged only as its ARN, so the model is sent as the profile id (`abc123`) and Nebuly cannot compute its cost. Map each profile id to its model in the `model_name` hook, for example from a mapping loaded in `load_context`.
- No user feedback in the logs.
