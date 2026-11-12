# AWS Bedrock → Nebuly Sync

Sync tool that reads the **Amazon Bedrock model invocation logs** delivered to S3, rebuilds conversations and user turns (including tool-use round trips), and POSTs each interaction to Nebuly's [trace interaction endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
S3 (hourly .json.gz files, one record per model call)
        │
        ▼
Parser (Converse, Anthropic native, OpenAI native, streams included)
        │
        ▼
Grouper (conversation id + tool round trips → one turn)
        │
        ▼
Nebuly trace_interaction
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- Bedrock **model invocation logging** enabled in the AWS account and region, with **S3** as destination and text data delivery on
- AWS credentials that can read the log bucket (see [AWS access](#aws-access))

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/bedrock
poetry install
cp .env.example .env
# Edit .env with your keys
```

## Configuration

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `NEBULY_API_KEY` | yes | none | Nebuly secret key |
| `BEDROCK_S3_BUCKET` | yes | none | Bucket set as destination in the Bedrock logging settings |
| `BEDROCK_ACCOUNT_ID` | yes | none | AWS account id whose logs are read |
| `BEDROCK_REGION` | yes | none | Region whose logs are read (e.g. `eu-central-1`) |
| `USER_HASH_SECRET` | when pseudonymizing | none | Secret key for deterministic `end_user` pseudonyms, see [End user](#end-user) |
| `BEDROCK_S3_PREFIX` | no | none | Key prefix set in the Bedrock logging settings |
| `AWS_PROFILE` / `AWS_REGION` | no | none | Standard boto3 credentials, used when `AWS_ROLE_ARN` is unset |
| `AWS_ROLE_ARN` | no | none | Read-only role to assume before reading S3 |
| `AWS_EXTERNAL_ID` | no | none | External id for `AWS_ROLE_ARN` |
| `NEBULY_ENDPOINT` | no | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint |
| `BEDROCK_SETTLE_LAG_SECONDS` | no | `300` | Files delivered in the last N seconds are left to the next run |
| `CONVERSATION_CHAIN_TTL_DAYS` | no | `30` | Days a conversation without a session id can still be continued |
| `ANONYMIZE` | no | `false` | Anonymize content in the Nebuly payload |
| `SEND_PLAIN_END_USER` | no | `false` | Send the raw user id as `end_user` instead of a pseudonym |

The logs are read under `[<BEDROCK_S3_PREFIX>/]AWSLogs/<BEDROCK_ACCOUNT_ID>/BedrockModelInvocationLogs/<BEDROCK_REGION>/`. Run one sync per account and region, each with its own `--cache-dir`.

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | none | ISO backfill start (required on first run) |
| `--to-date` | now − settle lag | ISO end bound |
| `--cache-dir` | `./.cache` | SQLite state directory |
| `--yes` / `--force` | off | Confirm gap-creating runs without prompting |
| `--dry-run` | off | Build payloads without POSTing; works on an in-memory copy of the state |
| `--verbose` | off | Debug logging |

### AWS access

The script uses the standard boto3 credential chain (`AWS_PROFILE`, environment keys, SSO, instance or task role). Set `AWS_ROLE_ARN` (and `AWS_EXTERNAL_ID` if the role requires it) to assume a dedicated role first; its credentials are renewed during long backfills.

The identity needs:

- `s3:ListBucket` on the bucket, for the log prefix
- `s3:GetObject` on the log prefix
- `kms:Decrypt` on the key, if the bucket uses SSE-KMS
- `sts:AssumeRole` on `AWS_ROLE_ARN`, if set

### End user

`end_user` is taken from the first call of the turn, in this order:

1. a user id in `requestMetadata` (`user_id`, `userId`, `end_user`, `endUser`, `user`);
2. the user name of an IAM Identity Center (SSO) session;
3. the Claude Code account, then the Claude Code device;
4. the caller ARN (often an application role shared by all users);
5. `unknown`.

By default the value is replaced by a deterministic UUID pseudonym via keyed HMAC-SHA256 (`USER_HASH_SECRET`), as in the Gemini Enterprise integration. The same id always maps to the same UUID; to recover real identities, re-hash your user list with the same secret. Set `SEND_PLAIN_END_USER=true` to send the raw value instead (no secret needed).

```bash
python -c "import secrets; print(secrets.token_hex(32))"
# or: openssl rand -hex 32
```

## Conversations and sessions

Bedrock's `Converse` and `InvokeModel` APIs are stateless: the log has no conversation id. The conversation id is chosen in this order:

1. a session id in `requestMetadata` (`session_id`, `sessionId`, `conversation_id`, `conversationId`);
2. the `session_id` Claude Code sends in `metadata.user_id` of each request;
3. a hash chain: each call re-sends the previous messages plus the previous answer, so a call whose history matches an earlier call joins its conversation (within `CONVERSATION_CHAIN_TTL_DAYS`);
4. otherwise a new conversation `bedrock-<request id>`.

The hash chain is a fallback: two users of the same application role asking the same thing can be merged, and an edited history starts a new conversation. For reliable conversations and users, set `requestMetadata` in your calls:

```python
bedrock.converse(
    modelId="...",
    messages=[...],
    requestMetadata={"session_id": "<your conversation id>", "user_id": "<your user id>"},
)
```

For `InvokeModel`, send the same keys as JSON in the `X-Amzn-Bedrock-Request-Metadata` header. Other `requestMetadata` keys become interaction tags.

## Data sources

| API | Request / response format | Notes |
| --- | ------------------------- | ----- |
| `Converse` / `ConverseStream` | Converse | All models, Nova on `InvokeModel` too |
| `InvokeModel` / `InvokeModelWithResponseStream` | Anthropic Messages | Streams are rebuilt from the logged events |
| `InvokeModel` / `InvokeModelWithResponseStream` | OpenAI Chat Completions | OpenAI models such as `openai.gpt-oss-120b-1:0` |

Bodies too large to inline are read from the `data/` folder Bedrock writes next to the log. Error records, other native formats, and service calls of agents (Claude Code probes with `max_tokens <= 1`, session titles, web page summaries) are skipped and counted in the final summary.

A **turn** is the user's question plus every tool-use round trip until the final answer. Round trips are linked by tool ids, so calls of the same session running in parallel do not mix. A turn waiting for a tool result for more than 2 hours is sent as it is.

## Field mapping (Nebuly v3 payload)

Each POST body has `{ interaction, traces, user_feedback, anonymize }`.

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | See [Conversations and sessions](#conversations-and-sessions) |
| `interaction.input` | Text of the user message that opened the turn, without injected context (Claude Code `<system-reminder>` blocks) |
| `interaction.output` | Last text answer of the turn |
| `interaction.time_start` / `time_end` | Start of the first call / end of the latest call (`timestamp` + latency) |
| `interaction.end_user` | See [End user](#end-user) |
| `interaction.tags` | `model`, `models` (if more than one), `aws account`, `tools used`, `claude code session-id`, other `requestMetadata` keys |
| `traces` | One `LLMTrace` per model with the tokens of all its calls in the turn (cache tokens included in input), plus one retrieval trace per tool call with its result |

Model ids are normalized to the Nebuly model name: `anthropic.claude-haiku-4-5-20251001-v1:0` and `eu.anthropic.claude-haiku-4-5-20251001-v1:0` both become `claude-haiku-4-5`. Content strings are truncated to 8000 characters.

## Customizing the payload

Edit `bedrock_sync/user_defined.py`:

| Function / constant | Purpose |
| ------------------- | ------- |
| `end_user_id` | Who asked, before pseudonymization |
| `build_tags` / `build_traces` / `build_user_feedback` | Per turn |
| `trace_cost` | Cost in micro-dollars; return `None` to let Nebuly compute it from model and tokens |
| `INJECTED_CONTEXT_PATTERNS` | Regexes stripped from the user and assistant text |
| `SERVICE_CALL_PATTERNS` | Side calls of an agent that are not user conversations |

## Caching & idempotency

Each run reads the S3 files delivered since the last run, up to now minus `BEDROCK_SETTLE_LAG_SECONDS`, and saves a checkpoint at every hour of files and at the end. SQLite at `<cache-dir>/sync_state.db` (WAL):

| Table | Role |
| ----- | ---- |
| `ingest_cursor` | Covered range `[coverage_from, coverage_until]` and the last S3 key read |
| `pending_call` / `open_turn` | Calls and turns not sent yet; only their S3 key and request id, the content is read again from S3 |
| `conversation_tip` | Hash chain digests that link a call to its conversation |
| `seen_request` / `answered_tool` | Duplicate request ids and tool results already seen |
| `emitted_interaction` | `sent` / `rejected` per interaction key (request id of the first call of the turn) |

- **Resume:** without `--from-date`, the run starts from the last checkpoint.
- **Backfill:** a `--from-date` before the covered range reads again from that date; interactions already sent are skipped.
- **Gap:** a range that does not touch the covered range prompts for confirmation and resets the coverage; use `--yes` in non-interactive environments.
- **Permanent Nebuly 4xx** (except 408/429): recorded as `rejected`, the run continues.
- **Transient errors:** a POST that still fails after its retries stops the run without a checkpoint; the next run starts again from the last one.
- **Reset:** delete the cache directory.

Do not start a run while the previous one on the same cache directory is still running.

## Known limitations

- Only the S3 destination is read; CloudWatch Logs is not supported.
- Without a session id in `requestMetadata`, conversations rely on the hash chain (see above).
- Text only: images and documents in the input are not sent.
- `InvokeModel` without streaming logs no latency, so `time_end` equals `time_start`.
- Application inference profiles log only their own ARN: the model name is the profile id.
- Claude Code sub-agent calls are sent as separate interactions of the same session.
- Cost is computed by Nebuly from model and tokens; override `trace_cost` to apply your Bedrock prices or prompt caching discounts.
- No user feedback in Bedrock logs.
- If a POST reaches Nebuly but its response times out, the retry can send the interaction twice.

## Running

```bash
poetry run python -m bedrock_sync --from-date 2026-01-01T00:00:00Z
poetry run python -m bedrock_sync
poetry run python -m bedrock_sync --dry-run --verbose --from-date 2026-07-02
```

Run it on a schedule (cron, ECS scheduled task, ...), hourly or more often, on storage that keeps the cache directory between runs.

## Tests

```bash
poetry install --with dev
poetry run pytest -q
```
