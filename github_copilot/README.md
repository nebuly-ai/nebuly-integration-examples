# GitHub Copilot usage records → Nebuly Sync

Sync tool that reads GitHub Copilot **enterprise usage records**, pairs each request with its response, rebuilds conversations from the message prefix, and POSTs each closed turn to Nebuly's [trace interaction endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Usage records (REST | S3 | Azure Blob) → SQLite (dedupe event_id)
  → pair request + response → conversation + turn → user resolution → Nebuly
```

This integration is for **Enterprise Managed Users (EMU)** enterprises on the Copilot usage-records public preview. It needs an enterprise-owner token with the `read:enterprise` scope. IDE and CLI traffic from VS Code, Visual Studio, JetBrains, Eclipse, the Copilot CLI, and the cloud agent can appear in these records. github.com chat, Mobile, Xcode, Slack, and Teams do not.

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- GitHub Enterprise Cloud with EMU, Copilot usage records enabled (public preview)
- Enterprise owner personal access token, `read:enterprise`
- For history older than 48 hours: audit-log streaming into S3 or Azure Blob (the REST API only returns the last 48 hours)

## API access

REST listing:

`GET https://api.github.com/enterprises/{enterprise}/copilot/usage-records`

Headers: `Accept: application/vnd.github+json`, `Authorization: Bearer <token>`, `X-GitHub-Api-Version: 2026-03-10` (`GITHUB_API_VERSION`).

Query: `per_page=25` (the API maximum), `order=asc`, `phrase=created:>=<iso>`. Pages follow `Link` `rel="next"`. `after` / `before` cursors from that header are followed as returned.

Documented record fields this sync reads: `type` (`request` or `response`), `user_id`, `enterprise_id`, `endpoint`, `body` (a JSON-encoded string), `@timestamp` (epoch milliseconds), `truncated` (GitHub cuts a body at 1 MB), `event_id`, `github_request_id`.

User resolution, when `RESOLVE_USERS=true`:

| Call | Used for |
| ---- | -------- |
| `GET /user/{account_id}` | `user_id` → login, cached in SQLite |
| `POST /graphql` `enterprise.ownerInfo.samlIdentityProvider.externalIdentities` | login → SCIM or SAML email, loaded once per run |

`RESOLVE_USERS=false` skips both calls and uses `github-user-<id>` as `end_user`.

## Assumptions

GitHub's preview docs do not specify how to join a request to its response, the body schema, or the streaming object layout. This sync assumes the following, and drops records that do not match:

- A request and its response share `github_request_id`. Records are processed in source order, so the request has to be seen before the response.
- `body` is an OpenAI Chat Completions request or response. The model name is read from that JSON. Streamed responses may be stored as SSE text (`data:` lines) and are reconstructed into one completion, including `tool_calls`.
- S3 and Blob objects are gzip or plain NDJSON, one JSON object per line. Lines that are not usage records (audit events in the same stream, missing `github_request_id` / `body` / `type` / `event_id` / `@timestamp`) are skipped and counted as `non_usage`.
- Object keys sort in the order they should be applied. The cursor is the last key, via S3 `StartAfter` or a blob-name comparison. Date-partitioned keys work when lexicographic order matches time order.

Record shape can change while the API is in preview. Unparseable bodies are counted and skipped.

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/github_copilot
poetry install
# For ENTRA_FETCH=true: poetry install --with entra
cp .env.example .env
# Edit .env with your keys
```

## Configuration

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `GITHUB_TOKEN` | yes | none | Enterprise owner token, `read:enterprise` |
| `GITHUB_ENTERPRISE` | yes | none | Enterprise slug |
| `NEBULY_API_KEY` | yes* | none | Nebuly secret key (*not required when `CSV_EXPORT=true`) |
| `GITHUB_API_URL` | no | `https://api.github.com` | GitHub API base URL |
| `GITHUB_API_VERSION` | no | `2026-03-10` | `X-GitHub-Api-Version` |
| `NEBULY_ENDPOINT` | no | Nebuly production ingestion URL | Nebuly ingestion endpoint |
| `REST_OVERLAP_SECONDS` | no | `60` | REST cursor is the newest `@timestamp` minus this overlap, so a record split across runs is still listed |
| `PAIR_WAIT_MINUTES` | no | `15` | Drop a request that still has no response this long before the newest record timestamp in the run |
| `CONVERSATION_GAP_HOURS` | no | `8` | Start a new conversation id when the same user and opening message are idle longer than this |
| `INTERACTION_IDLE_MINUTES` | no | `30` | Close an open turn whose last response is this far behind the newest record timestamp seen so far |
| `RESOLVE_USERS` | no | `true` | Resolve `user_id` to login and email. `false` sends `github-user-<id>` |
| `ANONYMIZE` | no | `false` | Sets `anonymize` on the Nebuly payload |
| `CSV_EXPORT` | no | `false` | Write interactions to a CSV file instead of POSTing to Nebuly |
| `CSV_EXPORT_PATH` | no | `./interactions.csv` | CSV output file when `CSV_EXPORT=true` |
| `S3_BUCKET` | if `s3` | none | Bucket that receives the usage-record stream |
| `S3_PREFIX` | no | empty | Key prefix passed to `list_objects_v2` |
| `S3_ENDPOINT_URL` | no | none | Custom S3 endpoint. Credentials use the standard boto3 chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` or the instance role) |
| `AZURE_STORAGE_CONNECTION_STRING` | if `azure_blob` and no account URL | none | Azure connection string |
| `AZURE_BLOB_ACCOUNT_URL` | if `azure_blob` and no connection string | none | Account URL. `AZURE_BLOB_SAS_TOKEN` is appended when set |
| `AZURE_BLOB_CONTAINER` | if `azure_blob` | none | Container name |
| `AZURE_BLOB_PREFIX` | no | empty | `name_starts_with` on `list_blobs` |
| `AZURE_BLOB_SAS_TOKEN` | no | none | SAS token, with or without a leading `?` |
| `ENTRA_FETCH` | no | `false` | Live Microsoft Graph directory fetch at startup (`poetry install --with entra`). Mutually exclusive with `ENTRA_EXTRACT_PATH` |
| `ENTRA_EXTRACT_PATH` | no | none | Pre-exported Entra JSON from [microsoft_entra](../microsoft_entra/README.md) |
| `AZURE_TENANT_ID` | if `ENTRA_FETCH` | none | Entra tenant id |
| `AZURE_CLIENT_ID` | if `ENTRA_FETCH` | none | App registration client id |
| `AZURE_CLIENT_SECRET` | if `ENTRA_FETCH` | none | Client secret |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | REST: now minus 47 hours. S3/Blob: no lower bound | ISO lower bound. On REST this is the first `phrase=created:>=` when the source has no stored cursor |
| `--to-date` | none | Exclusive upper bound on `@timestamp`. The source stops at the first record at or after this instant |
| `--sources` | `rest,s3,azure_blob` | Comma-separated: `rest`, `s3`, `azure_blob`. Pass `--sources rest` unless the S3 or Blob settings are filled in; an enabled source with an empty bucket or container fails the process |
| `--cache-dir` | `./.cache` | SQLite state directory |
| `--dry-run` | off | Build payloads without POSTing, without writing the CSV, and without touching the on-disk cache. State is an in-memory SQLite database for that process |
| `--verbose` | off | Debug logging, including HTTP traces |

## Data sources

| Source | What it reads | Cursor |
| ------ | ------------- | ------ |
| `rest` | `GET /enterprises/{enterprise}/copilot/usage-records` | Newest `@timestamp` minus `REST_OVERLAP_SECONDS`, stored as epoch ms |
| `s3` | `list_objects_v2` on `S3_BUCKET` / `S3_PREFIX`, then `get_object` | Last object key (`StartAfter`) |
| `azure_blob` | `list_blobs` on `AZURE_BLOB_CONTAINER` / `AZURE_BLOB_PREFIX`, then download | Last blob name (names at or before the cursor are skipped) |

REST logs a warning when the cursor is older than 48 hours: that window is all the REST API retains, and older history has to come from S3 or Blob. 429 responses and secondary rate limits (`403` with `X-RateLimit-Remaining: 0`) are retried using `Retry-After` when GitHub sends it, otherwise after 60 seconds, up to 10 attempts.

S3 and Blob objects are decoded as gzip, then as plain bytes. A corrupt gzip or a non-UTF-8 object is skipped (`unreadable_files`). A line that is not JSON is skipped (`unreadable_lines`). The rest of the object is kept.

## How a turn is built

1. **Dedupe.** `event_id` is the primary key. A repeated record is counted and not processed again.
2. **Pair.** A `request` is stored until a `response` with the same `github_request_id` arrives. A response with no stored request is ignored. At the end of a source, requests older than `PAIR_WAIT_MINUTES` before the newest timestamp in that source are dropped and counted as `unpaired`.
3. **Keep chat calls.** The `endpoint` must contain `/chat/completions` or `chat_completion`. Completions, NES, and embeddings are counted as non-chat and skipped. Bodies that do not parse as chat completions are counted and skipped.
4. **Drop helper calls.** `user_defined.is_background_call` and the default heuristic both run. The default drops a call with no `tools`, exactly one user message, and a system or developer message that starts with a prefix in `HELPER_SYSTEM_PREFIXES` (title generation). Extend that tuple or the hook for other helper prompts.
5. **Turn.** The turn key is a hash of `user_id` and the messages up through the latest user message that is not a tool result. Agent tool-loop calls resend that prefix, so they join the same turn.
6. **Conversation.** The conversation id is a hash of `user_id` and the first user message. It is stored in SQLite. The same opening message more than `CONVERSATION_GAP_HOURS` after the conversation's last activity gets a new id.
7. **Close.** A turn closes when a later turn starts in the same conversation, when its last response is older than `INTERACTION_IDLE_MINUTES` behind the newest record timestamp read so far (the data watermark, so a backfill does not use the wall clock), or at the end of a source that finishes without error. Open turns are held in memory for that source.

## Field mapping (Nebuly payload)

Each POST body is `{ interaction, traces, user_feedback, anonymize }`.

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.input` | Text of the turn-start user message, capped at 8000 characters |
| `interaction.output` | Final assistant text of the last call in the turn that has no `tool_calls`. Same cap |
| `interaction.time_start` / `time_end` | First request `@timestamp` / last response `@timestamp` |
| `interaction.conversation_id` | Rebuilt conversation id |
| `interaction.end_user` | Email, otherwise login, otherwise `github-user-<id>` |
| `interaction.tags` | `endpoint`, `model` (and `models` when a turn used more than one), `tools used`, `truncated` when GitHub cut any body, plus Entra directory tags |
| `interaction.team` / `market` / `tenure` | Entra department, country or usage location, and hire date, when a directory is loaded |
| `traces` | One LLM trace per model. Tokens are summed from response bodies that include `usage` (`prompt_tokens`, `completion_tokens`, `prompt_tokens_details.cached_tokens`). One retrieval trace per tool call paired with its `role: tool` result in the last call's messages |
| `user_feedback` | `[]` |

Turns with empty input or empty output are skipped.

The LLM trace does not send `cost`. Usage records carry no spend. Nebuly can estimate a cost from the model and the token counts; that figure is Nebuly's estimate and does not reflect GitHub's billed amount (AI credits, discounts). Per-interaction billing reconciliation and the OTel `nano_aiu` signal are not implemented. A response body with no `usage` object contributes no tokens, so the trace can show zeros.

## CSV export

Set `CSV_EXPORT=true` to write each interaction to `CSV_EXPORT_PATH` instead of sending it to Nebuly. One column per Nebuly interaction field (`conversation_id`, `end_user`, `team`, `market`, `tenure`, `time_start`, `time_end`, `input`, `output`, `hide_content`) plus `tags`, `traces`, and `user_feedback` as JSON strings, and `anonymize`. `team`, `market`, and `tenure` are filled when Entra is enabled.

- Rows are appended. The header is written only when the file is new or empty. Appending to a CSV from an older version with a different header mixes headers, so start a new file.
- CSV runs keep their own state (the cache key includes the CSV path), so exporting to CSV never marks a turn as already sent to Nebuly, and a new `CSV_EXPORT_PATH` is exported from scratch. Usage records are read again if both modes cover the same range; `event_id` dedupe still applies inside one cache.
- Re-sends after a partial failure can produce duplicate rows in the CSV.
- `--dry-run` still takes precedence: nothing is written to the CSV, and the on-disk cache is left unchanged.

```bash
CSV_EXPORT=true poetry run python -m github_copilot_sync --sources s3 --from-date 2026-11-01
```

## Customizing the payload

Edit `github_copilot_sync/user_defined.py`:

| Function | Purpose |
| -------- | ------- |
| `is_background_call` | Return true to drop a paired call before turn grouping (title generation, intent detection, and similar) |
| `HELPER_SYSTEM_PREFIXES` | System-prompt prefixes the default heuristic already treats as background |

The default heuristic also runs: no `tools`, a single user message, and a system text that starts with one of those prefixes.

## Caching and idempotency

SQLite at `<cache-dir>/<cache-key>/copilot_store.db`. The cache key is a short hash of the GitHub API URL, the enterprise slug, the selected sources, and the destination: the Nebuly endpoint and API key, or the CSV path when `CSV_EXPORT=true`. Changing any of those starts from an empty store. Rotating `GITHUB_TOKEN` keeps the store.

| Table | Role |
| ----- | ---- |
| `usage_record` | Raw records, primary key `event_id` |
| `pairing_pending` | Requests waiting for a response |
| `conversation_session` | Conversation id reuse and the gap split |
| `source_cursor` | Per-source watermark (REST timestamp or object key) |
| `user_login` | Cached `user_id` → login |
| `emitted_turn` | `sent` or `rejected` per turn key |

- **Reset:** delete the cache directory to reprocess history. Already-sent turns are not posted again while the row exists.
- **Permanent Nebuly 4xx** (except 408/429): recorded as `rejected`, and the run continues.
- **Transient GitHub or transport errors:** the source cursor is not advanced and the run stops. Records already inserted stay inserted. Open turns that had not closed yet are only in memory, so they are not rebuilt on the next run; give a backfill its own `--cache-dir` if you need to replay it.
- A finished source closes every turn still open, then advances the cursor. A second run sends nothing for turn keys already marked `sent`.

## Known limitations

### Cost and tokens

- Usage records have no spend field. The LLM trace omits `cost`. Any cost shown in Nebuly is estimated there from model and tokens, and it is not GitHub's invoice (AI credits and discounts are absent). Billing reconciliation and OTel `nano_aiu` are not implemented.
- Token counts exist only when the response body includes `usage`.

### Conversations

- There is no conversation id on the record. Ids are rebuilt from the user and the first user message, plus `CONVERSATION_GAP_HOURS`. Two parallel conversations from the same user that open with the same text share an id until the gap splits them.
- Helper calls that miss `is_background_call` and the prefix list are ingested as normal turns.
- Input and output sent to Nebuly are capped at 8000 characters. GitHub's own 1 MB body cap is separate and sets the `truncated` tag.
- Retrieval traces are taken from tool calls and `role: tool` results on the last call of the turn.

### Coverage

- REST retains about 48 hours, 25 records per page. Use S3 or Azure Blob streaming for anything older. A REST cursor behind that window logs a warning and still queries; GitHub will not return the missing days.
- Bodies over 1 MB are truncated by GitHub and tagged `truncated`.
- github.com chat, Mobile, Xcode, Slack, and Teams never appear in usage records.
- No thumbs or other feedback is present. `user_feedback` is always empty.
- Event Hubs is not a source. Streaming is S3 or Azure Blob.
- Team, market, and tenure need the Entra join, and the email has to match `mail` or `userPrincipalName`.
- Login → email needs an enterprise-owner token and SAML or SCIM external identities. Without them, `end_user` falls back to the login, then to `github-user-<id>`.
- The record format is taken from GitHub's preview docs and can change.

## Running

```bash
poetry run python -m github_copilot_sync --sources rest
poetry run python -m github_copilot_sync --sources rest --from-date 2026-11-23 --dry-run --verbose
poetry run python -m github_copilot_sync --sources s3 --from-date 2026-11-01
poetry run python -m github_copilot_sync --sources azure_blob --from-date 2026-11-01
```

Each finished source logs `ingested`, `deduped`, `sent`, `rejected`, `skipped`, `non_usage`, and `unpaired`.

## Tests

```bash
poetry run pytest -q
```
