# OpenAI ChatGPT Compliance → Nebuly Sync

Sync tool that polls the OpenAI **ChatGPT Compliance Logs Platform**, assembles ChatGPT conversations from `CONVERSATION_MESSAGE` plus `APP_LOG` retrieval traces, assembles Codex turns from `CODEX_LOG` (with optional cost from `CODEX_TURN`), and POSTs each interaction to Nebuly's [trace interaction endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Compliance Logs (JSONL files per event type) → SQLite assembly → Nebuly ingestion
```

**Codex interactions typically reach Nebuly 2–3 hours after they happen**, by design. The sync waits for OpenAI's per-turn `CODEX_TURN` analytics records before emitting Codex interactions with cost. This integration is meant for **analytics and compliance**, not realtime monitoring. Set `CODEX_COST_WAIT_HOURS=0` to send Codex sooner without cost (tokens and classification tags from `CODEX_TURN` are omitted when you skip the wait).

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- ChatGPT **Enterprise** or **Edu** workspace with Compliance Logs enabled

## API access

All list/download calls use:

`GET https://api.chatgpt.com/v1/compliance/workspaces/{workspace_id}/logs`

`COMPLIANCE_WORKSPACE_ID` must be the workspace **UUID** from the admin console (not an `org-*` id). Organization-scoped routes only cover a subset of event types; this integration always uses the workspace path.

Recommended fine-grained scopes (or use the broad `chatgpt.enterprise.compliance_logs_platform.read`):

| Scope | Used for |
| ----- | -------- |
| `conversation_message.read` | Chat messages |
| `app_log.read` | Retrieval traces |
| `codex_log.read` | Codex event stream |
| `workspace_analytics.read` | `CODEX_TURN` cost and classification (optional if `CODEX_COST_WAIT_HOURS=0`) |

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/openai_compliance
poetry install
# For ENTRA_FETCH=true: poetry install --with entra
cp .env.example .env
# Edit .env with your keys
```

## Configuration

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `NEBULY_API_KEY` | yes | none | Nebuly secret key |
| `COMPLIANCE_API_KEY` | yes | none | Compliance API bearer token |
| `COMPLIANCE_WORKSPACE_ID` | yes | none | Workspace UUID for `/workspaces/{id}/logs` |
| `COMPLIANCE_BASE_URL` | no | `https://api.chatgpt.com/v1/compliance` | Compliance API base URL |
| `NEBULY_ENDPOINT` | no | Nebuly production ingestion URL | Nebuly ingestion endpoint |
| `COMPLIANCE_MAX_REQUESTS_PER_MINUTE` | no | `100` | Client-side throttle. OpenAI documents **100 requests per minute per workspace, per HTTP method and route** (listing is a separate bucket from download). Lower this if other jobs read the same workspace. 429 responses are retried honouring `Retry-After`. |
| `ANONYMIZE` | no | `false` | When `true`, sets `anonymize` on the Nebuly payload |
| `INTERACTION_IDLE_MINUTES` | no | `30` | Close open chat/Codex turns after this idle window |
| `CODEX_COST_WAIT_HOURS` | no | `4` | Wait for a matching `CODEX_TURN` row before emitting Codex with cost. `0` skips `CODEX_TURN` listing entirely. |
| `ENTRA_FETCH` | no | `false` | Live Microsoft Graph directory fetch at startup (`poetry install --with entra`). Mutually exclusive with `ENTRA_EXTRACT_PATH`. |
| `ENTRA_EXTRACT_PATH` | no | none | Pre-exported Entra JSON from [microsoft_entra](../microsoft_entra/README.md) |
| `AZURE_TENANT_ID` | if `ENTRA_FETCH` | none | Entra tenant id |
| `AZURE_CLIENT_ID` | if `ENTRA_FETCH` | none | App registration client id |
| `AZURE_CLIENT_SECRET` | if `ENTRA_FETCH` | none | Client secret |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | now − 30 days | ISO backfill start for the ingest `after` cursor on first run |
| `--to-date` | none | Upper bound for file **listing** (`before=` on the Compliance API) |
| `--sources` | `chats,codex` | Comma-separated: `chats` (messages + app logs), `codex` |
| `--cache-dir` | `./.cache` | SQLite state directory |
| `--dry-run` | off | Build payloads without POSTing; in-memory SQLite only |
| `--verbose` | off | Debug logging |

## Data sources

| Source | Compliance `event_type` | Processing |
| ------ | ----------------------- | ---------- |
| `chats` | `CONVERSATION_MESSAGE`, `APP_LOG` | Rows stored in SQLite; chat interactions emitted when closed |
| `codex` | `CODEX_LOG`, optionally `CODEX_TURN` | Events stored and assembled per session; cost/tags from `CODEX_TURN` when available |

Listing uses `GET …/logs?event_type=&after=&before=&limit=` (max 100) with cursor `last_end_time`. Each file is immutable JSONL downloaded via `GET …/logs/{id}` (**307** redirect to a short-lived URL; the client follows redirects and omits auth on cross-origin targets).

## Field mapping (Nebuly v3 payload)

Each POST body has `{ interaction, traces, user_feedback, anonymize }`.

### Chat (`chats`)

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | `conversation.id` |
| `interaction.input` / `output` | User message / joined assistant chain |
| `interaction.time_start` / `time_end` | User / last assistant `message.created_at` (falls back to envelope `timestamp`) |
| `interaction.end_user` | `actor.user_id` |
| `interaction.tags` | Assistants' tools/skills, conversation metadata, Entra segments |
| `traces` | `APP_LOG` request/response pairs in the interaction window + `LLMTrace` (no token fields on chat) |
| `user_feedback` | Inferred `regenerate` / `edit` slugs from the message tree (see limitations) |

Interaction key: user `message.id`. Closed when a later user message exists in the thread or when the last assistant message is older than `INTERACTION_IDLE_MINUTES`. Duplicate `message.id` rows update the stored snapshot when the envelope timestamp is newer.

### Codex (`codex`)

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | `event_details.session_id` |
| `interaction.input` / `output` | `PROMPT_SENT` prompt / joined non-error `PROMPT_RESPONSE_RECEIVED` texts |
| `interaction.time_start` / `time_end` | Prompt timestamp / last response or tool event |
| `interaction.end_user` | `actor.user_id` |
| `interaction.cost` | `CODEX_TURN` `cost_usd` + `estimated_cost_usd` (micro-dollars) when a turn record is present |
| `traces` | `LLMTrace` plus tool traces from `TOOL_CALL_COMPLETED` / `TOOL_CALL_FAILED` |
| `user_feedback` | Customer hooks via `build_codex_user_feedback` (no platform thumbs on Codex rows) |

Interaction key: `PROMPT_SENT.event_id`. Rows are ingested from `PROMPT_SENT`, `PROMPT_RESPONSE_RECEIVED`, and tool call events; legacy single-row `PROMPT_RESPONSE_RECEIVED` blobs with both prompt and response are still accepted.

## Customizing the payload

Edit `openai_compliance_sync/user_defined.py`:

| Function | Purpose |
| -------- | ------- |
| `build_chat_tags` / `build_chat_traces` / `build_chat_user_feedback` | Chat interactions |
| `build_codex_tags` / `build_codex_traces` / `build_codex_user_feedback` | Codex turns |

Directory `team` / `market` / `tenure` come from Entra via `actor.user_email`, matching the Copilot integrations.

## Caching & idempotency

SQLite at `<cache-dir>/compliance_store.db` (WAL):

| Table | Role |
| ----- | ---- |
| `ingest_cursor` | Per `event_type` ingest watermark |
| `processed_file` | Completed log file ids |
| `chat_message` / `app_call` | Raw rows for chat assembly |
| `codex_event` / `codex_turn_record` | Codex assembly and cost join |
| `emitted_interaction` | `sent` / `rejected` per interaction key |

- **Reset:** delete the cache directory after schema changes or when reprocessing history.
- **Permanent Nebuly 4xx** (except 408/429): recorded as `rejected`, run continues.
- **Transient Compliance errors:** cursor not advanced for that source.
- **Retention:** stored rows older than 35 days are pruned after each run.
- **Cursor lag:** warning logged when ingest lags more than 7 days (30-day platform retention).

## Known limitations

### Cost and timing

- **Chat cost** is not mapped (`COSTS` is hourly per user on the org route, not per interaction).
- **Codex cost** comes from OpenAI's `CODEX_TURN` (`cost_usd` plus `estimated_cost_usd`; the credit portion is OpenAI's estimate, not an invoice line item). Turns emitted without a turn record have no `cost` (Nebuly may estimate cost on its own).
- **Codex latency:** see the note at the top of this README.

### Feedback

- Explicit thumbs ratings exist in `AUDIT_LOG` only and are **not** mapped to Nebuly.
- **`regenerate`** and **`edit`** are inferred from the message tree (`regenerate` when multiple assistant siblings exist; `edit` when a user message has an earlier user sibling with the same parent). Regenerations that happen **after** an interaction was already sent are not reflected retroactively.

### Data coverage

- `APP_LOG` has no call id; each response pairs with the latest unmatched request in the same conversation and app (a failed call leaves an earlier request unpaired with empty retrieval outputs).
- Codex **tool outputs** are not present in compliance logs; tool traces carry inputs only.
- Codex **`APP_MCP_*`** tool calls are not mapped (no session link in logs).
- Codex turns with **no `PROMPT_SENT`** event in the stream cannot be assembled and are skipped.
- Codex turns with errors or empty prompt/output after assembly are skipped.
- Hidden system prompts and reasoning content are not exported.
- Workspace agents (`CUSTOM_AGENTS_LOG`) are not mapped.
- `APP_LOG` calls that arrive after a chat interaction closes are not attached to that interaction.
- Enterprise and Edu workspaces only.
- Compliance retention is about 30 days; run at least daily (hourly recommended).
- Team, market, and tenure tags require the Entra join.

## Running

```bash
poetry run python -m openai_compliance_sync
poetry run python -m openai_compliance_sync --sources chats
poetry run python -m openai_compliance_sync --dry-run --verbose
```

## Tests

```bash
poetry run pytest -q
```
