# OpenAI ChatGPT Compliance → Nebuly Sync

Sync tool that polls the OpenAI **ChatGPT Compliance Logs Platform**, assembles ChatGPT conversations from `CONVERSATION_MESSAGE` plus `APP_LOG` retrieval traces, ingests Codex turns from `CODEX_LOG`, and POSTs each interaction to Nebuly's [trace interaction endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Compliance Logs (JSONL files per event type) → SQLite assembly → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- ChatGPT **Enterprise** or **Edu** workspace with Compliance Logs enabled

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
| `COMPLIANCE_PRINCIPAL_ID` | yes | none | Workspace or organization principal id (`org-*` uses the `organizations` scope) |
| `COMPLIANCE_BASE_URL` | no | `https://api.chatgpt.com/v1/compliance` | Compliance API base URL |
| `NEBULY_ENDPOINT` | no | Nebuly production ingestion URL | Nebuly ingestion endpoint |
| `COMPLIANCE_MAX_REQUESTS_PER_MINUTE` | no | `200` | Client-side throttle. OpenAI caps list requests at 200 per minute per workspace, shared across all event types and consumers. Lower it if other tools read the same workspace. 429 responses are retried honouring `Retry-After`. |
| `ANONYMIZE` | no | `false` | When `true`, sets `anonymize` on the Nebuly payload |
| `INTERACTION_IDLE_MINUTES` | no | `30` | Close open chat interactions after this idle window |
| `ENTRA_FETCH` | no | `false` | Live Microsoft Graph directory fetch at startup (`poetry install --with entra`). Mutually exclusive with `ENTRA_EXTRACT_PATH`. |
| `ENTRA_EXTRACT_PATH` | no | none | Pre-exported Entra JSON from [microsoft_entra](../microsoft_entra/README.md) |
| `AZURE_TENANT_ID` | if `ENTRA_FETCH` | none | Entra tenant id |
| `AZURE_CLIENT_ID` | if `ENTRA_FETCH` | none | App registration client id |
| `AZURE_CLIENT_SECRET` | if `ENTRA_FETCH` | none | Client secret |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | now − 30 days | ISO backfill start for the ingest `after` cursor on first run |
| `--to-date` | now (UTC) | Interactions with `time_end` after this are held back |
| `--sources` | `chats,codex` | Comma-separated: `chats` (messages + app logs), `codex` |
| `--cache-dir` | `./.cache` | SQLite state directory |
| `--dry-run` | off | Build payloads without POSTing; in-memory SQLite only |
| `--verbose` | off | Debug logging |

## Data sources

| Source | Compliance `event_type` | Processing |
| ------ | ----------------------- | ---------- |
| `chats` | `CONVERSATION_MESSAGE`, `APP_LOG` | Lines stored in SQLite; interactions assembled when closed |
| `codex` | `CODEX_LOG` | Self-contained rows converted and sent inline |

Listing uses `GET …/logs?event_type=&after=&limit=` with cursor `last_end_time`. Each file is immutable JSONL downloaded via `GET …/logs/{id}` (redirects supported).

## Field mapping (Nebuly v3 payload)

Each POST body has `{ interaction, traces, user_feedback, anonymize }`.

### Chat (`chats`)

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | `conversation.id` |
| `interaction.input` / `output` | User message / joined assistant chain |
| `interaction.time_start` / `time_end` | User / last assistant `message.created_at` |
| `interaction.end_user` | `actor.user_id` |
| `interaction.tags` | `build_chat_tags` + optional Entra segments |
| `traces` | `APP_LOG` retrieval pairs in the interaction window + `LLMTrace` (no token fields) |

Interaction key: user `message.id`. Closed when a later user message exists in the thread or when the last assistant message is older than `INTERACTION_IDLE_MINUTES`.

### Codex (`codex`)

| Nebuly field | Source |
| ------------ | ------ |
| `interaction.conversation_id` | `event_details.session_id` |
| `interaction.input` / `output` | `prompt_text` / `response_text` |
| `interaction.time_start` / `time_end` | Record `timestamp` (same value) |
| `traces` | `LLMTrace` with `input_tokens` / `output_tokens` when present |

Only `PROMPT_RESPONSE_RECEIVED` rows with prompt, response, and non-error status are sent. Dedup key: `event_id`.

## Customizing the payload

Edit `openai_compliance_sync/user_defined.py`:

| Function | Purpose |
| -------- | ------- |
| `build_chat_tags` / `build_chat_traces` / `build_chat_user_feedback` | Chat interactions |
| `build_codex_tags` / `build_codex_traces` / `build_codex_user_feedback` | Codex turns |

Directory `team` / `market` / `tenure` come from Entra via `actor.user_email`, matching the Copilot integrations.

## Caching & idempotency

SQLite at `<cache-dir>/sync_state.db` (WAL):

| Table | Role |
| ----- | ---- |
| `ingest_cursor` | Per `event_type` ingest watermark |
| `processed_file` | Completed log file ids |
| `chat_message` / `app_call` | Raw rows for assembly |
| `emitted_interaction` | `sent` / `rejected` per interaction key |

- **Reset:** delete the cache directory.
- **Permanent Nebuly 4xx** (except 408/429): recorded as `rejected`, run continues.
- **Transient Compliance errors:** cursor not advanced for that source.
- **Retention:** stored rows older than 35 days are pruned after each run.
- **Cursor lag:** warning logged when ingest lags more than 7 days (30-day platform retention).

## Known limitations

- No cost: Costs and Daily Usage Analytics APIs are not integrated.
- No token usage on ChatGPT chat interactions (Codex includes tokens when the platform provides them).
- Enterprise and Edu workspaces only.
- Compliance retention is about 30 days, so run at least daily (hourly recommended).
- `APP_LOG` calls that arrive after an interaction closes are not attached.
- On regeneration, only the latest assistant branch is kept.
- No explicit user feedback (`AUDIT_LOG` not mapped).
- Codex turn start time is not available; `time_start` equals `time_end`.
- Codex turns with errors or prompt logging disabled are skipped.
- Hidden system prompts and reasoning content are not exported.
- Workspace agents (`CUSTOM_AGENTS_LOG`) are not mapped.
- Team, market, and tenure are available only through the Entra join.

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
