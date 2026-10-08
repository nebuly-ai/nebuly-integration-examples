# Claude Compliance → Nebuly Sync

Sync tool that pulls **chats**, **local sessions**, and **remote sessions** from the [Claude Compliance API](https://platform.claude.com/docs/en/api/compliance/apps) and POSTs each closed interaction to Nebuly's [trace interaction endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Compliance API (chats + local/remote session transcripts) → interaction cut → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/claude_compliance
poetry install --with dev
cp .env.example .env
# Edit .env with your keys
```

## Configuration

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `NEBULY_API_KEY` | yes | — | Nebuly secret key |
| `COMPLIANCE_API_KEY` | yes | — | Compliance API key (`x-api-key` header) |
| `COMPLIANCE_BASE_URL` | no | `https://api.anthropic.com/v1/compliance` | Compliance API base URL |
| `ORGANIZATION_UUID` | yes | — | Organization UUID to sync |
| `NEBULY_ENDPOINT` | no | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint |
| `COMPLIANCE_MAX_REQUESTS_PER_MINUTE` | no | `600` | Rate limit for Compliance API requests |
| `ANONYMIZE` | no | `false` | Set to `true` to anonymize content in the Nebuly payload |
| `SESSION_IDLE_MINUTES` | no | `30` | Minutes without a new prompt before a session interaction is considered closed |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | — | ISO backfill start (e.g. `2025-01-01T00:00:00Z`). Re-processes conversations whose cached coverage starts later than this. |
| `--to-date` | now (UTC) | ISO end bound. Interactions with `time_end` after this are held back; the conversation stays `pending`. |
| `--sources` | `chats,local_sessions,remote_sessions` | Comma-separated sources to sync |
| `--cache-dir` | `./.cache` | Directory for the sync state database |
| `--dry-run` | off | Build payloads without POSTing to Nebuly; nothing is persisted in SQLite |
| `--verbose` | off | Debug logging (includes HTTP traces) |

## Data sources

| Source | Listing | Notes |
| ------ | ------- | ----- |
| `chats` | Org-wide `GET apps/chats` with `order_by=updated_at` | Skips `deleted_at` chats and chats with `user: null` |
| `local_sessions` | `GET apps/sessions/local` (client-side org filter) | 404 “not available” → source skipped for the run |
| `remote_sessions` | `GET apps/sessions/remote` by `created_at` | Open (`pending`/`active`/`paused`) sessions revisited each run; list metadata cached for `end_user` |

## Field mapping (Nebuly v3 payload)

Each POST body has `{ interaction, traces, user_feedback, anonymize }`.

| Nebuly field | Chats | Local / remote sessions |
| ------------ | ----- | ------------------------ |
| `interaction.conversation_id` | Chat id | Session id |
| `interaction.input` / `output` | User / assistant text pair | Prompt text / last verified assistant text in the cut |
| `interaction.time_start` / `time_end` | User / assistant message times | Prompt time / max `created_at` in the interaction |
| `interaction.end_user` | `chat.user.id` | `session.user.id`, or remote `started_by_user.id` from list metadata |
| `interaction.tags` | From `build_tags` + `claude source=chat` | From `build_session_tags` + `claude source=local_session` or `remote_session` |
| `traces` | Tool `RetrievalTrace`s by default | Session `LLMTrace` + tool retrievals via hooks |

Content strings are truncated to 8000 characters (same idea as the Bedrock example).

## Customizing the payload

Edit `compliance_sync/user_defined.py`:

| Function | Purpose |
| -------- | ------- |
| `build_tags` / `build_traces` / `build_user_feedback` | Per chat message pair |
| `build_session_tags` / `build_session_traces` | Per session interaction cut |
| `INJECTED_CONTEXT_PATTERNS` | Regexes stripped from user text before classifying a “prompt” (includes compaction summary heuristic) |

`team`, `market`, and `tenure` are not provided by Compliance; set them in these hooks if you have an external directory.

## Caching & idempotency

SQLite at `<cache-dir>/sync_state.db` (WAL mode):

| Table | Role |
| ----- | ---- |
| `sync_source_state` | Per-source watermark (`run_started` when a source finishes without transient failure) |
| `sync_conversation_state` | `completed` / `pending` / `failed` / `gone`, `coverage_from`, optional `metadata_json` (remote list fields) |
| `sync_emitted_interaction` | `(source, conversation_id, interaction_key)` → `sent` or `rejected` |

Interaction keys: assistant message id (chats) or first human prompt message id (sessions).

Listing overlap: without `--from-date`, each source uses `watermark − 10 minutes` as `updated_at.gte` / `created_at.gte`.

- **Reset:** delete the cache directory.
- **Poison payloads:** Nebuly 4xx (except 408/429) are recorded as `rejected` and the run continues.
- **Transient errors:** send/fetch failures mark the conversation `failed` and stop the current source without advancing its watermark.

## Known limitations

- No token or cost fields from Compliance (use Analytics APIs separately if needed).
- Chat branches/regenerations are flattened to user/assistant pairs; a second assistant after one user message is dropped.
- Remote sessions do not expose `model` on messages; session tags may show `unknown`.
- Prompt detection uses heuristics (`<system-reminder>` stripping, `INJECTED_CONTEXT_PATTERNS`); filter by `product surface` in Nebuly if needed.
- `--to-date` filters on interaction `time_end`, not listing timestamps.
- Cowork / Office / Chrome injected wrappers may need extra patterns (see open questions in the integration plan).

## Running

```bash
poetry run python -m compliance_sync --from-date 2026-01-01T00:00:00Z
poetry run python -m compliance_sync --sources chats,local_sessions
```

### Against the local Compliance mock (nebuly-ingestion-worker)

```bash
# Terminal 1 — mock API
cd path/to/nebuly-ingestion-worker
poetry run uvicorn mocks.claude_compliance.app:app --port 8088

# Terminal 2 — capture Nebuly POSTs (optional)
python3 capture_server.py 8099 ./captured.jsonl

# Terminal 3 — sync
cd claude_compliance
COMPLIANCE_BASE_URL=http://localhost:8088/v1/compliance \
ORGANIZATION_UUID=org_demo \
COMPLIANCE_API_KEY=x \
NEBULY_API_KEY=x \
NEBULY_ENDPOINT=http://localhost:8099/ingest \
poetry run python -m compliance_sync --cache-dir /tmp/compliance-cache --verbose
```

Second run should send **0** interactions. After appending a message via the mock admin route, the next run should send exactly one new interaction.

## Tests

```bash
poetry run pytest -q
```
