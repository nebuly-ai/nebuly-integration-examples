# Claude Compliance → Nebuly Sync

Sync tool that pulls Users → Chats → Interactions from the Claude Compliance API and POSTs each interaction to Nebuly's [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Compliance API (users → chats → messages) → pair conversion → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)

(It's possible to setup the environment by manually installing the packages listed in `pyproject.toml` although this method is not suggested).

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/claude_compliance 
poetry install
cp .env.example .env
# Edit .env with your keys
```

## Configuration

### Environment variables


| Variable                             | Required | Default                                                                      | Description                                                                              |
| ------------------------------------ | -------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `NEBULY_API_KEY`                     | yes      | —                                                                            | Nebuly secret key, retrieve it from Settings > Projects > View Nebuly keys > Secret keys |
| `COMPLIANCE_API_KEY`                 | yes      | —                                                                            | Compliance API key (`x-api-key` header)                                                  |
| `COMPLIANCE_BASE_URL`                | no       | `https://api.anthropic.com/v1/compliance`                                    | Compliance API base URL                                                                  |
| `ORGANIZATION_UUID`                  | yes      | —                                                                            | Organization UUID to sync                                                                |
| `NEBULY_ENDPOINT`                    | no       | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint override                                                       |
| `COMPLIANCE_MAX_REQUESTS_PER_MINUTE` | no       | `600`                                                                        | Rate limit for Compliance API requests                                                   |
| `ANONYMIZE`                          | no       | `false`                                                                      | Set to `true` to anonymize content in the Nebuly payload                                 |


### CLI flags


| Flag          | Default    | Description                                            |
| ------------- | ---------- | ------------------------------------------------------ |
| `--from-date` | —          | ISO backfill start date (e.g. `2025-01-01`)            |
| `--to-date`   | —          | ISO end date filter                                    |
| `--cache-dir` | `./.cache` | Directory for the sync state database                  |
| `--dry-run`   | off        | Query Claude Compliance data without POSTing to Nebuly |
| `--verbose`   | off        | Enable debug logging (includes HTTP request traces)    |


## Customizing the payload

Edit `compliance_sync/user_defined.py` — the single extension point for customer-specific logic. Three functions are called for every user/assistant pair:


| Function                    | Returns          | Purpose                                                  |
| --------------------------- | ---------------- | -------------------------------------------------------- |
| `build_tags(pair)`          | `dict[str, str]` | String tags attached to the interaction                  |
| `build_traces(pair)`        | `list[dict]`     | Intermediate steps (LLM, Retrieval, or Embedding traces) |
| `build_user_feedback(pair)` | `list[dict]`     | Explicit user feedback events                            |


Each function receives a `MessagePair` with `user_message`, `assistant_message`, and `chat` (so `chat.user.id`, `chat.user.email_address`, `chat.model`, and message content blocks are all reachable).

`build_tags` ships with default Claude compliance metadata tags (`claude chat-id`, `claude project-id`, `model`, `chat name`, `href`). Remove any you don't need, or add your own. 

`build_traces` and `build_user_feedback` return empty lists by default; implement your logic here if needed.

## Caching & resumable sync

The sync is incremental and resumable. State is stored in a SQLite database at `.cache/sync_state.db`.

The database tracks:

- **Per-user watermarks** — highest completed chat timestamp and coverage window
- **Per-chat state** — coverage windows and status (`in_progress`, `completed`, `failed`, `deleted`)

After each interaction is successfully sent to Nebuly, the cache is checkpointed so a rerun resumes from where it left off instead of re-POSTing already-synced pairs.

- `--dry-run` — nothing is persisted.
- **Reset** by deleting the cache directory (e.g. `rm -rf .cache`).
- **Relocate** the cache with `--cache-dir /path/to/cache`.

## Running

```bash
poetry run python -m compliance_sync --from-date 2026-01-01
```

