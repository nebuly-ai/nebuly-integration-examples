# Claude Compliance → Nebuly POC Sync

Standalone POC that pulls Users → Chats → Interactions from the Claude Compliance API and POSTs each interaction to Nebuly's public v3 ingestion endpoint.

## Setup

```bash
cd claude_compliance
poetry install
cp .env.example .env
# Edit .env with your keys
```

## Run the sync

```bash
cd claude_compliance
poetry run python -m compliance_sync --from-date 2025-01-01 --dry-run
poetry run python -m compliance_sync --from-date 2025-01-01
```

## Options

| Env / CLI | Description |
|-----------|-------------|
| `NEBULY_API_KEY` | Bearer token for Nebuly ingestion |
| `COMPLIANCE_API_KEY` | Compliance API key (`x-api-key` header) |
| `COMPLIANCE_BASE_URL` | Compliance API base URL (default mock: `http://localhost:8088`) |
| `ORGANIZATION_UUID` | Organization UUID to sync |
| `NEBULY_ENDPOINT` | Override Nebuly v3 endpoint |
| `COMPLIANCE_MAX_REQUESTS_PER_MINUTE` | Rate limit (default 600) |
| `ANONYMIZE` | `true`/`false` (default `false`) |
| `--from-date` | ISO backfill start |
| `--to-date` | ISO end filter |
| `--cache-dir` | Checkpoint directory (default `./.cache`) |
| `--dry-run` | Log payloads without POSTing |

Checkpoint file: `.cache/checkpoint.json` — per-user watermark + boundary ids for resumable incremental sync.

## Tests

```bash
poetry run pytest
```
