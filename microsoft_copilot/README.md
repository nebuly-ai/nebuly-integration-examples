# Microsoft Copilot Enterprise → Nebuly Sync

Sync tool that pulls Copilot Enterprise interactions from Microsoft Graph and POSTs each paired prompt/response to Nebuly's [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Microsoft Graph (licensed users → interactions) → pair conversion → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- Azure app registration with `AiEnterpriseInteraction.Read.All` (application permission)

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/microsoft_copilot
poetry install
cp .env.example .env
# Edit .env with your keys
```

## Configuration

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `AZURE_TENANT_ID` | yes | — | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | yes | — | App registration client ID |
| `AZURE_CLIENT_SECRET` | yes | — | App registration client secret |
| `NEBULY_API_KEY` | yes | — | Nebuly secret key |
| `NEBULY_ENDPOINT` | no | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint |
| `COPILOT_SKU` | no | `639dec6b-bb19-468b-871c-c5c441c4b0cb` | Microsoft 365 Copilot SKU GUID |
| `GRAPH_MAX_REQUESTS_PER_MINUTE` | no | `600` | Rate limit for Graph interaction requests |
| `INGESTION_LAG_MINUTES` | no | `15` | Minutes to subtract from now when no `--to-date` |
| `ANONYMIZE` | no | `false` | Set to `true` to anonymize content in the Nebuly payload |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | — | ISO backfill start date (required on first run) |
| `--to-date` | — | ISO end date filter |
| `--cache-dir` | `./.cache` | Directory for the sync state database |
| `--dry-run` | off | Fetch interactions without POSTing to Nebuly |
| `--verbose` | off | Enable debug logging |

## Caching & resumable sync

State is stored in SQLite at `.cache/sync_state.db`. Each user has a coverage window `[coverage_from, coverage_until]`. Re-runs skip already-covered date ranges and only fetch backfill or tail intervals.

- First run without any cached coverage **requires** `--from-date`.
- `--dry-run` uses an in-memory cache; nothing is persisted.
- Reset by deleting the cache directory (e.g. `rm -rf .cache`).

## Running

```bash
# First run (backfill)
poetry run python -m copilot_sync --from-date 2026-01-01

# Dry run (no POST)
poetry run python -m copilot_sync --from-date 2026-06-01 --to-date 2026-06-23 --dry-run --verbose

# Incremental tail sync (uses cached coverage per user)
poetry run python -m copilot_sync
```

## Customizing the payload

Edit `copilot_sync/user_defined.py` for customer-specific tags, traces, and user feedback.
