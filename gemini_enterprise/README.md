# Gemini Enterprise Traces → Nebuly Sync

Sync tool that reads **Cloud Logging** (primary stream) and **Cloud Trace** (token correlation) for Gemini Enterprise user activity, then POSTs each interaction to Nebuly's v3 `trace_interaction` endpoint.

```
Cloud Logging (ascending log sweep)
        │
        ├─► user identity (userIamPrincipal)
        ├─► prompt / response / grounding
        │
        ▼
Parallel Cloud Trace get_trace (per batch)
        │
        └─► gen_ai.usage.* token counts
                │
                ▼
        Nebuly trace_interaction
```

Unlike the sibling `gemini_enterprise/` integration (Discovery Engine sessions API), this project uses real IAM principals instead of rotating `userPseudoId` values and includes token counts from trace span labels.

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- GCP credentials via ADC or `GOOGLE_APPLICATION_CREDENTIALS`

## Setup

```bash
cd gemini_enterprise_traces
poetry install
cp .env.example .env
```

Authenticate with GCP:

```bash
gcloud auth application-default login
```

## Configuration

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `NEBULY_API_KEY` | yes | — | Nebuly secret key |
| `GCP_PROJECT_ID` | yes | — | GCP project ID |
| `GCP_LOCATION` | yes | — | Discovery Engine region |
| `GCP_ENGINE_ID` | yes | — | Gemini Enterprise engine ID |
| `GCP_COLLECTION` | no | `default_collection` | Discovery Engine collection |
| `NEBULY_ENDPOINT` | no | v3 `trace_interaction` URL | Nebuly ingestion endpoint |
| `GCP_SETTLE_LAG_SECONDS` | no | `60` | Upper bound lag before ingesting recent logs |
| `GCP_LOG_BATCH_SIZE` | no | `500` | Max log records per batch |
| `GCP_TRACE_MAX_WORKERS` | no | `16` | Parallel `get_trace` workers per batch |
| `ANONYMIZE` | no | `false` | Anonymize content in Nebuly payload |

### CLI flags

| Flag | Description |
| ---- | ----------- |
| `--from-date` | ISO backfill start (overrides cursor on first run) |
| `--to-date` | ISO end date upper bound |
| `--cache-dir` | Cursor directory (default `./.cache`) |
| `--batch-size` | Override `GCP_LOG_BATCH_SIZE` |
| `--trace-workers` | Override `GCP_TRACE_MAX_WORKERS` |
| `--dry-run` | Build payloads without POSTing |
| `--verbose` | Debug logging |

### IAM permissions

- `logging.logEntries.list`
- `cloudtrace.traces.get`

## Running

```bash
poetry run python -m gemini_enterprise_sync
poetry run python -m gemini_enterprise_sync --dry-run --verbose --from-date 2026-07-02T00:00:00Z
```

Resume state is stored in `cache_dir/cursor.json` (single global timestamp cursor). Nebuly ingestion is idempotent — re-sending the last entry after a crash is harmless.

## Customizing payloads

Edit `gemini_enterprise_sync/user_defined.py` for tags, traces (LLMTrace + RetrievalTrace), and user feedback.

## Tests

```bash
poetry run pytest
```
