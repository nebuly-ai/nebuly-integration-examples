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
| `GCP_LOG_BATCH_SIZE` | no | `500` | Max matching log records per batch |
| `GCP_LOG_PAGE_SIZE` | no | `1000` | Cloud Logging page size for `entries.list` pagination |
| `GCP_TRACE_CONCURRENCY` | no | `32` | Parallel async `get_trace` calls |
| `ANONYMIZE` | no | `false` | Anonymize content in Nebuly payload |

### CLI flags

| Flag | Description |
| ---- | ----------- |
| `--from-date` | ISO backfill start (required on first run; overlaps merge, gaps prompt) |
| `--to-date` | ISO end date upper bound |
| `--cache-dir` | Coverage state directory (default `./.cache`) |
| `--batch-size` | Override `GCP_LOG_BATCH_SIZE` |
| `--trace-concurrency` | Override `GCP_TRACE_CONCURRENCY` |
| `--trace-workers` | Alias for `--trace-concurrency` |
| `--yes` / `--force` | Confirm gap-creating runs without prompting |
| `--dry-run` | Build payloads without POSTing or persisting coverage |
| `--verbose` | Debug logging |

### IAM permissions

- `logging.logEntries.list`
- `cloudtrace.traces.get`

## Running

```bash
poetry run python -m gemini_enterprise_sync
poetry run python -m gemini_enterprise_sync --dry-run --verbose --from-date 2026-07-02T00:00:00Z
```

Resume state is stored in `cache_dir/coverage.json` as a covered range `[coverage_from, coverage_until]`. Overlapping or adjacent backfills merge into that range; Nebuly dedup handles re-sends. A manually supplied `--from-date` that would create a **gap** (disjoint from existing coverage) prompts for confirmation; use `--yes` in non-interactive environments. First run without existing coverage **requires** `--from-date`.

Log fetch uses a server-side reply predicate (`serviceTextReply` / `protoPayload.response.reply`) so only matching entries are returned. `entries.list` latency is dominated by the Cloud Logging API itself — expect multi-second fetches for modest windows; Log Analytics/BigQuery is not required.

Trace fetch uses async gRPC with a shared, pre-refreshed credential so high concurrency does not trigger per-RPC token refresh storms against `oauth2.googleapis.com`.

## Customizing payloads

Edit `gemini_enterprise_sync/user_defined.py` for tags, traces (LLMTrace + RetrievalTrace), and user feedback.

## Tests

```bash
poetry run pytest
```
