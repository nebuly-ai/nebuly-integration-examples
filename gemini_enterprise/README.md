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

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- GCP credentials via ADC or `GOOGLE_APPLICATION_CREDENTIALS`

## Setup

```bash
cd gemini_enterprise
poetry install
cp .env.example .env
```

Authenticate with GCP:

```bash
gcloud auth application-default login
```



## Configuration


| Variable                 | Required        | Default                    | Description                                                                       |
| ------------------------ | --------------- | -------------------------- | --------------------------------------------------------------------------------- |
| `NEBULY_API_KEY`         | yes             | —                          | Nebuly secret key                                                                 |
| `GCP_PROJECT_ID`         | yes             | —                          | GCP project ID                                                                    |
| `GCP_LOCATION`           | yes             | —                          | Discovery Engine region                                                           |
| `GCP_ENGINE_ID`          | yes             | —                          | Gemini Enterprise engine ID                                                       |
| `GCP_COLLECTION`         | no              | `default_collection`       | Discovery Engine collection                                                       |
| `NEBULY_ENDPOINT`        | no              | v3 `trace_interaction` URL | Nebuly ingestion endpoint                                                         |
| `GCP_SETTLE_LAG_SECONDS` | no              | `60`                       | Upper bound lag before ingesting recent logs                                      |
| `GCP_LOG_BATCH_SIZE`     | no              | `500`                      | Max matching log records per batch                                                |
| `GCP_LOG_PAGE_SIZE`      | no              | `1000`                     | Cloud Logging page size for `entries.list` pagination                             |
| `GCP_TRACE_CONCURRENCY`  | no              | `32`                       | Parallel async `get_trace` calls                                                  |
| `ANONYMIZE`              | no              | `false`                    | Anonymize content in Nebuly payload                                               |
| `GCP_LOG_SOURCE`         | no              | `logging`                  | Log source: `logging` (live API) or `bigquery`                                    |
| `GCP_BIGQUERY_TABLE`     | when `bigquery` | —                          | Fully-qualified table (`project.dataset.table` or `..._*` for date-sharded sinks) |
| `GCP_BIGQUERY_LOCATION`  | no              | auto-detect                | BigQuery dataset location                                                         |




### Log source

By default the sync reads logs from the **Cloud Logging** `entries.list` API. For faster fetches, you can export logs to BigQuery via a Cloud Logging sink and set `GCP_LOG_SOURCE=bigquery`.

Switch via env var or CLI:

```bash
GCP_LOG_SOURCE=bigquery poetry run python -m gemini_enterprise_sync ...
poetry run python -m gemini_enterprise_sync --log-source bigquery ...
```

**Create the sink** with this filter (required — only matching entries are exported):

```
logName="projects/{PROJECT_ID}/logs/discoveryengine.googleapis.com%2Fgemini_enterprise_user_activity"
AND (jsonPayload.serviceTextReply:* OR protoPayload.response.reply:*)
```

Example:

```bash
gcloud logging sinks create gemini-enterprise-logs \
  bigquery.googleapis.com/projects/PROJECT_ID/datasets/gemini_enterprise_logs \
  --log-filter='logName="projects/PROJECT_ID/logs/discoveryengine.googleapis.com%2Fgemini_enterprise_user_activity" AND (jsonPayload.serviceTextReply:* OR protoPayload.response.reply:*)'
```

`GCP_BIGQUERY_TABLE`: for date-sharded sinks (the default), use the base table name with `_*` suffix, e.g. `my-project.gemini_enterprise_logs.discoveryengine_googleapis_com_gemini_enterprise_user_activity_*`. For partitioned-table sinks, use the plain table name.

Cloud Trace token enrichment is unchanged — it runs per batch regardless of log source.

### CLI flags

| Flag                  | Description                                                             |
| --------------------- | ----------------------------------------------------------------------- |
| `--from-date`         | ISO backfill start (required on first run; overlaps merge, gaps prompt) |
| `--to-date`           | ISO end date upper bound                                                |
| `--cache-dir`         | Coverage state directory (default `./.cache`)                           |
| `--batch-size`        | Override `GCP_LOG_BATCH_SIZE`                                           |
| `--trace-concurrency` | Override `GCP_TRACE_CONCURRENCY`                                        |
| `--trace-workers`     | Alias for `--trace-concurrency`                                         |
| `--log-source`        | Override `GCP_LOG_SOURCE` (`logging` or `bigquery`)                     |
| `--yes` / `--force`   | Confirm gap-creating runs without prompting                             |
| `--dry-run`           | Build payloads without POSTing or persisting coverage                   |
| `--verbose`           | Debug logging                                                           |

### IAM permissions

- `logging.logEntries.list` (Cloud Logging source only)
- `cloudtrace.traces.get`
- `bigquery.jobs.create` + `bigquery.tables.getData` (BigQuery source only; `roles/bigquery.jobUser` + `roles/bigquery.dataViewer`)

## Running

```bash
poetry run python -m gemini_enterprise_sync
poetry run python -m gemini_enterprise_sync --dry-run --verbose --from-date 2026-07-02T00:00:00Z
```

Resume state is stored in `cache_dir/coverage.json` as a covered range `[coverage_from, coverage_until]`. Overlapping or adjacent backfills merge into that range; Nebuly dedup handles re-sends. A manually supplied `--from-date` that would create a **gap** (disjoint from existing coverage) prompts for confirmation; use `--yes` in non-interactive environments. First run without existing coverage **requires** `--from-date`.

## Customizing payloads

Edit `gemini_enterprise_sync/user_defined.py` for tags, traces (LLMTrace + RetrievalTrace), and user feedback.

## Tests

```bash
poetry run pytest
```

