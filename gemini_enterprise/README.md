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

Follow Nebuly's [setup guide](https://docs.nebuly.com/integrations/gemini-enterprise/gemini-enterprise)
to retrieve all the required credentials and setup GCP.

## Setup

```bash
poetry install
cp .env.example .env
```

## Configuration


| Variable                 | Required        | Default                    | Description                                                                       |
| ------------------------ | --------------- | -------------------------- | --------------------------------------------------------------------------------- |
| `NEBULY_API_KEY`         | yes             | —                          | Nebuly secret key                                                                 |
| `GCP_PROJECT_ID`         | yes             | —                          | GCP project ID                                                                    |
| `GCP_LOCATION`           | yes             | —                          | Discovery Engine region                                                           |
| `GCP_ENGINE_ID`          | yes             | —                          | Gemini Enterprise engine ID                                                       |
| `USER_HASH_SECRET`       | when pseudonymizing | —                          | Secret key for deterministic `end_user` pseudonyms, see below           |
| `GCP_COLLECTION`         | no              | `default_collection`       | Discovery Engine collection                                                       |
| `NEBULY_ENDPOINT`        | no              | v3 `trace_interaction` URL | Nebuly ingestion endpoint                                                         |
| `GCP_SETTLE_LAG_SECONDS` | no              | `60`                       | Upper bound lag before ingesting recent logs                                      |
| `GCP_LOG_BATCH_SIZE`     | no              | `500`                      | Max matching log records per batch                                                |
| `GCP_LOG_PAGE_SIZE`      | no              | `1000`                     | Cloud Logging page size for `entries.list` pagination                             |
| `GCP_TRACE_CONCURRENCY`  | no              | `32`                       | Parallel async `get_trace` calls                                                  |
| `ANONYMIZE`              | no              | `false`                    | Anonymize content in Nebuly payload                                               |
| `SEND_PLAIN_END_USER`    | no              | `false`                    | Send raw email as `end_user` instead of pseudonymizing                            |
| `GCP_LOG_SOURCE`         | no              | `logging`                  | Log source: `logging` (live API) or `bigquery`                                    |
| `GCP_BIGQUERY_TABLE`     | when `bigquery` | —                          | Fully-qualified table (`project.dataset.table` or `..._*` for date-sharded sinks) |
| `GCP_BIGQUERY_LOCATION`  | no              | auto-detect                | BigQuery dataset location                                                         |

By default, `end_user` is a deterministic UUID pseudonym derived from the user's email via keyed HMAC-SHA256 (`USER_HASH_SECRET`). No mapping file or cache is required — the same email always maps to the same UUID. To recover real identities, re-hash your org's user list with the same secret and match tokens.

Set `SEND_PLAIN_END_USER=true` to send raw emails instead (no secret needed).

Generate a secret once and store it securely (e.g. in your secrets manager or `.env`):

```bash
python -c "import secrets; print(secrets.token_hex(32))"
# or: openssl rand -hex 32
```

### Log source

By default the sync reads logs from the **Cloud Logging** API. For faster fetches, you can export logs to BigQuery via a Cloud Logging sink and set `GCP_LOG_SOURCE=bigquery`. Follow Nebuly's [setup guide](https://docs.nebuly.com/integrations/gemini-enterprise/gemini-enterprise) to setup the sink.

Cloud Trace token enrichment is unchanged — it runs per batch regardless of log source.

### CLI flags

| Flag                  | Description                                                             |
| --------------------- | ----------------------------------------------------------------------- |
| `--from-date`         | ISO backfill start (required on first run)                              |
| `--to-date`           | ISO end date upper bound                                                |
| `--cache-dir`         | Coverage state directory (default `./.cache`)                           |
| `--batch-size`        | Override `GCP_LOG_BATCH_SIZE`                                           |
| `--trace-concurrency` | Override `GCP_TRACE_CONCURRENCY`                                        |
| `--trace-workers`     | Alias for `--trace-concurrency`                                         |
| `--log-source`        | Override `GCP_LOG_SOURCE` (`logging` or `bigquery`)                     |
| `--yes` / `--force`   | Confirm gap-creating runs without prompting                             |
| `--dry-run`           | Build payloads without POSTing or persisting coverage                   |
| `--verbose`           | Debug logging                                                           |

## Running

```bash
poetry run python -m gemini_enterprise_sync --from-date 2026-01-01
poetry run python -m gemini_enterprise_sync --dry-run --verbose --from-date 2026-07-02
```

Resume state is stored in `cache_dir/coverage.json` as a covered range `[coverage_from, coverage_until]`. Overlapping or adjacent backfills merge into that range; Nebuly dedup handles re-sends.

A manually supplied `--from-date` that would create a **gap** (disjoint from existing coverage) prompts for confirmation; use `--yes` in non-interactive environments. 

First run without existing coverage **requires** `--from-date`.

## Customizing payloads

Edit `gemini_enterprise_sync/user_defined.py` to customize tags, traces (LLMTrace + RetrievalTrace), or integrate with your own user feedback system (if any).

## Tests

```bash
poetry run pytest
```

