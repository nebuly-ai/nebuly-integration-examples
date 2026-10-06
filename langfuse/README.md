# Langfuse → Nebuly Sync

Incremental export that pulls traces and observations from the [Langfuse](https://langfuse.com/) v3 public API, converts them to Nebuly interactions, and POSTs each one to the [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2) (v3).

```
Langfuse public API (traces + observations) → pair conversion → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- Langfuse project API keys (public + secret) with access to the public API

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/langfuse
poetry install
cp .env.example .env
# Edit .env with your keys
```

## Configuration

### Environment variables

| Variable                    | Required | Default                                                                      | Description                                              |
| --------------------------- | -------- | ---------------------------------------------------------------------------- | -------------------------------------------------------- |
| `LANGFUSE_PUBLIC_KEY`       | yes      | —                                                                            | Langfuse public key                                      |
| `LANGFUSE_SECRET_KEY`       | yes      | —                                                                            | Langfuse secret key                                      |
| `NEBULY_API_KEY`            | yes      | —                                                                            | Nebuly secret key, from Settings → Projects → Secret keys |
| `LANGFUSE_BASE_URL`           | no       | `https://cloud.langfuse.com`                                                 | Langfuse origin (cloud or self-hosted)                   |
| `LANGFUSE_SETTLE_LAG_SECONDS` | no       | `900`                                                                        | Do not sync traces newer than `now - lag`                |
| `NEBULY_ENDPOINT`             | no       | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint                                |
| `ANONYMIZE`                   | no       | `false`                                                                      | Set to `true` to anonymize content in the Nebuly payload |

For self-hosted Langfuse, set `LANGFUSE_BASE_URL` to your instance origin (e.g. `https://langfuse.mycompany.com`) with no `/api/public` suffix.

### CLI flags

| Flag          | Description                                           |
| ------------- | ----------------------------------------------------- |
| `--from-date` | ISO start (required on first run)                     |
| `--to-date`   | ISO end (default: now − settle lag)                   |
| `--cache-dir` | Coverage directory (default: `./.cache`)              |
| `--dry-run`   | No POSTs or cache writes                              |
| `--verbose`   | Debug logging                                         |
| `--yes`       | Confirm gap-creating runs without prompting           |

**Migration:** replace `START_DATE` / `END_DATE` and `langfuse_to_nebuly.py` with `python -m langfuse_sync --from-date …`.

## Caching & resumable sync

Coverage is stored in `.cache/coverage.json` (`coverage_from`, `coverage_until`, `coverage_until_ids`).

- First run without cache **requires** `--from-date`.
- **Tail / new data:** after each POST (including 413 skips and empty traces), `coverage_until` moves forward and `coverage.json` is rewritten — resume with `poetry run python -m langfuse_sync`.
- **Backfill** (dates before the existing watermark): sends proceed, but `coverage_until` is not moved backward; `coverage_from` updates when that interval finishes.
- `--dry-run` does not persist cache or POST.
- Reset with `rm -rf .cache`.

## Running

```bash
poetry run python -m langfuse_sync --from-date 2026-01-01
poetry run python -m langfuse_sync --from-date 2026-06-01 --to-date 2026-06-23 --dry-run --verbose
poetry run python -m langfuse_sync
```

## Known limitations

- **Trace updates after settle lag:** Langfuse v3 has no trace `updatedAt`; late changes are not re-sent. Increase `LANGFUSE_SETTLE_LAG_SECONDS` for long runs.
- **Langfuse Cloud legacy API sunset (Nov 16, 2026):** This integration targets self-hosted v3 page APIs; v4 is out of scope.
- **Skipped empty traces:** Empty input and output are not POSTed; watermark still advances.
- **HTTP 413:** Skipped with a warning; watermark advances.
- **Tags:** `team:Engineering` maps via Nebuly tags; metadata is not sent.

## Layout

- `langfuse_sync/` — config, coverage, sync, converter, clients
- `python -m langfuse_sync` — entry point

## References

- [Langfuse public API](https://langfuse.com/docs/api-and-data-platform/features/public-api)
- [Nebuly ingestion API reference](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2)
