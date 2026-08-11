# Langfuse → Nebuly Sync

Batch export that pulls traces and observations from the [Langfuse](https://langfuse.com/) public API for a configured date range, converts them to Nebuly interactions, and POSTs each one to the [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2) (v3).

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
| `LANGFUSE_BASE_URL`         | no       | `https://cloud.langfuse.com`                                                 | Langfuse origin (cloud or self-hosted)                   |
| `START_DATE`                | no       | `2026-01-01`                                                                 | Inclusive start date (`YYYY-MM-DD`)                      |
| `END_DATE`                  | no       | `2026-12-31`                                                                 | Inclusive end date (`YYYY-MM-DD`)                        |
| `NEBULY_ENDPOINT`           | no       | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint                                |
| `ANONYMIZE`                 | no       | `false`                                                      | Set to `true` to anonymize content in the Nebuly payload |

For self-hosted Langfuse, set `LANGFUSE_BASE_URL` to your instance origin (e.g. `https://langfuse.mycompany.com`) with no `/api/public` suffix. Other cloud regions use hosts such as `https://us.cloud.langfuse.com`.

## Running

Set `START_DATE` and `END_DATE` in `.env`, then:

```bash
poetry run python langfuse_to_nebuly.py
```

Each run fetches the full date window from Langfuse and POSTs all converted interactions to Nebuly. There is no local cache or incremental resume — narrow the date range or rely on Nebuly dedup if you re-run the same window.

## Known limitations

- **No incremental sync:** Unlike the other integrations in this repo, this script does not track coverage or watermarks. Every run re-fetches the configured date range.
- **Skipped empty traces:** Traces with both empty input and output are dropped before POSTing.
- **Payload size cap:** Interactions that exceed Nebuly's ingestion limit return HTTP 413 and are skipped (logged as warnings); the run continues for remaining interactions.
- **Langfuse rate limits:** The client retries on HTTP 429 with backoff; very large backfills may take a while.

## Layout

- `langfuse_to_nebuly.py` — entry script
- `langfuse_sync/` — config, models, converter, and API clients

## References

- [Langfuse public API](https://langfuse.com/docs/api-and-data-platform/features/public-api)
- [Nebuly ingestion API reference](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2)
