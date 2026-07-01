# Gemini Enterprise → Nebuly Sync

Sync tool that pulls conversation sessions from a GCP Discovery Engine (Gemini Enterprise) assistant and POSTs each user↔agent turn to Nebuly's [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Gemini Enterprise (sessions → turns) → turn conversion → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- GCP credentials via Application Default Credentials (ADC), or a service account key via `GOOGLE_APPLICATION_CREDENTIALS`

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/gemini_enterprise
poetry install
cp .env.example .env
# Edit .env with your keys and Discovery Engine details
```

Authenticate with GCP (if not using a service account key file):

```bash
gcloud auth application-default login
```

## Configuration

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `NEBULY_API_KEY` | yes | — | Nebuly secret key |
| `GCP_PROJECT_ID` | yes | — | GCP project ID |
| `GCP_LOCATION` | yes | — | Discovery Engine region (e.g. `eu`, `us`) |
| `GCP_COLLECTION` | no | `default_collection` | Discovery Engine collection ID |
| `GCP_ENGINE_ID` | yes | — | Gemini Enterprise engine ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | no | — | Path to service account JSON (optional; ADC used otherwise) |
| `NEBULY_ENDPOINT` | no | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint override |
| `GCP_MAX_REQUESTS_PER_MINUTE` | no | `600` | Rate limit for Discovery Engine API requests |
| `GCP_SETTLE_LAG_SECONDS` | no | `60` | Hold back turns whose last reply is within this many seconds of now |
| `ANONYMIZE` | no | `false` | Set to `true` to anonymize content in the Nebuly payload |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | — | ISO backfill start date, applied server-side via session list filter |
| `--to-date` | — | ISO end date filter, applied client-side against session `endTime` |
| `--cache-dir` | `./.cache` | Directory for the sync state database |
| `--dry-run` | off | Fetch sessions and build payloads without POSTing to Nebuly |
| `--verbose` | off | Enable debug logging |

### IAM permissions

The authenticated principal needs access to list and read sessions on the Discovery Engine assistant, typically:

- `discoveryengine.sessions.list`
- `discoveryengine.sessions.get`

## Running the sync

```bash
poetry run python -m gemini_enterprise_sync
poetry run python -m gemini_enterprise_sync --dry-run --verbose
poetry run python -m gemini_enterprise_sync --from-date 2026-06-01 --to-date 2026-06-30
```

## Customizing the payload

Edit `gemini_enterprise_sync/user_defined.py` — the extension point for customer-specific logic:

| Function | Returns | Purpose |
| -------- | ------- | ------- |
| `build_tags(turn, session)` | `dict[str, str]` | String tags attached to the interaction |
| `build_traces(turn)` | `list[dict]` | Per-step traces (retrieval from grounding metadata) |
| `build_user_feedback(turn)` | `list[dict]` | Explicit user feedback events |

Turns are grouped by session turn (one user query = one Nebuly interaction). Thought replies and inline suggestion payloads are excluded from the final answer text. Grounding references on separate replies are emitted as retrieval traces.

## Limitations

- **No token usage**: Gemini Enterprise session payloads do not expose prompt/completion token counts, so LLM traces with token metrics are not available.
- **Grounding only**: Intermediate reasoning steps appear only when the API returns `textGroundingMetadata` on a reply.

## Shared modules

`nebuly_client.py` and the datetime helpers in `config.py` (`timestamp_str_to_datetime`, `datetime_to_timestamp_str`) are intentional copies from the other integrations in this repo — they are kept local so each package remains self-contained.

## Caching & incremental sync

State is stored in SQLite at `.cache/sync_state.db` (in-memory when `--dry-run`).

The sync skips re-fetching a session when its cached `last_seen_end_time` matches the current session `endTime` and status is `complete`. Within a session, a per-turn watermark (`last_sent_turn_time` + `last_sent_query_id`) avoids re-sending already exported turns.

Recent turns within `GCP_SETTLE_LAG_SECONDS` of the current time are held back and marked `partial` so the next run can pick them up once settled.

To force a full rescan, delete `.cache/sync_state.db` or the relevant rows in `sync_session_state`.

## Tests

```bash
poetry run pytest
```
