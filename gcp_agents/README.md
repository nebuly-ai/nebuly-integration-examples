# GCP Reasoning Engine → Nebuly Sync

Sync tool that pulls conversation sessions from a GCP Vertex AI Reasoning Engine agent and POSTs each user↔agent turn to Nebuly's [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
GCP Reasoning Engine (sessions → events) → turn conversion → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- GCP credentials via Application Default Credentials (ADC), or a service account key via `GOOGLE_APPLICATION_CREDENTIALS`

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/gcp_agents
poetry install
cp .env.example .env
# Edit .env with your keys and reasoning engine details
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
| `GCP_LOCATION` | yes | — | Reasoning engine region (e.g. `us-west1`) |
| `GCP_REASONING_ENGINE_ID` | yes | — | Reasoning engine ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | no | — | Path to service account JSON (optional; ADC used otherwise) |
| `NEBULY_ENDPOINT` | no | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint override |
| `GCP_MAX_REQUESTS_PER_MINUTE` | no | `600` | Rate limit for GCP API requests |
| `GCP_SETTLE_LAG_SECONDS` | no | `60` | Hold back turns whose last event is within this many seconds of now |
| `ANONYMIZE` | no | `false` | Set to `true` to anonymize content in the Nebuly payload |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | — | ISO backfill start date, applied client-side against session `updateTime` |
| `--to-date` | — | ISO end date filter, applied client-side against session `updateTime` |
| `--cache-dir` | `./.cache` | Directory for the sync state database |
| `--dry-run` | off | Fetch sessions and build payloads without POSTing to Nebuly |
| `--verbose` | off | Enable debug logging |

### IAM permissions

The authenticated principal needs access to list sessions and events on the reasoning engine, typically:

- `aiplatform.reasoningEngines.get`
- `aiplatform.reasoningEngines.query` (or equivalent session/event read permissions for your engine setup)

## Running the sync

```bash
poetry run python -m gcp_agents_sync
poetry run python -m gcp_agents_sync --dry-run --verbose
poetry run python -m gcp_agents_sync --from-date 2026-06-01 --to-date 2026-06-30
```

## Customizing the payload

Edit `gcp_agents_sync/user_defined.py` — the extension point for customer-specific logic:

| Function | Returns | Purpose |
| -------- | ------- | ------- |
| `build_tags(turn, session)` | `dict[str, str]` | String tags attached to the interaction |
| `build_traces(turn)` | `list[dict]` | Per-step LLM traces with token usage |
| `build_user_feedback(turn)` | `list[dict]` | Explicit user feedback events |

Turns are grouped by GCP `invocationId` (one user message = one Nebuly interaction). Intermediate agent steps (routing function calls, sub-agent generations) are emitted as LLM traces.

## Caching & incremental sync

State is stored in SQLite at `.cache/sync_state.db` (in-memory when `--dry-run`).

The sync skips fetching events for a session when its cached `last_seen_update_time` matches the current session `updateTime` and status is `complete`. Within a session, a per-turn watermark (`last_sent_event_time` + `last_sent_invocation_id`) avoids re-sending already exported turns.

Recent turns within `GCP_SETTLE_LAG_SECONDS` of the current time are held back and marked `partial` so the next run can pick them up once settled.

To force a full rescan, delete `.cache/sync_state.db` or the relevant rows in `sync_session_state`.

## Demo agent

The `example_agent/` folder contains a sample ADK agent used to exercise the reasoning engine locally. It is separate from the sync pipeline.
