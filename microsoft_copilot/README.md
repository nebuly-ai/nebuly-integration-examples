# Microsoft Copilot Enterprise → Nebuly Sync

Sync tool that pulls Copilot Enterprise interactions from Microsoft Graph and POSTs each paired prompt/response to Nebuly's [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Microsoft Graph (licensed users → interactions) → pair conversion → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- Azure app registration with authorizations:
  - `AiEnterpriseInteraction.Read.All`
  - `User.Read.All`

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


| Variable                        | Required | Default                                                                      | Description                                              |
| ------------------------------- | -------- | ---------------------------------------------------------------------------- | -------------------------------------------------------- |
| `AZURE_TENANT_ID`               | yes      | —                                                                            | Azure AD tenant ID                                       |
| `AZURE_CLIENT_ID`               | yes      | —                                                                            | App registration client ID                               |
| `AZURE_CLIENT_SECRET`           | yes      | —                                                                            | App registration client secret                           |
| `NEBULY_API_KEY`                | yes      | —                                                                            | Nebuly secret key                                        |
| `NEBULY_ENDPOINT`               | no       | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint                                |
| `COPILOT_SKU`                   | no       | `639dec6b-bb19-468b-871c-c5c441c4b0cb`                                       | Microsoft 365 Copilot SKU GUID                           |
| `GRAPH_MAX_REQUESTS_PER_MINUTE` | no       | `1800`                                                                       | Rate limit for Graph interaction requests                |
| `ANONYMIZE`                     | no       | `false`                                                                      | Set to `true` to anonymize content in the Nebuly payload |


### CLI flags


| Flag          | Default    | Description                                     |
| ------------- | ---------- | ----------------------------------------------- |
| `--from-date` | —          | ISO backfill start date (required on first run) |
| `--to-date`   | —          | ISO end date filter                             |
| `--cache-dir` | `./.cache` | Directory for the sync state database           |
| `--dry-run`   | off        | Fetch interactions without POSTing to Nebuly    |
| `--verbose`   | off        | Enable debug logging                            |


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

## Tags

Each interaction includes Entra ID directory metadata on the licensed user (no extra Graph permission beyond `User.Read.All`):

| Tag key | Source (Graph user property) |
| ------- | ---------------------------- |
| `department` | `department` |
| `job_title` | `jobTitle` |
| `office_location` | `officeLocation` |
| `city` | `city` |
| `country` | `country`, or `usageLocation` when `country` is empty |
| `company_name` | `companyName` |

These tags reflect directory profile fields, not message content; they are not affected by `ANONYMIZE`. Org tags are omitted when the Entra profile field is empty (Nebuly requires string tag values).

## Known limitations

- **Excel & PowerPoint conversation grouping:** Graph returns unstable `sessionId`/`requestId` for these apps that change between turns of the same user session, so their multi-turn conversations can't be grouped — each turn lands as a separate conversation in Nebuly.
- **Copilot Studio agents:** The enterprise interaction export API does not return interactions from agents created in Copilot Studio. But interactions for agents invoked in a Copilot Studio chat are returned.
- **No per-trace token/cost metrics:** Licensed Microsoft 365 Copilot has no per-interaction usage or cost data in Graph (or elsewhere at trace level). Copilot credits for metered experiences (for example Copilot Studio, PAYG chat) are available only as daily per-user or per-agent aggregates in the admin center or platform APIs, not per interaction. This sync sends retrieval (grounding) traces only; LLM token/cost traces are omitted.
- **Graph throttling:** Microsoft documents a limit of 30 requests per second per app per tenant. The default `GRAPH_MAX_REQUESTS_PER_MINUTE=1800` matches that ceiling; parallel sync jobs against the same tenant may receive HTTP 429 responses.

## References

- [Microsoft Teams export content Copilot](https://learn.microsoft.com/en-us/microsoftteams/export-teams-content-copilot)
- [Nebuly ingestion API reference](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2)