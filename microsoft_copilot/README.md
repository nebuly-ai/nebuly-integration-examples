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
  - `AuditLogsQuery.Read.All` (optional; Purview audit enrichment)

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
| `COPILOT_SETTLE_LAG_SECONDS`    | no       | `60`                                                                         | Hold back tail turns until Graph data settles            |
| `AUDIT_ENRICHMENT`              | no       | `false`                                                                      | Enrich turns from Purview Copilot audit logs (opt-in)    |
| `AUDIT_SETTLE_LAG_SECONDS`      | no       | `7200`                                                                       | Extra tail hold-back while audit records arrive (~60–90 min) |
| `AUDIT_POLL_INTERVAL_SECONDS` | no       | `30`                                                                         | Poll interval for async audit log queries                |
| `AUDIT_QUERY_TIMEOUT_SECONDS`   | no       | `3600`                                                                       | Timeout per audit log query                              |


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

## Licensed user eligibility

User discovery starts from users with the Microsoft 365 Copilot SKU (`COPILOT_SKU`). For each candidate, the sync reads `assignedLicenses.disabledPlans` and `licenseDetails` and keeps the user only if that SKU has at least one **M365 Copilot** service plan (name prefix `M365_COPILOT_`) with `provisioningStatus` **Success** that is not disabled. Users with an active assignment but 0 enabled plans are skipped before interaction fetch.

If `getAllEnterpriseInteractions` returns **403**, the user id is stored in SQLite (`sync_user_interaction_denied`) and excluded from later sync and audit window planning until you delete that row (for example after fixing licensing). This should not happen in a regular scenario.

## Purview audit enrichment

When `AUDIT_ENRICHMENT=true`, the sync queries Microsoft Graph [audit log queries](https://learn.microsoft.com/en-us/graph/api/security-auditlogquery-post) for `CopilotInteraction` records, caches them in SQLite, and joins to Graph turns by message id (`Messages[].Id` ↔ interaction `id`). Audit failures are logged and never fail the sync. Settled day-chunks are tracked in SQLite so repeat runs skip re-querying Graph for the same period; chunks in the settle-lag tail are always refreshed.

Audit data is retained for up to 180 days (Audit Standard).

| Audit tag | Meaning |
| --------- | ------- |
| `model_name` / `model_provider` / `model_version` | From `ModelTransparencyDetails` when present |
| `app_host` / `app_identity` | Copilot surface (Office, Teams, etc.) |
| `agent_id` / `agent_name` | Agent metadata when present |
| `plugins` | Comma-separated AI system plugins |
| `web_search` | `true` when Bing web search plugin was used |
| `jailbreak_detected` / `xpia_detected` / `sensitive_resource_accessed` | Set to `true` when applicable |

Model name coverage varies by scenario (often missing on Auto / M365 Copilot; more common when users pick a model in Copilot Chat).

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
| `employee_hire_date` | `employeeHireDate` (ISO `YYYY-MM-DD` in UTC) |

These tags reflect directory profile fields, not message content; they are not affected by `ANONYMIZE`. Org tags are omitted when the Entra profile field is empty (Nebuly requires non-null string tag values).

## Known limitations

- **Excel & PowerPoint conversation grouping:** Graph returns unstable `sessionId`/`requestId` for these apps that change between turns of the same user session, so their multi-turn conversations can't be grouped — each turn lands as a separate conversation in Nebuly.
- **Copilot Studio agents:** The enterprise interaction export API does not return interactions from agents created in Copilot Studio. But interactions for agents invoked in a Copilot Studio chat are returned.
- **No per-trace token/cost metrics:** Licensed Microsoft 365 Copilot has no per-interaction usage or cost data in Graph (or elsewhere at trace level). Copilot credits for metered experiences (for example Copilot Studio, PAYG chat) are available only as daily per-user or per-agent aggregates in the admin center or platform APIs, not per interaction. Without audit enrichment, the sync sends retrieval (grounding) traces only. With `AUDIT_ENRICHMENT=true`, an LLM trace is added only when Purview provides `ModelName`.
- **Audit latency:** Copilot audit records often land 60–90 minutes after the interaction; use `AUDIT_SETTLE_LAG_SECONDS` (default 2h) on tail syncs.
- **Graph throttling:** Microsoft documents a limit of 30 requests per second per app per tenant. The default `GRAPH_MAX_REQUESTS_PER_MINUTE=1800` matches that ceiling; parallel sync jobs against the same tenant may receive HTTP 429 responses.

## References

- [Microsoft Teams export content Copilot](https://learn.microsoft.com/en-us/microsoftteams/export-teams-content-copilot)
- [Nebuly ingestion API reference](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2)