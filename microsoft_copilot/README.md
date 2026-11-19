# Microsoft Copilot Enterprise → Nebuly Sync

Sync tool that pulls Copilot Enterprise interactions from Microsoft Graph and POSTs each paired prompt/response to Nebuly's [ingestion endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2).

```
Microsoft Graph (licensed users → interactions) → pair conversion → Nebuly ingestion
```

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- Azure app registration with **Application** permissions (not Delegated), each granted **admin consent**:
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
| `NEBULY_API_KEY`                | yes*     | —                                                                            | Nebuly secret key (*not required when `CSV_EXPORT=true`) |
| `NEBULY_ENDPOINT`               | no       | `https://backend.nebuly.com/event-ingestion/api/v3/events/trace_interaction` | Nebuly ingestion endpoint                                |
| `COPILOT_SKU`                   | no       | `639dec6b-bb19-468b-871c-c5c441c4b0cb`                                       | Microsoft 365 Copilot SKU GUID, or a comma-separated list (see below) |
| `GRAPH_MAX_REQUESTS_PER_MINUTE` | no       | `1800`                                                                       | Rate limit for Graph interaction requests                |
| `ANONYMIZE`                     | no       | `false`                                                                      | Set to `true` to anonymize content in the Nebuly payload |
| `COPILOT_SETTLE_LAG_SECONDS`    | no       | `60`                                                                         | Hold back tail turns until Graph data settles            |
| `AUDIT_ENRICHMENT`              | no       | `false`                                                                      | Enrich turns from Purview Copilot audit logs (opt-in)    |
| `AUDIT_SETTLE_LAG_SECONDS`      | no       | `7200`                                                                       | Extra tail hold-back while audit records arrive (~60–90 min) |
| `AUDIT_POLL_INTERVAL_SECONDS` | no       | `30`                                                                         | Poll interval for async audit log queries                |
| `AUDIT_QUERY_TIMEOUT_SECONDS`   | no       | `3600`                                                                       | Timeout per audit log query                              |
| `CSV_EXPORT`                    | no       | `false`                                                                      | Write interactions to a CSV file instead of POSTing to Nebuly |
| `CSV_EXPORT_PATH`               | no       | `./interactions.csv`                                                         | CSV output file used when `CSV_EXPORT=true`              |


### CLI flags


| Flag          | Default    | Description                                     |
| ------------- | ---------- | ----------------------------------------------- |
| `--from-date` | —          | ISO backfill start date (required on first run) |
| `--to-date`   | —          | ISO end date filter                             |
| `--cache-dir` | `./.cache` | Directory for the sync state database           |
| `--dry-run`   | off        | Fetch interactions without POSTing to Nebuly    |
| `--verbose`   | off        | Enable debug logging                            |


## Caching & resumable sync

State is stored in SQLite at `.cache/<cache-key>/sync_state.db`. The cache key is a short hash of the tenant id, the app (client) id and the destination: the Nebuly endpoint and API key, or the CSV export path when `CSV_EXPORT=true`. Changing any of them starts from an empty cache, so a new Nebuly project or a new CSV file receives the full range instead of skipping ranges already sent elsewhere. Each user has a coverage window `[coverage_from, coverage_until]`. Re-runs skip already-covered date ranges and only fetch backfill or tail intervals.

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

## CSV export

Set `CSV_EXPORT=true` to write each interaction to `CSV_EXPORT_PATH` instead of sending it to Nebuly. One column per Nebuly interaction field (`conversation_id`, `end_user`, `team`, `market`, `tenure`, `time_start`, `time_end`, `input`, `output`, `hide_content`) plus `tags`, `traces` and `user_feedback` as JSON strings, and `anonymize`. `team`, `market` and `tenure` are empty when the user's Entra profile field is empty (see [Tags](#tags)).

- Rows are appended, so repeated runs extend the same file. The header is written only when the file is new or empty. Appending to a CSV produced by an older version with a different header mixes headers, so start a new file.
- CSV runs keep their own coverage (the cache key includes the CSV path), so exporting to CSV never marks a date range as already sent to Nebuly, and a new `CSV_EXPORT_PATH` is exported from scratch.
- `--dry-run` still takes precedence: nothing is written.

```bash
CSV_EXPORT=true poetry run python -m copilot_sync --from-date 2026-06-01 --to-date 2026-06-23
```

## Troubleshooting

- `--dry-run` always needs `--from-date`.
- To re-export to the same CSV file from scratch, delete the file and its cache folder under `.cache/`, or simply export to a new `CSV_EXPORT_PATH`.

## Customizing the payload

Edit `copilot_sync/user_defined.py` for customer-specific tags, traces, and user feedback.

## Licensed user eligibility

User discovery starts from users with the Microsoft 365 Copilot SKU (`COPILOT_SKU`). `COPILOT_SKU` accepts a comma-separated list of SKU GUIDs (for example when the tenant uses Microsoft 365 E7, Copilot Business or an EDU Copilot SKU); users are listed once per SKU and de-duplicated. Find the GUIDs in the Entra admin center under Billing > Licenses, or with `Get-MgSubscribedSku`. For each candidate, the sync reads `assignedLicenses.disabledPlans` and `licenseDetails` and keeps the user only if that SKU has at least one **M365 Copilot** service plan (name prefix `M365_COPILOT_`) with `provisioningStatus` **Success** that is not disabled. Users with an active assignment but 0 enabled plans are skipped before interaction fetch.

If `getAllEnterpriseInteractions` returns **403**, the user id is stored in SQLite (`sync_user_interaction_denied`) and excluded from later sync and audit window planning. The denial is re-checked after 7 days, so a user whose licensing was fixed is retried automatically; a user who is still denied costs one extra Graph call per week. To retry immediately, delete the cache folder under `.cache/` (or the `sync_user_interaction_denied` rows). This should not happen in a regular scenario.

## Purview audit enrichment

When `AUDIT_ENRICHMENT=true`, the sync queries Microsoft Graph [audit log queries](https://learn.microsoft.com/en-us/graph/api/security-auditlogquery-post) for `CopilotInteraction` records, caches them in SQLite, and joins to Graph turns by message id (`Messages[].Id` ↔ interaction `id`). Audit failures are logged and never fail the sync. Settled day-chunks are tracked in SQLite so repeat runs skip re-querying Graph for the same period; chunks in the settle-lag tail are always refreshed.

A turn can match several audit records (for example a Cowork task with steering prompts, or a long agent run that Purview splits into several records). All records matching the turn's response, prompt or follow-up prompt ids are merged into one before enrichment: resources are concatenated in time order, plugins are deduplicated, and model and agent fields come from the first record that has them.

Audit data is retained for up to 180 days (Audit Standard).

| Audit tag | Meaning |
| --------- | ------- |
| `model_name` / `model_provider` / `model_version` | From `ModelTransparencyDetails` when present |
| `app_host` / `app_identity` | Copilot surface (Office, Teams, etc.) |
| `agent_id` / `agent_name` | Agent metadata when present |
| `plugins` | Comma-separated AI system plugins, including agent bookkeeping such as `Skill`, `TaskCreate` and `TaskUpdate` |
| `web_search` | `true` when Bing web search plugin was used |
| `jailbreak_detected` / `xpia_detected` / `sensitive_resource_accessed` | Set to `true` when applicable |
| `tool_approval_denied` | `true` when the final approval event of any agent tool call is `approvalDenied`; omitted otherwise |

Model name coverage varies by scenario (often missing on Auto / M365 Copilot; more common when users pick a model in Copilot Chat).

Audit enrichment adds these traces, in order, after the Graph retrieval traces. Nebuly has no tool trace type, so tool calls are sent as retrieval traces (`source`, `input`, `outputs`):

- **Documents:** one trace per accessed resource, as before.
- **Tool calls:** accessed resources whose `Type` starts with `approval` are agent tool call events, not documents. They are grouped by call id (across all merged records) into one trace per call: `source` is the tool name (for example `outlook_calendar/UpdateEvent`), `input` is empty and `outputs` holds the last approval event (for example `approvalDenied`).
- **Plugin-only tools:** AI system plugins named `mcp__<server>__<tool>` or `core-<Tool>` become `<server>/<tool>` or `core/<Tool>` traces with empty `input` and `outputs`, unless a call trace for the same tool exists. A `_deferred` suffix on the server is dropped so deferred plugins match their calls. `TaskCreate`, `TaskUpdate` and `BuiltIn` never become traces.
- **LLM:** one trace when the audit has a model name.

The audit log does not include tool inputs or outputs, so tool traces carry only the tool name and approval status. Skills appear only as an entry named `Skill` with no skill name, so they are not traced. A plugin entry named `Skill:<name>` or `Skill/<name>` would be sent as `skill/<name>` (a guess at a future format).

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
| `context_type` | Sorted, comma-separated unique `contextType` values from the prompt's `contexts` (for example `TeamsMeeting`). Omitted when the prompt has no contexts. |

These tags reflect directory profile fields, not message content; they are not affected by `ANONYMIZE`. Org tags are omitted when the Entra profile field is empty (Nebuly requires non-null string tag values).

The same profile fields also fill Nebuly's `team`, `market` and `tenure` interaction fields, each omitted when empty:

| Field | Source (Graph user property) |
| ----- | ---------------------------- |
| `team` | `department` |
| `market` | `country`, or `usageLocation` when `country` is empty |
| `tenure` | `employeeHireDate` (ISO `YYYY-MM-DD` in UTC) |

## Known limitations

- **Excel & PowerPoint conversation grouping:** Graph returns unstable `sessionId`/`requestId` for these apps that change between turns of the same user session, so their multi-turn conversations can't be grouped — each turn lands as a separate conversation in Nebuly.
- **Copilot Cowork tasks:** Cowork runs long agent tasks and exports them differently from chat. One answer can be split across several records, cut mid-sentence; these are joined (reasoning summaries starting with a `**Heading**` are dropped). A prompt sent while the agent is still working, before it has produced any text, steers the same task: Graph and Purview both pair the later answers with the latest prompt and give nothing that links each answer to its prompt, so the task is sent as one interaction with all its prompts as input and all its answers as output. Messages that Microsoft glues into one record without a separator get a paragraph break. Tasks that never produce text (for example, only saving a file) are skipped. A Cowork task can run for minutes, so on tail syncs raise `COPILOT_SETTLE_LAG_SECONDS` (or enable audit enrichment, which uses a 2h lag) to avoid sending a task before it finishes.
- **Copilot Studio agents:** The enterprise interaction export API does not return interactions from agents created in Copilot Studio, and Purview audit only carries the agent ID. But interactions for agents invoked in a Copilot Studio chat are returned.
- **Unlicensed and pay-as-you-go Copilot Chat:** Prompt and response text is not available through any API for these users. Purview audit records hold metadata only, no prompt or response text.
- **Deleted users:** Their interactions are missed, because only currently licensed users are enumerated.
- **Audit-only hosts:** Surfaces that appear only in Purview audit (PowerPoint, OneNote, Notebooks, Edge and similar) and Facilitator (`TeamCopilotInteraction`) are not ingested.
- **No per-trace token/cost metrics:** Licensed Microsoft 365 Copilot has no per-interaction usage or cost data in Graph (or elsewhere at trace level). Copilot credits for metered experiences (for example Copilot Studio, PAYG chat) have so far been available as daily per-user or per-agent aggregates; a beta Cost Management API now exposes per-session Copilot credits, but this sync does not use it. Without audit enrichment, the sync sends retrieval (grounding) traces only. With `AUDIT_ENRICHMENT=true`, tool call traces are added (without inputs or outputs) and an LLM trace is added only when Purview provides `ModelName`.
- **Audit latency:** Copilot audit records often land 60–90 minutes after the interaction; use `AUDIT_SETTLE_LAG_SECONDS` (default 2h) on tail syncs.
- **Graph throttling:** Microsoft documents a limit of 30 requests per second per app per tenant. The default `GRAPH_MAX_REQUESTS_PER_MINUTE=1800` matches that ceiling; parallel sync jobs against the same tenant may receive HTTP 429 responses.

## References

- [Microsoft Teams export content Copilot](https://learn.microsoft.com/en-us/microsoftteams/export-teams-content-copilot)
- [Nebuly ingestion API reference](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2)