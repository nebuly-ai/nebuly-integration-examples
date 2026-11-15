# Amazon Bedrock → Nebuly

Two connectors that read what Amazon Bedrock logs in your AWS account and POST each interaction to Nebuly's [trace interaction endpoint](https://docs.nebuly.com/tracking/api-reference/events/post-events-interaction-with-trace-v2). You run them yourself, on a schedule (hourly or daily).

| Connector | Reads | Use it when | Guide |
| --------- | ----- | ----------- | ----- |
| Runtime | Model invocation logs on **S3** | Your application calls models through `bedrock-runtime` (Converse, InvokeModel) | [runtime/README.md](bedrock_sync/runtime/README.md) |
| AgentCore | AgentCore observability logs on **CloudWatch** | Your agents run on Bedrock **AgentCore** | [agentcore/README.md](bedrock_sync/agentcore/README.md) |

Both share the same setup, settings, state file and delivery rules, described below.

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- A Nebuly API key
- AWS credentials that can read the logs (see each guide)

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/bedrock
poetry install
cp .env.example .env
# Edit .env with your keys
```

## Settings shared by both connectors

Where the data is and the secrets come from environment variables (or `.env`). How one run behaves is set with flags.

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `NEBULY_API_KEY` | yes, except with `--dry-run` | none | Nebuly secret key |
| `BEDROCK_ACCOUNT_ID` | yes | none | AWS account id whose logs are read (12 digits) |
| `BEDROCK_REGION` | yes | none | Region whose logs are read, for example `eu-central-1` |
| `USER_HASH_SECRET` | unless `SEND_PLAIN_END_USER=true` | none | Secret key of the end user pseudonym, see [End user](#end-user) |
| `AWS_PROFILE` | no | none | Standard boto3 credentials, used when `AWS_ROLE_ARN` is unset |
| `AWS_ROLE_ARN` | no | none | Read-only role to assume before reading the logs |
| `AWS_EXTERNAL_ID` | no | none | External id for `AWS_ROLE_ARN` |
| `NEBULY_ENDPOINT` | no | Nebuly production ingestion URL | Nebuly ingestion endpoint |
| `SEND_PLAIN_END_USER` | no | `false` | Send the user id as it is instead of its pseudonym |
| `ANONYMIZE` | no | `false` | Sets `anonymize` on the Nebuly payload |

Booleans accept `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`; anything else is an error.

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | none | Start of the first run, or of a backfill (ISO date or timestamp, UTC if no offset) |
| `--to-date` | now | Stop at this time |
| `--state-db` | `.cache/<connector>/state.db` | SQLite state file, see [State](#state) |
| `--content-retention-seconds` | `604800` (7 days) | How long sent or rejected payloads stay in the state |
| `--dry-run` | off | Build the payloads without sending them; the state file is not changed |
| `--verbose` | off | Debug logs. HTTP headers and bodies are never logged |
| `--no-retry-http-429` | off | Treat a 429 as `uncertain` (never sent again) instead of retrying it: use it if your Nebuly endpoint may ingest before rate limiting |

Each connector adds its own flags; run it with `--help` to see them all.

### AWS access

Both use the standard boto3 credential chain (`AWS_PROFILE`, environment keys, SSO, instance or task role). With `AWS_ROLE_ARN` (and `AWS_EXTERNAL_ID` if the role requires it) they assume that role first and renew its credentials during long runs; the identity then needs `sts:AssumeRole` on it. The read permissions are in each guide.

## End user

The user id (`user_id` for the runtime, the actor id for AgentCore) becomes `interaction.end_user`. By default it is sent as an HMAC-SHA256 pseudonym keyed with `USER_HASH_SECRET`: the same user always gets the same pseudonym, and Nebuly never sees the raw id. Keep the secret stable, or the same users will look like new ones. Set `SEND_PLAIN_END_USER=true` to send the raw id.

**One conversation belongs to one user.** A call or trace of a conversation that already belongs to another user is dropped with a warning, never merged.

## Running

```bash
poetry run python -m bedrock_sync.runtime --from-date 2026-11-01   # first run
poetry run python -m bedrock_sync.runtime                          # later runs
```

(`bedrock_sync.agentcore` works the same way, with its required flags.) The first run needs `--from-date`; later runs continue from where the previous one stopped. Schedule it with cron or any job runner, hourly or daily. Two runs on the same state file at the same time are refused.

Logs go to stderr. When the run completes, its summary is printed on stdout as one JSON object. The exit code is:

| Code | Meaning |
| ---- | ------- |
| `0` | Everything closed was delivered (or nothing to do) |
| `1` | The run failed, payloads of this run were rejected or are uncertain, or deliveries wait for a retry (also one asked by an earlier run) |
| `130` | Interrupted (Ctrl-C) |

## State

Each connector keeps one SQLite file per source (`--state-db`). It holds how far the logs were read, what is still being assembled, and the outbox of payloads with their delivery status. Deleting it makes the next run start over from `--from-date`, and interactions already sent would be sent again. A file of another source or of another connector version is refused. Content is kept only as long as needed: open work, then `--content-retention-seconds` after delivery; after that only digests stay, so nothing is sent twice.

## Delivery

Nebuly does not deduplicate, so a POST whose outcome is unknown is never sent again automatically. Every payload ends in one of these states:

| Status | Meaning | Next run |
| ------ | ------- | -------- |
| `sent` | Nebuly confirmed it (2xx) | Never sent again |
| `rejected` | Nebuly refused it (4xx, except 408 and 429); 401/403 also stop the run | Never sent again |
| `uncertain` | Nebuly may have received it (timeout after sending, 408, 5xx, a crash while sending) | Not sent again: check it by hand |
| `pending` | Not sent yet, or a connection failure or 429 (Nebuly did not receive it). A short wait is retried in the same run | Sent again by a later run, no earlier than the `Retry-After` delay (at least 60 seconds, at most 1 day). Until then no payload is sent |

## Customizing the payload

Each connector has a `user_defined.py` with hooks you can edit: `end_user_id`, `build_tags`, `build_segments` (team, market, tenure: the connectors cannot infer them, return them only if you have them), `customize_traces`, `build_user_feedback` and `load_context`, which runs once per run to load your own mappings. See each guide.

## Development

```bash
poetry install --with dev
poetry run pytest
poetry run ruff check --config ../pyproject.toml .
poetry run mypy bedrock_sync tests
```

Tests use only synthetic or anonymized data; they make no AWS or Nebuly calls.
