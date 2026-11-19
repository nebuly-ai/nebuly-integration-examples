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
| `AWS_PROFILE` | no | none | Standard boto3 credentials, used when `AWS_ROLE_ARN` is unset |
| `AWS_ROLE_ARN` | no | none | Read-only role to assume before reading the logs |
| `AWS_EXTERNAL_ID` | no | none | External id for `AWS_ROLE_ARN` |
| `NEBULY_ENDPOINT` | no | Nebuly production ingestion URL | Nebuly ingestion endpoint |
| `PSEUDONYMIZE_END_USER` | no | `false` | Send a pseudonym of the user id instead of the id itself, see [End user](#end-user) |
| `USER_HASH_SECRET` | if `PSEUDONYMIZE_END_USER=true` | none | Secret key of the end user pseudonym |
| `ANONYMIZE` | no | `false` | Sets `anonymize` on the Nebuly payload |

Booleans accept `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`; anything else is an error.

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--from-date` | none | Start of the first run, or of a backfill (ISO date or timestamp, UTC if no offset) |
| `--to-date` | now | Stop at this time |
| `--retry-given-up INTERACTION_KEY` | none | Requeue a retained payload that reached the retry limit, using the key from its warning |
| `--state-db` | `.cache/<connector>/state.db` | SQLite state file, see [State](#state) |
| `--content-retention-seconds` | `604800` (7 days) | How long sent or rejected payloads stay in the state |
| `--history-retention-seconds` | `2592000` (30 days) | How long the record of finished interactions stays in the state; not shorter than `--content-retention-seconds` |
| `--dry-run` | off | Build the payloads without sending them; the state file is not changed |
| `--verbose` | off | Debug logs. HTTP headers and bodies are never logged |

Each connector adds its own flags; run it with `--help` to see them all.

### AWS access

Both use the standard boto3 credential chain (`AWS_PROFILE`, environment keys, SSO, instance or task role). With `AWS_ROLE_ARN` (and `AWS_EXTERNAL_ID` if the role requires it) they assume that role first and renew its credentials during long runs; the identity then needs `sts:AssumeRole` on it. The read permissions are in each guide.

## End user

The user id (`user_id` for the runtime, the actor id for AgentCore) becomes `interaction.end_user`, sent as it is by default. Set `PSEUDONYMIZE_END_USER=true` to send an HMAC-SHA256 pseudonym keyed with `USER_HASH_SECRET` instead: the same user always gets the same pseudonym, and Nebuly never sees the raw id. Use it if the id holds personal data such as an email. Choose before the first run and keep the secret stable: Nebuly matches interactions on the end user, so changing either later makes the same users look like new ones.

**A conversation per user.** Nebuly identifies a conversation by its id together with its end user. If the same conversation id (runtime) or session (AgentCore) has calls or traces of several users, each user's are sent as their own conversation, with a warning in the log and a counter in the summary: nothing is dropped, but it usually means the ids are not set as intended.

## Running

```bash
poetry run python -m bedrock_sync.runtime --from-date 2026-11-01   # first run
poetry run python -m bedrock_sync.runtime                          # later runs
```

(`bedrock_sync.agentcore` works the same way, with its required flags.) The first run needs `--from-date`; later runs continue from where the previous one stopped. A later run with `--from-date` is a backfill: it reads again from that date, skipping the interactions already sent. Schedule it with cron or any job runner, hourly or daily. Two runs on the same state file at the same time are refused. The state file path is relative to the folder the command runs from: in cron or a job runner pass an absolute `--state-db`, and in a container keep it on a persistent volume. Each run logs the path it uses, and warns when the file does not exist yet.

Logs go to stderr. When the run completes, its summary is printed on stdout as one JSON object. The exit code is:

| Code | Meaning |
| ---- | ------- |
| `0` | Everything closed was delivered (or nothing to do) |
| `1` | The run failed, payloads of this run were rejected, deliveries wait for a retry (also one asked by an earlier run), an interaction could not be converted (for example an error in a `user_defined.py` hook), a runtime log file or line could not be read, or AgentCore traces expired before they could be sent |
| `130` | Interrupted (Ctrl-C) |

If the run fails with an unexpected error, the log shows its type and where it happened, for example `Sync failed (KeyError at bedrock_sync/runtime/parser.py:142 in _usage)`. The error message is never logged, because it may hold conversation text or secrets. With `--verbose` the log also shows the full stack (files, lines and functions only): include it when you contact support.

## State

Each connector keeps one SQLite file per source (`--state-db`). It holds how far the logs were read, what is still being assembled, and the outbox of payloads with their delivery status. Deleting it makes the next run start over from `--from-date` and send again the interactions already sent: Nebuly updates them, but it is wasted work. The default path is relative to the working directory, so a job started from another folder, or a container without a persistent volume, would start over with an empty file: use an absolute path there. Each run logs the absolute path it uses and warns when the file does not exist yet. New state files are created with owner-only access. Existing state files can contain prompts and responses. Restrict their permissions and keep the state directory private. A file of another source or of another connector version is refused. Content is kept only as long as needed: open work, then `--content-retention-seconds` after delivery; after that only digests stay, so logs read again are recognized. After `--history-retention-seconds` (30 days, counted from the delivery) the digests are deleted too, so the file does not grow forever; an interaction still waiting for delivery is kept. A backfill over that older time sends those interactions again: with the same input, user and start time, Nebuly updates them instead of adding copies.

## Delivery

When an interaction with the same input, end user and start time arrives again, Nebuly updates the existing one instead of adding a copy. The connectors retry unconfirmed payloads according to the rules below. AgentCore can also update a previously sent interaction with late spans while the original content is retained and those three deduplication fields stay the same. Every payload is in one of these states:

| Status | Meaning | Next run |
| ------ | ------- | -------- |
| `sent` | Nebuly confirmed it (2xx) | AgentCore may send a correction when it receives late spans |
| `rejected` | Nebuly refused it (4xx, except 401, 403, 404, 405, 408 and 429), or kept failing it for 7 days | The run logs an opaque interaction key. A payload that reached the retry limit can be requeued explicitly while its content is retained. A `413` means the payload is too large: texts are cut at 8000 characters to avoid it, lower `MAX_CONTENT_CHARS` in `user_defined.py` if it still happens |
| `pending` | Not sent yet, or not confirmed | Sent again, see below |

A payload that was not confirmed stays `pending`:

| Cause | When it is sent again |
| ----- | --------------------- |
| Connection failure, 429 | Short waits in the same run; otherwise by a later run, after the `Retry-After` delay (at least 60 seconds, at most 1 day) |
| Timeout after sending, 408, 5xx, a crash while sending | By a later run, at least 60 seconds later (or after `Retry-After`), never at once, so two copies do not reach Nebuly at the same time. Each new failure of the same payload waits longer, as long as it has been failing (at most 6 hours); after 7 days of failures it is given up as `rejected`, and the warning includes the interaction key |
| Wrong key (401, 403), wrong endpoint (404, 405) or a redirect (3xx) | By the next run, once the setting is fixed: the run fails with an error naming it |

After resolving the failure, run the connector with the same state file and `--retry-given-up INTERACTION_KEY` using the key from the warning. This attempts only the original retained payload and skips reading the source. Run it without `--from-date`, `--to-date`, or `--dry-run`. If the item needs another retry, the regular schedule picks it up. Ordinary 4xx rejections and compacted payloads are ineligible. A regular backfill leaves these rejections in place while their delivery history is retained.

A connection failure, a 429 or a wrong setting stops the run at once, and no payload is sent until the delay has passed: an outage costs one attempt. A timeout or a 5xx may come from one bad payload, so the run goes on with the next one and stops at the second failure in a row; payloads that keep failing go after the others, so one of them never holds the rest.

## Customizing the payload

Each connector has a `user_defined.py` with hooks you can edit: `end_user_id`, `build_tags`, `build_segments` (team, market, tenure: the connectors cannot infer them, return them only if you have them), `customize_traces`, `model_name` (runtime only), `build_user_feedback` and `load_context`, which runs once per run to load your own mappings. See each guide.

## Development

```bash
poetry install --with dev
poetry run pytest
poetry run ruff check --config ../pyproject.toml .
poetry run mypy bedrock_sync tests
```

Tests use only synthetic or anonymized data; they make no AWS or Nebuly calls.
