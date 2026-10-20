# Microsoft Entra → directory extract

Exports Microsoft Entra users from Microsoft Graph into the JSON file the [Claude Compliance sync](../claude_compliance/README.md) reads through `ENTRA_EXTRACT_PATH`.

This script does not send anything to Nebuly. It only writes the directory file. Run it **before** the first Claude Compliance sync. The Compliance sync attaches directory tags on the same POST as the interaction, and a later run of that sync does not send an interaction again.

```
Microsoft Graph (users) → entra.json → Claude Compliance sync
```

The file shape is defined in [EXTRACTS.md](../claude_compliance/EXTRACTS.md).

## Prerequisites

- Python ≥ 3.12
- [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
- An Entra app registration with the application permission **`User.Read.All`**, and admin consent for the tenant

## Setup

```bash
git clone https://github.com/nebuly-ai/nebuly-integration-examples.git
cd nebuly-integration-examples/microsoft_entra
poetry install
cp .env.example .env
# Edit .env with AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET
```

## Configuration

### Environment variables

| Variable | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `AZURE_TENANT_ID` | yes | — | Directory (tenant) ID |
| `AZURE_CLIENT_ID` | yes | — | Application (client) ID |
| `AZURE_CLIENT_SECRET` | yes | — | Client secret **value** |
| `AZURE_LOGIN_BASE_URL` | no | `https://login.microsoftonline.com` | Token endpoint host |
| `GRAPH_BASE_URL` | no | `https://graph.microsoft.com` | Graph host |

### CLI flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--output` | `entra.json` | Where to write the extract |
| `--dry-run` | off | List users and print the count without writing a file |
| `--verbose` | off | Debug logging |

## Running

```bash
poetry run python -m entra_export --output entra.json
poetry run python -m entra_export --dry-run --verbose
```

Then point the Claude Compliance sync at the file:

```bash
# in claude_compliance/.env
ENTRA_EXTRACT_PATH=../microsoft_entra/entra.json
```

Re-running overwrites the file. Users are selected with `id`, `mail`, `userPrincipalName`, `department`, `jobTitle`, `officeLocation`, `city`, `country`, `usageLocation`, `companyName`, and `employeeHireDate`. The Claude sync matches on `mail`, then `userPrincipalName`.

`entra.json` is a directory export. Do not commit it.

### Against the local Entra mock (nebuly-ingestion-worker)

The mock uses the same seed as the Claude Compliance mock, so the exported emails match that mock's users (one user is left unmatched). Any tenant, client id, and secret are accepted.

```bash
# Terminal 1 — Entra mock
cd path/to/nebuly-ingestion-worker
poetry run uvicorn mocks.entra.app:app --port 8089

# Terminal 2 — export
cd microsoft_entra
AZURE_TENANT_ID=tenant \
AZURE_CLIENT_ID=client \
AZURE_CLIENT_SECRET=secret \
AZURE_LOGIN_BASE_URL=http://localhost:8089 \
GRAPH_BASE_URL=http://localhost:8089 \
poetry run python -m entra_export --output entra.json
```
