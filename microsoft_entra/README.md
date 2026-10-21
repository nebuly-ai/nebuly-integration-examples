# Microsoft Entra → directory extract

Exports Microsoft Entra users from Microsoft Graph for the [Claude Compliance sync](../claude_compliance/README.md).

This package is also a Poetry dependency of `claude_compliance` (`poetry install --with entra`). With `ENTRA_FETCH=true`, the sync calls Graph at startup and does not need a file on disk.

For the file-based flow, this script writes `entra.json`; set `ENTRA_EXTRACT_PATH` in the Compliance sync. It does not send anything to Nebuly. Run Entra setup **before** the first Compliance sync so interactions are tagged on the first POST.

```
Microsoft Graph (users) → entra.json → Claude Compliance sync   (ENTRA_EXTRACT_PATH)
Microsoft Graph (users) ─────────────────→ Claude Compliance sync   (ENTRA_FETCH=true)
```

The JSON shape is documented in [README_ENTRA_JOIN.md](../claude_compliance/README_ENTRA_JOIN.md).

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