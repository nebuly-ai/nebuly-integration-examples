# nebuly-integration-examples

Integration examples for syncing AI/LLM interactions to and from the  
[Nebuly](https://www.nebuly.com/) analytics platform.

## Components


| Folder                                   | Purpose                                                                                                                                                        |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [claude_compliance/](claude_compliance/) | Syncs Claude conversations from the Anthropic Compliance API to Nebuly. See its [README](claude_compliance/README.md) for setup and configuration.             |
| [api/](api/)                             | Simple utility — uploads JSONL trace interactions to Nebuly's ingestion API in batches (`upload_json.py`).                                                     |
| [export/](export/)                       | Simple utility — fetches historical interactions from Nebuly with pagination and date ranges, saving to JSON (optional Power BI format) (`export_in_json.py`). |
| [langfuse/](langfuse/)                   | Example script — reads Langfuse traces/observations and forwards them to Nebuly (`langfuse_api.py`).                                                           |
| [microsoft_copilot/](microsoft_copilot/) | Example script — exports Microsoft Copilot Enterprise interactions via the Microsoft Graph beta API to Nebuly (`copilot_enterprise.py`).                       |


## Setup

This repo uses [Poetry](https://python-poetry.org/). Install dependencies and copy
the example environment file:

```bash
poetry install
cp .env.example .env  # then fill in API keys / credentials
```

Each component reads its configuration from environment variables — see the
individual scripts (or `claude_compliance/`'s README) for the specific variables
required.