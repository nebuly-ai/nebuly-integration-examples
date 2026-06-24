# nebuly-integration-examples

Integration examples for syncing AI/LLM interactions to and from the [Nebuly](https://www.nebuly.com/) analytics platform.

## Components

| Folder | Purpose |
|--------|---------|
| [claude_compliance/](claude_compliance/) | Syncs Claude conversations from the Anthropic Compliance API to Nebuly. See its [README](claude_compliance/README.md) for setup and configuration. |
| [microsoft_copilot/](microsoft_copilot/) | Syncs Microsoft Copilot Enterprise interactions from the Microsoft Graph API to Nebuly. See its [README](microsoft_copilot/README.md) for setup and configuration. |
| [export/](export/) | Simple utility — fetches historical interactions from Nebuly with pagination and date ranges, saving to JSON (optional Power BI format) (`export_in_json.py`). |


## Setup

Check each component's README for setup and configuration instructions.
