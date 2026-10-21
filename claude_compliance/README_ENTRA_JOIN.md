# Claude Compliance and Entra extracts

This optional join matches Claude activity to a Microsoft Entra directory, then sends each interaction to Nebuly once.

The running sync reads Claude from the Compliance API. The Claude section below is that same data, written as JSON. For Entra you can either:

1. **One step:** set `ENTRA_FETCH=true` in `claude_compliance/.env` (after `poetry install --with entra`) with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`. The sync pulls users from Microsoft Graph at startup and keeps them in memory.
2. **File-based:** produce `entra.json` with the [microsoft_entra](../microsoft_entra/README.md) script (`poetry run python -m entra_export`), then set `ENTRA_EXTRACT_PATH`.

Use one option only (`ENTRA_FETCH` and `ENTRA_EXTRACT_PATH` are mutually exclusive). The sync attaches directory tags before the POST. `end_user` stays the Claude user id. Run Entra setup before the first Compliance sync: a later run does not re-send interactions that were already posted.

Without either option, payloads are unchanged: Claude user id, no directory tags.

## Join

Match on email, compared case-insensitively after trimming whitespace.

| Side | Email used |
| ---- | ---------- |
| Claude chat | `user.email_address` on the chat |
| Claude local or remote session | Email on the **session listing**. The messages endpoint often returns `email_address: null`, so the listing value is the one that counts. For a remote session with no owner, `started_by_user.email_address` is used. |
| Entra | `mail`, then `userPrincipalName` |

An interaction with no email, or an email that is not in the Entra file, is still sent. It just has no directory tags.

If the Entra file lists the same email twice, the first row wins.

## Claude Compliance extract

One JSON object. It is the slice of Compliance data this sync turns into Nebuly interactions: users (for the email), chats, local sessions, and remote sessions. Message text is included because it becomes `input` and `output`.

```json
{
  "organization_uuid": "91012d09-e48b-438e-a489-1bebfd8fa6f9",
  "users": [
    { "id": "user_01", "email_address": "ada@example.com" }
  ],
  "chats": [
    {
      "id": "chat_01",
      "name": "Design review",
      "href": "https://claude.ai/chat/chat_01",
      "model": "claude-opus-4-8",
      "organization_uuid": "91012d09-e48b-438e-a489-1bebfd8fa6f9",
      "project_id": "proj_01",
      "deleted_at": null,
      "user": { "id": "user_01", "email_address": "ada@example.com" },
      "messages": [
        {
          "id": "msg_user_01",
          "role": "user",
          "created_at": "2026-03-01T10:00:00Z",
          "content": [{ "type": "text", "text": "What changed?" }]
        },
        {
          "id": "msg_asst_01",
          "role": "assistant",
          "created_at": "2026-03-01T10:00:05Z",
          "content": [{ "type": "text", "text": "The header." }]
        }
      ]
    }
  ],
  "local_sessions": [
    {
      "id": "clls_01",
      "organization_uuid": "91012d09-e48b-438e-a489-1bebfd8fa6f9",
      "workspace_id": "ws_01",
      "product_surface": "claude_code",
      "truncated": false,
      "created_at": "2026-03-01T11:00:00Z",
      "updated_at": "2026-03-01T11:05:00Z",
      "user": { "id": "user_01", "email_address": "ada@example.com" },
      "messages": [
        {
          "id": "prompt_01",
          "role": "user",
          "created_at": "2026-03-01T11:00:00Z",
          "content": [{ "type": "text", "text": "Fix the test." }]
        },
        {
          "id": "reply_01",
          "role": "assistant",
          "created_at": "2026-03-01T11:00:08Z",
          "model": "claude-sonnet",
          "content": [{ "type": "text", "text": "Updated the assertion." }]
        }
      ]
    }
  ],
  "remote_sessions": [
    {
      "id": "cse_01",
      "organization_uuid": "91012d09-e48b-438e-a489-1bebfd8fa6f9",
      "status": "completed",
      "product_surface": "cowork_remote",
      "agent_id": null,
      "claude_project_id": "proj_01",
      "created_at": "2026-03-01T12:00:00Z",
      "updated_at": "2026-03-01T12:10:00Z",
      "user": { "id": "user_01", "email_address": "ada@example.com" },
      "started_by_user": { "id": "user_01", "email_address": "ada@example.com" },
      "messages": [
        {
          "id": "prompt_02",
          "role": "user",
          "created_at": "2026-03-01T12:00:00Z",
          "content": [{ "type": "text", "text": "Summarize the doc." }]
        },
        {
          "id": "reply_02",
          "role": "assistant",
          "created_at": "2026-03-01T12:00:20Z",
          "content": [{ "type": "text", "text": "Three points." }]
        }
      ]
    }
  ]
}
```

`product_surface` on a session is what distinguishes Claude Code, Cowork, Claude in Chrome, Claude Science, and the Microsoft 365 surfaces. Chats do not carry that split. Deleted chats (`deleted_at` set) and chats with `user: null` are not sent.

`end_user` on the Nebuly interaction is `user.id` (or `started_by_user.id` when a remote session has no owner). It is not the email and not the Entra object id.

## Entra extract

A JSON object with a `users` array, or a bare array of the same user objects. Field names match Microsoft Graph. Unknown fields are ignored. The [microsoft_entra](../microsoft_entra/README.md) script writes this object from `GET /users`.

```json
{
  "users": [
    {
      "id": "11111111-2222-3333-4444-555555555555",
      "mail": "ada@example.com",
      "userPrincipalName": "ada@example.com",
      "department": "Engineering",
      "jobTitle": "Staff Engineer",
      "officeLocation": "Milan",
      "city": "Milan",
      "country": "Italy",
      "usageLocation": "IT",
      "companyName": "Example",
      "employeeHireDate": "2020-03-15T00:00:00Z"
    }
  ]
}
```

Each matched interaction gets `team`, `market`, and `tenure` on the interaction itself, plus these Nebuly tags. The tag names match the Copilot integration. Null and blank values are omitted, so a user with only `department` set does not get empty tags for the rest.

| Entra field | Nebuly interaction field |
| ----------- | ------------------------ |
| `department` | `team` |
| `country`, else `usageLocation` | `market` |
| `employeeHireDate` | `tenure` (`YYYY-MM-DD`, UTC). Nebuly turns the date into a tenure band. |

| Entra field | Nebuly tag | Notes |
| ----------- | ---------- | ----- |
| `department` | `department` | |
| `jobTitle` | `job_title` | |
| `officeLocation` | `office_location` | |
| `city` | `city` | |
| `country`, else `usageLocation` | `country` | `usageLocation` is used only when `country` is empty |
| `companyName` | `company_name` | |
| `employeeHireDate` | `employee_hire_date` | UTC calendar date, `YYYY-MM-DD` |

These tags are added on the same payload as the Claude tags (`claude source`, `claude chat-id` or `claude session-id`, `product surface`, and the rest). The script records the interaction as sent after that one POST, so a later run of this same script does not send it again.
