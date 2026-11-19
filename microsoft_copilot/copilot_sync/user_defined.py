from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .converter import InteractionTurn
    from .models import (
        AiInteraction,
        AISystemPlugin,
        CopilotAuditRecord,
        CopilotUser,
        ModelTransparencyDetail,
    )

_ADAPTIVE_CARD_CONTENT_TYPE = "application/vnd.microsoft.card.adaptive"
_PLACEHOLDER_ATTACHMENT_NAME = "unknown-file-name"
_PLACEHOLDER_ATTACHMENT_URL = "file:///unknown-url"
_TOOL_DENIED_EVENT = "approvalDenied"
_MCP_TOOL_RE = re.compile(r"^mcp__(?P<server>.+?)__(?P<tool>.+)$")
_CORE_TOOL_RE = re.compile(r"^core-(?P<tool>.+)$")
# Forward-compatible guess: today the Skill entry carries no skill name.
_SKILL_RE = re.compile(r"^Skill[:/](?P<name>.+)$")


def _non_empty_tags(fields: dict[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in fields.items() if value}


def _format_hire_date(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).date().isoformat()


def _model_transparency_tags(
    model: ModelTransparencyDetail | None,
) -> dict[str, str]:
    if model is None:
        return {}
    return _non_empty_tags(
        {
            "model_name": model.model_name,
            "model_provider": model.model_provider_name,
            "model_version": model.model_version,
        },
    )


def _audit_tags(audit: CopilotAuditRecord) -> dict[str, str]:
    tags = _model_transparency_tags(audit.primary_model)
    tags.update(
        _non_empty_tags(
            {
                "app_host": audit.app_host,
                "app_identity": audit.app_identity,
                "agent_id": audit.agent_id,
                "agent_name": audit.agent_name,
                "plugins": _plugin_names(audit),
            },
        ),
    )
    tags["web_search"] = "true" if audit.web_search_used else "false"
    if audit.jailbreak_detected:
        tags["jailbreak_detected"] = "true"
    if audit.xpia_detected:
        tags["xpia_detected"] = "true"
    if _sensitive_resource_accessed(audit):
        tags["sensitive_resource_accessed"] = "true"
    if any(events[-1] == _TOOL_DENIED_EVENT for _, events in _tool_calls(audit)):
        tags["tool_approval_denied"] = "true"
    return tags


def _context_types(prompt: AiInteraction) -> str | None:
    types = {c.context_type for c in prompt.contexts if c.context_type}
    return ",".join(sorted(types)) or None


def build_tags(
    turn: InteractionTurn,
    user: CopilotUser,
    audit: CopilotAuditRecord | None = None,
) -> dict[str, str]:
    prompt = turn.prompt
    effective = turn.effective_response
    raw: dict[str, str | None] = {
        "app_class": prompt.app_class,
        "conversation_type": prompt.conversation_type,
        "locale": prompt.locale,
        "context_type": _context_types(prompt),
        "session_id": prompt.session_id,
        "request_id": prompt.request_id,
        "copilot_app": effective.sender_model_name if effective else None,
        "department": user.department,
        "job_title": user.job_title,
        "office_location": user.office_location,
        "city": user.city,
        "country": user.country or user.usage_location,
        "company_name": user.company_name,
        "employee_hire_date": _format_hire_date(user.employee_hire_date),
    }
    if audit is not None:
        raw.update(_audit_tags(audit))
    return {key: value for key, value in raw.items() if value is not None}


def build_segments(user: CopilotUser) -> dict[str, str]:
    """Nebuly ``team``/``market``/``tenure`` fields from the Entra profile,
    omitted when the profile field is empty."""
    return _non_empty_tags(
        {
            "team": user.department,
            "market": user.country or user.usage_location,
            "tenure": _format_hire_date(user.employee_hire_date),
        },
    )


def build_traces(
    turn: InteractionTurn,
    *,
    audit: CopilotAuditRecord | None = None,
    user_input: str = "",
    assistant_output: str = "",
) -> list[dict[str, Any]]:
    effective = turn.effective_response
    if effective is None:
        return []
    traces = _build_retrieval_traces(effective)
    graph_sources = _graph_retrieval_sources(effective)
    if audit is not None:
        traces.extend(
            _audit_retrieval_traces(audit, skip_sources=graph_sources),
        )
        traces.extend(_tool_traces(audit))
        model = audit.primary_model
        if model is not None and model.model_name:
            traces.append(
                {
                    "model": model.model_name,
                    "messages": [{"role": "user", "content": user_input}],
                    "output": assistant_output,
                },
            )
    return traces


def build_user_feedback(turn: InteractionTurn) -> list[dict[str, Any]]:  # noqa: ARG001
    return []


def _plugin_names(audit: CopilotAuditRecord) -> str | None:
    names: list[str] = []
    for plugin in audit.ai_system_plugins:
        label = plugin.name or plugin.plugin_id
        if label:
            names.append(label)
    if not names:
        return None
    return ",".join(names)


def _sensitive_resource_accessed(audit: CopilotAuditRecord) -> bool:
    return any(
        resource.sensitivity_label_id
        for resource in audit.accessed_resources
        if resource.sensitivity_label_id
    )


def _graph_retrieval_sources(final: AiInteraction) -> set[str]:
    sources: set[str] = set()
    for att in final.attachments:
        if att.content_type == _ADAPTIVE_CARD_CONTENT_TYPE:
            continue
        if att.content_url:
            sources.add(att.content_url)
        if att.name:
            sources.add(att.name)
    for link in final.links:
        if link.link_url:
            sources.add(link.link_url)
    return sources


def _audit_retrieval_traces(
    audit: CopilotAuditRecord,
    *,
    skip_sources: set[str],
) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []
    for resource in audit.accessed_resources:
        if resource.is_tool_call_event:
            continue
        name = resource.name or resource.resource_id or "resource"
        site = resource.site_url or name
        if site in skip_sources or name in skip_sources:
            continue
        traces.append(
            {
                "source": name,
                "input": site,
                "outputs": [name],
            },
        )
    return traces


def _tool_calls(audit: CopilotAuditRecord) -> list[tuple[str, list[str]]]:
    """Tool calls as (name, approval event types in order), by first appearance."""
    calls: dict[str, tuple[str, list[str]]] = {}
    for resource in audit.accessed_resources:
        if not resource.is_tool_call_event:
            continue
        call_id = resource.resource_id or resource.name or ""
        _, events = calls.setdefault(call_id, (resource.name or call_id, []))
        events.append(resource.resource_type or "")
    return list(calls.values())


def _tool_source(plugin: AISystemPlugin) -> str | None:
    """Normalize a plugin entry to the `<server>/<tool>` name used by call events.

    Agent bookkeeping (TaskCreate, TaskUpdate, a bare Skill) and non-tool
    plugins such as BuiltIn match no pattern and yield None.
    """
    for label in (plugin.name, plugin.plugin_id):
        if not label:
            continue
        if match := _MCP_TOOL_RE.match(label):
            server = match["server"].removesuffix("_deferred")
            return f"{server}/{match['tool']}"
        if match := _CORE_TOOL_RE.match(label):
            return f"core/{match['tool']}"
        if match := _SKILL_RE.match(label):
            return f"skill/{match['name']}"
    return None


def _tool_traces(audit: CopilotAuditRecord) -> list[dict[str, Any]]:
    """Retrieval traces for agent tool calls.

    One trace per approval-tracked call with its last event as output, then one
    per plugin-declared tool that has no call trace. The audit log has no tool
    inputs or outputs.
    """
    traces: list[dict[str, Any]] = []
    call_sources: set[str] = set()
    for source, events in _tool_calls(audit):
        call_sources.add(source)
        traces.append({"source": source, "input": "", "outputs": [events[-1]]})
    for plugin in audit.ai_system_plugins:
        tool_source = _tool_source(plugin)
        if tool_source is None or tool_source in call_sources:
            continue
        call_sources.add(tool_source)
        traces.append({"source": tool_source, "input": "", "outputs": []})
    return traces


def _is_placeholder_attachment(name: str | None, content_url: str | None) -> bool:
    """Cowork exports unresolved file refs as a fixed name and URL."""
    return (
        name == _PLACEHOLDER_ATTACHMENT_NAME
        or content_url == _PLACEHOLDER_ATTACHMENT_URL
    )


def _build_retrieval_traces(final: AiInteraction) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []

    for att in final.attachments:
        if att.content_type == _ADAPTIVE_CARD_CONTENT_TYPE:
            continue
        if _is_placeholder_attachment(att.name, att.content_url):
            continue
        source = att.name or att.content_url or "attachment"
        traces.append(
            {
                "source": source,
                "input": att.content_url or source,
                "outputs": [att.name or source],
            },
        )

    traces.extend(
        {
            "source": link.link_url,
            "input": link.link_url,
            "outputs": [link.display_name or link.link_url],
        }
        for link in final.links
        if link.link_url
    )

    for ment in final.mentions:
        text = ment.get("mentionText") or str(ment.get("id") or "")
        if not text:
            continue
        traces.append({"source": text, "input": text, "outputs": []})

    return traces
