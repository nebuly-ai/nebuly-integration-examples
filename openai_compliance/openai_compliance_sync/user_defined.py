from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .chat_converter import AssembledInteraction
    from .models import CodexLogRecord


def _drop_none_tags(tags: dict[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in tags.items() if value is not None}


def build_chat_tags(interaction: AssembledInteraction) -> dict[str, str]:
    user = interaction.user
    conversation = user.conversation
    author = user.message.author
    tags: dict[str, str | None] = {
        "chatgpt source": "chat",
        "conversation id": conversation.id,
        "conversation title": conversation.title,
        "gpt name": conversation.gpt_name,
        "project name": conversation.project_name,
        "client type": author.client_type if author else None,
        "temporary chat": (
            str(conversation.is_temporary_chat).lower()
            if conversation.is_temporary_chat is not None
            else None
        ),
        "skills used": ",".join(author.skills_used)
        if author and author.skills_used
        else None,
        "tools used": ",".join(author.tools_used)
        if author and author.tools_used
        else None,
        "model": interaction.llm_model,
        "models": interaction.models_tag,
    }
    return _drop_none_tags(tags)


def build_chat_traces(
    interaction: AssembledInteraction,
    retrieval_traces: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    from .chat_converter import truncate_text, user_text  # noqa: PLC0415

    traces: list[dict[str, Any]] = list(retrieval_traces)
    traces.append(
        {
            "model": interaction.llm_model,
            "messages": [
                {"role": "user", "content": truncate_text(user_text(interaction.user))}
            ],
            "output": truncate_text(interaction.assistant_output_text()),
        }
    )
    return traces


def build_chat_user_feedback(
    _interaction: AssembledInteraction,
) -> list[dict[str, Any]]:
    return []


def build_codex_tags(record: CodexLogRecord) -> dict[str, str]:
    details = record.event_details
    tags: dict[str, str | None] = {
        "chatgpt source": "codex",
        "client id": record.client_id,
        "model": details.model if details else None,
        "reasoning effort": details.reasoning_effort if details else None,
        "service tier": details.service_tier if details else None,
        "environment id": details.environment_id if details else None,
    }
    return _drop_none_tags(tags)


def build_codex_traces(record: CodexLogRecord) -> list[dict[str, Any]]:
    from .chat_converter import truncate_text  # noqa: PLC0415
    from .codex_converter import prompt_text, response_text  # noqa: PLC0415

    details = record.event_details
    if details is None:
        return []
    usage = details.token_usage
    return [
        {
            "model": details.model or "unknown",
            "messages": [
                {"role": "user", "content": truncate_text(prompt_text(details))}
            ],
            "output": truncate_text(response_text(details)),
            "input_tokens": usage.input_tokens if usage and usage.input_tokens else 0,
            "output_tokens": usage.output_tokens
            if usage and usage.output_tokens
            else 0,
        }
    ]


def build_codex_user_feedback(_record: CodexLogRecord) -> list[dict[str, Any]]:
    return []
