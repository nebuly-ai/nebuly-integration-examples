from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .chat_converter import AssembledInteraction
    from .codex_converter import CodexTurn
    from .models import CodexTurnRecord


def _drop_none_tags(tags: dict[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in tags.items() if value is not None}


def _assistant_tool_skill_names(
    interaction: AssembledInteraction,
) -> tuple[list[str], list[str], list[str]]:
    tools: list[str] = []
    skills: list[str] = []
    skill_names: list[str] = []
    seen_tools: set[str] = set()
    seen_skills: set[str] = set()
    seen_skill_names: set[str] = set()
    for record in interaction.assistants:
        author = record.message.author
        if author is None:
            continue
        for tool in author.tools_used:
            if tool not in seen_tools:
                seen_tools.add(tool)
                tools.append(tool)
        for skill in author.skills_used:
            if skill not in seen_skills:
                seen_skills.add(skill)
                skills.append(skill)
        for name in author.skills_used_names:
            if name not in seen_skill_names:
                seen_skill_names.add(name)
                skill_names.append(name)
    return tools, skills, skill_names


def build_chat_tags(interaction: AssembledInteraction) -> dict[str, str]:
    user = interaction.user
    conversation = user.conversation
    author = user.message.author
    tools, skills, skill_names = _assistant_tool_skill_names(interaction)
    tags: dict[str, str | None] = {
        "chatgpt source": "chat",
        "conversation id": conversation.id,
        "conversation title": conversation.title,
        "gpt name": conversation.gpt_name,
        "project name": conversation.project_name,
        "conversation mode": conversation.mode,
        "client type": author.client_type if author else None,
        "temporary chat": (
            str(conversation.is_temporary_chat).lower()
            if conversation.is_temporary_chat is not None
            else None
        ),
        "skills used": ",".join(skills) if skills else None,
        "skills used names": ",".join(skill_names) if skill_names else None,
        "tools used": ",".join(tools) if tools else None,
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
    interaction: AssembledInteraction,
) -> list[dict[str, Any]]:
    from .chat_converter import truncate_text, user_text  # noqa: PLC0415

    feedback: list[dict[str, Any]] = []
    if interaction.regenerated:
        feedback.append({"slug": "regenerate"})
    if interaction.edited:
        # EditAction.text is required; a slug-only item fails the union.
        feedback.append(
            {"slug": "edit", "text": truncate_text(user_text(interaction.user))}
        )
    return feedback


def build_codex_tags(
    turn: CodexTurn,
    turn_record: CodexTurnRecord | None,
) -> dict[str, str]:
    details = turn.prompt.event_details
    tags: dict[str, str | None] = {
        "chatgpt source": "codex",
        "client id": turn.prompt.client_id,
        "agent name": turn.prompt.agent.name if turn.prompt.agent else None,
        "model": details.model if details else None,
        "reasoning effort": details.reasoning_effort if details else None,
        "service tier": details.service_tier if details else None,
        "environment id": details.environment_id if details else None,
    }
    if turn_record and turn_record.payload.classification:
        cls = turn_record.payload.classification
        tags["codex category"] = cls.level_1
        tags["codex subcategory"] = cls.level_2
    return _drop_none_tags(tags)


def build_codex_traces(
    turn: CodexTurn,
    *,
    llm_model: str,
    input_tokens: int,
    output_tokens: int,
    output_text: str,
) -> list[dict[str, Any]]:
    from .chat_converter import truncate_text  # noqa: PLC0415
    from .codex_converter import prompt_text  # noqa: PLC0415

    traces: list[dict[str, Any]] = []
    for tool_event in turn.tool_calls:
        details = tool_event.event_details
        if details is None:
            continue
        tool_input = details.tool_input
        input_repr = ""
        if tool_input is not None:
            input_repr = truncate_text(
                json.dumps(tool_input, separators=(",", ":"), ensure_ascii=False)
            )
        outputs: list[str] = []
        if tool_event.event_type == "TOOL_CALL_FAILED":
            outputs = ["failed"]
        traces.append(
            {
                "source": details.tool_name or "tool",
                "input": input_repr,
                "outputs": outputs,
            }
        )

    prompt = prompt_text(turn.prompt.event_details)

    traces.append(
        {
            "model": llm_model,
            "messages": [{"role": "user", "content": truncate_text(prompt)}],
            "output": truncate_text(output_text),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }
    )
    return traces


def build_codex_user_feedback(_turn: CodexTurn) -> list[dict[str, Any]]:
    return []
