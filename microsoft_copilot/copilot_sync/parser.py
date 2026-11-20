from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from .models import AiInteraction

_ATTACHMENT_TAG_RE = re.compile(r'<attachment id="[^"]*"></attachment>')
_POINTER_ID_RE = re.compile(r'<attachment id="([^"]+)"></attachment>')
_ADAPTIVE_CARD_CONTENT_TYPE = "application/vnd.microsoft.card.adaptive"
# Copilot citation markers such as 【1-abc123】. Graph exports no mapping from
# the marker to its source, so they are unreadable outside Copilot.
_CITATION_MARKER_RE = re.compile(r"[ \t]*【[A-Za-z0-9]+-[0-9a-f]+】")


def _render_questions(question_data: object) -> str:
    """Render a questionData form as each question followed by its options."""
    if not isinstance(question_data, dict):
        return ""
    blocks: list[str] = []
    for question in question_data.get("questions", []):
        lines = [question.get("questionText", "")]
        for option in question.get("options", []):
            label = option.get("label", "")
            description = option.get("description")
            lines.append(f"- {label}: {description}" if description else f"- {label}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _unwrap_final_response(text: str) -> str:
    """Some app cards (e.g. Copilot in Excel, PowerPoint) embed
    {"thoughts": ..., "finalResponse": ...} as the TextBlock text.
    Return only the user-facing finalResponse when present; otherwise
    the text unchanged. An empty finalResponse with questionData means
    Copilot asked the user to pick options in a form, so the questions
    are returned instead."""
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text
    if isinstance(payload, dict) and isinstance(payload.get("finalResponse"), str):
        final_response = cast(str, payload["finalResponse"])
        return final_response or _render_questions(payload.get("questionData"))
    return text


def _visible_text(text: str) -> str:
    """Copilot in PowerPoint exports control records (pending task ids,
    model choice) as {"text": "", "hiddenText": "..."}. Only ``text`` is
    shown to the user."""
    if not text.startswith("{"):
        return text
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text
    if isinstance(payload, dict) and "hiddenText" in payload:
        return str(payload.get("text") or "")
    return text


def strip_attachment_tags(text: str) -> str:
    return _ATTACHMENT_TAG_RE.sub("", text).strip()


def extract_adaptive_card_text(card_content: str) -> str:
    """Extract human-readable text from an AdaptiveCard JSON string.

    TextBlock contributes its ``text``; RichTextBlock contributes the
    concatenation of its TextRun ``inlines``. Other element types
    (CustomRichUx, ResponseAction, Image, ...) carry no plain text and are
    skipped. Malformed or non-object JSON yields an empty string.
    """
    try:
        card = json.loads(card_content)
    except (json.JSONDecodeError, TypeError):
        return ""
    if not isinstance(card, dict):
        return ""

    parts: list[str] = []
    for element in card.get("body", []):
        if not isinstance(element, dict):
            continue
        if element.get("type") == "TextBlock":
            text = element.get("text")
            if text:
                parts.append(_CITATION_MARKER_RE.sub("", _unwrap_final_response(text)))
        elif element.get("type") == "RichTextBlock":
            joined = "".join(
                inline.get("text", "")
                for inline in element.get("inlines", [])
                if isinstance(inline, dict)
            )
            if joined:
                parts.append(joined)
    return "\n".join(parts)


def is_warmup_response_text(text: str) -> bool:
    """Return True when text is Copilot's internal warmup payload,
    not user-facing content."""
    stripped = text.strip()
    if not stripped.startswith("{"):
        return False
    try:
        payload = json.loads(stripped)
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(payload, dict) or len(payload) != 1:
        return False
    value = payload.get("IsWarmupRequest")
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return False


def parse_interaction_text(interaction: AiInteraction) -> str:
    """Resolve the readable text of an interaction.

    ``html`` bodies are only an ``<attachment id="X">`` pointer into the
    attachments list; the real text lives in the matching adaptive-card
    attachment. Everything else is plain text that may carry embedded
    attachment tags to strip.
    """
    body = interaction.body
    if body.content_type == "html":
        match = _POINTER_ID_RE.search(body.content)
        if match:
            pointer_id = match.group(1)
            for att in interaction.attachments:
                if (
                    att.attachment_id == pointer_id
                    and att.content_type == _ADAPTIVE_CARD_CONTENT_TYPE
                    and att.content
                ):
                    return extract_adaptive_card_text(att.content)
    return _visible_text(strip_attachment_tags(body.content))
