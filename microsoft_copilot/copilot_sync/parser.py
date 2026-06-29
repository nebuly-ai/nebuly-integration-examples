from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from .models import AiInteraction

_ATTACHMENT_TAG_RE = re.compile(r'<attachment id="[^"]*"></attachment>')
_POINTER_ID_RE = re.compile(r'<attachment id="([^"]+)"></attachment>')
_ADAPTIVE_CARD_CONTENT_TYPE = "application/vnd.microsoft.card.adaptive"


def _unwrap_final_response(text: str) -> str:
    """Some app cards (e.g. Copilot in Excel) embed
    {"thoughts": ..., "finalResponse": ...} as the TextBlock text.
    Return only the user-facing finalResponse when present; otherwise
    the text unchanged."""
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text
    if isinstance(payload, dict) and isinstance(payload.get("finalResponse"), str):
        return cast(str, payload["finalResponse"])
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
                parts.append(_unwrap_final_response(text))
        elif element.get("type") == "RichTextBlock":
            joined = "".join(
                inline.get("text", "")
                for inline in element.get("inlines", [])
                if isinstance(inline, dict)
            )
            if joined:
                parts.append(joined)
    return "\n".join(parts)


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
    return strip_attachment_tags(body.content)
