from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from copilot_sync.models import AiInteraction, Attachment, InteractionBody
from copilot_sync.parser import (
    extract_adaptive_card_text,
    parse_interaction_text,
    strip_attachment_tags,
)


def _card(*body_elements: dict[str, Any]) -> str:
    return json.dumps(
        {"type": "AdaptiveCard", "version": "1.0", "body": list(body_elements)}
    )


def _interaction(
    *,
    content_type: str,
    content: str,
    attachments: list[Attachment] | None = None,
) -> AiInteraction:
    return AiInteraction(
        id="x",
        request_id="req",
        session_id="sess",
        interaction_type="aiResponse",
        app_class="IPM.SkypeTeams.Message.Copilot.BizChat",
        conversation_type="bizchat",
        locale="en-us",
        created_datetime=datetime(2026, 6, 24, 13, 0, tzinfo=UTC),
        body=InteractionBody(content_type=content_type, content=content),
        attachments=attachments or [],
    )


def _adaptive_attachment(attachment_id: str, card_json: str) -> Attachment:
    return Attachment(
        attachmentId=attachment_id,
        content=card_json,
        contentType="application/vnd.microsoft.card.adaptive",
    )


def test_strip_attachment_tags_removes_empty_tag() -> None:
    assert strip_attachment_tags('ponti<attachment id=""></attachment>') == "ponti"


def test_strip_attachment_tags_removes_id_tag_and_trims() -> None:
    assert strip_attachment_tags('  hi <attachment id="abc"></attachment>  ') == "hi"


def test_extract_single_textblock() -> None:
    card = _card({"type": "TextBlock", "text": "Hello world"})
    assert extract_adaptive_card_text(card) == "Hello world"


def test_extract_textblock_unwraps_final_response() -> None:
    card = _card(
        {
            "type": "TextBlock",
            "text": '{"thoughts":"internal reasoning","finalResponse":"answer"}',
        }
    )
    assert extract_adaptive_card_text(card) == "answer"


def test_extract_textblock_without_final_response_returns_raw_json() -> None:
    raw = '{"thoughts":"x"}'
    card = _card({"type": "TextBlock", "text": raw})
    assert extract_adaptive_card_text(card) == raw


def test_extract_multiple_textblocks_joined_by_newline() -> None:
    card = _card(
        {"type": "TextBlock", "text": "First"},
        {"type": "TextBlock", "text": "Second"},
    )
    assert extract_adaptive_card_text(card) == "First\nSecond"


def test_extract_richtextblock_concatenates_inlines() -> None:
    card = _card(
        {
            "type": "RichTextBlock",
            "inlines": [
                {"type": "TextRun", "text": "import os"},
                {"type": "TextRun", "text": "\nprint(1)"},
            ],
        }
    )
    assert extract_adaptive_card_text(card) == "import os\nprint(1)"


def test_extract_skips_non_text_elements() -> None:
    card = _card(
        {"type": "TextBlock", "text": "Answer"},
        {"type": "CustomRichUx", "uxType": "x", "uxJson": "{}", "uxId": "1"},
        {"type": "ResponseAction", "url": "https://example.com"},
    )
    assert extract_adaptive_card_text(card) == "Answer"


def test_extract_malformed_json_returns_empty() -> None:
    assert extract_adaptive_card_text("{not valid json") == ""


def test_extract_non_dict_json_returns_empty() -> None:
    assert extract_adaptive_card_text("[1, 2, 3]") == ""


def test_parse_text_body_strips_embedded_tag() -> None:
    inter = _interaction(
        content_type="text",
        content='Crea una slide<attachment id=""></attachment>',
    )
    assert parse_interaction_text(inter) == "Crea una slide"


def test_parse_html_body_resolves_adaptive_card() -> None:
    card = _card({"type": "TextBlock", "text": "Risposta finale"})
    inter = _interaction(
        content_type="html",
        content='<attachment id="abc"></attachment>',
        attachments=[_adaptive_attachment("abc", card)],
    )
    assert parse_interaction_text(inter) == "Risposta finale"


def test_parse_html_body_missing_attachment_returns_empty() -> None:
    inter = _interaction(
        content_type="html",
        content='<attachment id="missing"></attachment>',
        attachments=[],
    )
    assert parse_interaction_text(inter) == ""


def test_parse_html_body_null_content_attachment_returns_empty() -> None:
    inter = _interaction(
        content_type="html",
        content='<attachment id="ref"></attachment>',
        attachments=[
            Attachment(
                attachmentId=None, content=None, contentType="reference", name="x"
            ),
        ],
    )
    assert parse_interaction_text(inter) == ""
