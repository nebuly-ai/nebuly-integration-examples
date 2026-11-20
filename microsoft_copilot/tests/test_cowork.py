from __future__ import annotations

from datetime import UTC, datetime

from copilot_sync.converter import SkipReason, group_interactions, turn_to_payload
from copilot_sync.models import AiInteraction, CopilotUser, InteractionBody

_COWORK = "IPM.SkypeTeams.Message.Copilot.CoworkChat"


def _record(
    kind: str, content: str, minute: int, second: int, ms: int
) -> AiInteraction:
    return AiInteraction(
        id=f"{kind}_{minute}_{second}_{ms}",
        request_id=None,
        session_id="19:cowork@thread.v2",
        interaction_type=kind,
        app_class=_COWORK,
        conversation_type="coworkchat",
        locale="en-us",
        created_datetime=datetime(
            2026, 11, 19, 11, minute, second, ms * 1000, tzinfo=UTC
        ),
        body=InteractionBody(
            content_type="text" if kind == "userPrompt" else "html",
            content=content,
        ),
    )


def _prompt(content: str, minute: int, second: int, ms: int = 0) -> AiInteraction:
    return _record("userPrompt", content, minute, second, ms)


def _response(content: str, minute: int, second: int, ms: int = 0) -> AiInteraction:
    return _record("aiResponse", content, minute, second, ms)


def _payloads(
    interactions: list[AiInteraction],
) -> list[tuple[str, str] | SkipReason]:
    turns, _ = group_interactions(interactions)
    results: list[tuple[str, str] | SkipReason] = []
    for turn in turns:
        payload = turn_to_payload(turn, user=CopilotUser(id="u1"), anonymize=False)
        if isinstance(payload, SkipReason):
            results.append(payload)
        else:
            interaction = payload["interaction"]
            results.append((interaction["input"], interaction["output"]))
    return results


def test_chunks_split_mid_sentence_are_joined() -> None:
    interactions = [
        _prompt("What can you do?", 22, 21, 219),
        _response("I can set up briefings, reminders, and", 22, 21, 452),
        _response("workflows.\n\nTry asking me anything.", 22, 21, 798),
    ]
    assert _payloads(interactions) == [
        (
            "What can you do?",
            "I can set up briefings, reminders, and workflows.\n\n"
            "Try asking me anything.",
        )
    ]


def test_steering_prompt_merges_into_running_task() -> None:
    """Shape of a real export: the follow-up arrives while the agent works,
    and the answers to both prompts come after it, glued in shared records."""
    interactions = [
        _prompt("Move my meeting and tell the team", 23, 9, 413),
        _response("", 23, 9, 441),
        _response("", 23, 28),
        _prompt("Write the message in Italian", 24, 35),
        _response("", 24, 52, 767),
        _response(
            "Not sent, approval declined.Moved the meeting to 16:30.", 24, 52, 883
        ),
        _response("", 25, 11),
        _response("Messaggio inviato?Inviato nella chat.", 26, 27, 304),
        _prompt("I changed the message myself", 26, 27, 378),
        _response("Perfect, thanks!", 26, 27, 473),
    ]
    assert _payloads(interactions) == [
        (
            "Move my meeting and tell the team\n\nWrite the message in Italian",
            "Not sent, approval declined.\n\nMoved the meeting to 16:30."
            "\n\nMessaggio inviato?\n\nInviato nella chat.",
        ),
        ("I changed the message myself", "Perfect, thanks!"),
    ]


def test_task_that_never_produces_text_is_skipped() -> None:
    interactions = [
        _prompt("Save a file to OneDrive", 23, 9),
        _response("", 23, 10),
        _response("", 23, 30),
    ]
    assert _payloads(interactions) == [SkipReason.EMPTY_OUTPUT]
    _, dangling = group_interactions(interactions)
    assert dangling == []


def test_reasoning_summary_is_dropped_unless_final() -> None:
    interactions = [
        _prompt("Plan my week", 20, 0),
        _response("**Finding an efficient solution** planning text.", 20, 1),
        _response("Here is your plan.", 20, 2),
    ]
    assert _payloads(interactions) == [("Plan my week", "Here is your plan.")]


def test_html_entities_are_decoded() -> None:
    interactions = [
        _prompt("Draft it", 20, 0),
        _response("&gt; Salt &amp; Pepper", 20, 1),
    ]
    assert _payloads(interactions) == [("Draft it", "> Salt & Pepper")]


def test_glued_message_split_ignores_acronyms_and_abbreviations() -> None:
    interactions = [
        _prompt("Which stack?", 20, 0),
        _response("We use ASP.NET and e.g. Python.", 20, 1),
    ]
    assert _payloads(interactions) == [
        ("Which stack?", "We use ASP.NET and e.g. Python.")
    ]
