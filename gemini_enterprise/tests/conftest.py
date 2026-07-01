from __future__ import annotations

from typing import Any

from gemini_enterprise_sync.models import Session, SessionTurn

_SESSION_NAME = (
    "projects/p/locations/eu/collections/default_collection/"
    "engines/gemini-enterprise/sessions/{session_id}"
)

_DEFAULT_QUERY_CONFIG: dict[str, str] = {
    "google.discoveryengine.googleapis.com.Assistant.model_id": "gemini-2.5-flash",
    "google.discoveryengine.googleapis.com.Assistant.answer_generation_mode": "NORMAL",
}


def _session(
    session_id: str = "sess_1",
    *,
    end_time: str = "2026-06-29T12:50:07.000000Z",
    user_id: str = "user_1",
    turns: list[dict[str, Any]] | None = None,
) -> Session:
    return Session.model_validate(
        {
            "name": _SESSION_NAME.format(session_id=session_id),
            "userPseudoId": user_id,
            "startTime": "2026-06-29T12:50:00.000000Z",
            "endTime": end_time,
            "updateTime": end_time,
            "turns": turns or [],
        }
    )


def _reply_text(text: str, *, create_time: str) -> dict[str, Any]:
    return {
        "groundedContent": {"content": {"role": "model", "text": text}},
        "createTime": create_time,
    }


def _reply_thought(text: str, *, create_time: str) -> dict[str, Any]:
    return {
        "groundedContent": {
            "content": {"role": "model", "text": text, "thought": True}
        },
        "createTime": create_time,
    }


def _reply_inline_suggestions(*, create_time: str) -> dict[str, Any]:
    return {
        "groundedContent": {
            "content": {
                "role": "model",
                "inlineData": {
                    "mimeType": "application/json+suggestions",
                    "data": "eyJyZWNvbW1lbmRlZFF1ZXN0aW9uc1Jlc3BvbnNlIjp7InF1ZXN",
                },
            }
        },
        "createTime": create_time,
    }


def _reply_grounding(
    references: list[dict[str, Any]], *, create_time: str
) -> dict[str, Any]:
    return {
        "groundedContent": {"textGroundingMetadata": {"references": references}},
        "createTime": create_time,
    }


def _grounding_reference(
    index: int, *, uri: str | None = None, title: str | None = None
) -> dict[str, Any]:
    return {
        "content": f"Reference snippet {index}",
        "documentMetadata": {
            "uri": uri or f"https://example.com/doc-{index}",
            "title": title or f"Document {index}",
            "domain": "example.com",
        },
    }


def _turn(
    query_text: str,
    replies: list[dict[str, Any]],
    *,
    query_id: str = "query-1",
) -> dict[str, Any]:
    return {
        "query": {"queryId": query_id, "text": query_text},
        "detailedAssistAnswer": {"state": "SUCCEEDED", "replies": replies},
        "queryConfig": dict(_DEFAULT_QUERY_CONFIG),
    }


def plain_text_turn(
    query_text: str = "hello",
    answer_text: str = "world",
    *,
    query_id: str = "query-plain",
    answer_time: str = "2026-06-29T12:50:03.000000Z",
) -> SessionTurn:
    turn_dict = _turn(
        query_text,
        [_reply_text(answer_text, create_time=answer_time)],
        query_id=query_id,
    )
    return SessionTurn.model_validate(turn_dict)


def thought_reply_turn(
    query_text: str = "explain",
    answer_text: str = "Here is the answer.",
    *,
    query_id: str = "query-thought",
) -> SessionTurn:
    turn_dict = _turn(
        query_text,
        [
            _reply_thought(
                "Thinking step one",
                create_time="2026-06-29T12:50:02.000000Z",
            ),
            _reply_text(
                answer_text,
                create_time="2026-06-29T12:50:03.000000Z",
            ),
        ],
        query_id=query_id,
    )
    return SessionTurn.model_validate(turn_dict)


def inline_data_suggestions_turn(
    query_text: str = "suggest",
    answer_text: str = "Main answer.",
    *,
    query_id: str = "query-inline",
) -> SessionTurn:
    turn_dict = _turn(
        query_text,
        [
            _reply_text(
                answer_text,
                create_time="2026-06-29T12:50:03.000000Z",
            ),
            _reply_inline_suggestions(create_time="2026-06-29T12:50:03.500000Z"),
        ],
        query_id=query_id,
    )
    return SessionTurn.model_validate(turn_dict)


def grounding_separate_reply_turn(
    query_text: str = "search nebuly",
    answer_text: str = "Summary from sources.",
    *,
    query_id: str = "query-ground",
    ref_count: int = 5,
) -> SessionTurn:
    references = [_grounding_reference(i + 1) for i in range(ref_count)]
    turn_dict = _turn(
        query_text,
        [
            _reply_text(
                answer_text,
                create_time="2026-06-29T12:50:03.000000Z",
            ),
            _reply_grounding(
                references,
                create_time="2026-06-29T12:50:03.500000Z",
            ),
        ],
        query_id=query_id,
    )
    return SessionTurn.model_validate(turn_dict)


def empty_replies_turn(
    query_text: str = "hello?",
    *,
    query_id: str = "query-empty",
) -> SessionTurn:
    turn_dict = _turn(query_text, [], query_id=query_id)
    return SessionTurn.model_validate(turn_dict)


def multi_turn_session(
    session_id: str = "sess_multi",
) -> Session:
    turns = [
        plain_text_turn("first", "one", query_id="q-a"),
        plain_text_turn(
            "second",
            "two",
            query_id="q-b",
            answer_time="2026-06-29T12:51:02.000000Z",
        ),
    ]
    return _session(
        session_id,
        end_time="2026-06-29T12:51:07.000000Z",
        turns=[turn.model_dump(by_alias=True) for turn in turns],
    )
