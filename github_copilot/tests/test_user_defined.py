from __future__ import annotations

import pytest
from github_copilot_sync.models import Call
from github_copilot_sync.user_defined import default_is_background_call

CALL = Call(
    github_request_id="req-1",
    user_id=1,
    endpoint="/enterprises/e/copilot/chat/completions",
    request_body="",
    response_body="",
    request_timestamp_ms=1_000,
    response_timestamp_ms=2_000,
    request_event_id="e-req",
    response_event_id="e-resp",
)


@pytest.mark.parametrize(
    "system_content",
    [
        "Generate a concise title for this chat.",
        [{"type": "text", "text": "Generate a concise title for this chat."}],
    ],
)
def test_helper_system_prompt_is_detected(system_content: object) -> None:
    request_json: dict[str, object] = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": "hello"},
        ]
    }

    assert default_is_background_call(CALL, request_json)


@pytest.mark.parametrize(
    "system_content",
    ["You are a coding assistant.", [{"type": "text", "text": "Be helpful."}]],
)
def test_regular_system_prompt_is_not_background(system_content: object) -> None:
    request_json: dict[str, object] = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": "hello"},
        ]
    }

    assert not default_is_background_call(CALL, request_json)
