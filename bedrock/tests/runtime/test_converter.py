from __future__ import annotations

from typing import Any

import pytest

from bedrock_sync.runtime import user_defined
from bedrock_sync.runtime.converter import (
    END_USER_NAMESPACE,
    InteractionSkip,
    interaction_to_payload,
)
from bedrock_sync.runtime.grouping import Interaction, triplet_of
from bedrock_sync.runtime.models import Invocation
from bedrock_sync.runtime.parser import parse_record
from bedrock_sync.shared.keys import pseudonymize
from tests.runtime.records import assistant, converse_record, user

SUB = {"agent_role": "subagent"}


def _meta(**extra: str) -> dict[str, str]:
    return {
        "interaction_id": "i-1",
        "conversation_id": "c-1",
        "user_id": "u-1",
        **extra,
    }


def _call(
    request_id: str,
    second: int,
    messages: list[dict[str, Any]],
    answer: str | list[dict[str, Any]],
    *,
    metadata: dict[str, str] | None = None,
    model_id: str = "global.anthropic.claude-haiku-5-5",
    stop: str = "end_turn",
    cache: tuple[int, int] = (0, 0),
) -> Invocation:
    record = converse_record(
        messages,
        answer,
        stop=stop,
        cache=cache,
        request_id=request_id,
        timestamp=f"2026-11-11T10:00:{second:02d}Z",
        metadata=metadata or _meta(),
        model_id=model_id,
    )
    parsed = parse_record(record, source_key="key")
    assert isinstance(parsed, Invocation)
    return parsed


def _interaction(*calls: Invocation) -> Interaction:
    found = triplet_of(calls[0])
    assert found is not None
    interaction = Interaction(found)
    for invocation in calls:
        interaction.add(invocation)
    return interaction


def _result(
    interaction: Interaction,
    **kwargs: Any,  # noqa: ANN401
) -> dict[str, Any] | InteractionSkip:
    options: dict[str, Any] = {
        "anonymize": False,
        "send_plain_end_user": True,
        "user_hash_secret": "",
    }
    return interaction_to_payload(interaction, **{**options, **kwargs})


def _payload(interaction: Interaction, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
    result = _result(interaction, **kwargs)
    assert isinstance(result, dict)
    return result


TOOL_USE = {"toolUse": {"toolUseId": "t1", "name": "delegate", "input": {"task": "x"}}}
ASKED = {"role": "assistant", "content": [TOOL_USE]}
RESULT = {
    "role": "user",
    "content": [
        {"toolResult": {"toolUseId": "t1", "content": [{"text": "child says 42"}]}}
    ],
}


def _agent_turn() -> Interaction:
    """Parent delegates to a child through a tool, then answers; a background
    child finishes after the answer."""
    return _interaction(
        _call("p1", 0, [user("Compute it")], [TOOL_USE], stop="tool_use"),
        _call(
            "c1",
            1,
            [user('{"task": "x"}')],
            "42",
            metadata=_meta(**SUB),
            model_id="global.openai.gpt-6.1-terra",
        ),
        _call("p2", 5, [user("Compute it"), ASKED, RESULT], "The answer is 42."),
        _call(
            "c2",
            9,
            [user("label")],
            "internal label",
            metadata=_meta(**SUB),
            model_id="global.openai.gpt-6.1-terra",
        ),
    )


def test_input_output_and_conversation_come_from_the_main_agent() -> None:
    payload = _payload(_agent_turn())["interaction"]

    assert payload["input"] == "Compute it"
    assert payload["output"] == "The answer is 42."
    assert payload["conversation_id"] == "c-1"
    assert payload["time_start"].startswith("2026-11-11T10:00:00")
    assert payload["end_user"] == "u-1"


def test_subagent_answer_is_one_retrieval_trace_and_child_tokens_are_counted() -> None:
    traces = _payload(_agent_turn())["traces"]

    llm = {t["model"]: t for t in traces if "model" in t}
    assert set(llm) == {"claude-haiku-5-5", "gpt-6.1-terra"}
    assert llm["claude-haiku-5-5"]["input_tokens"] == 20
    assert llm["gpt-6.1-terra"]["output_tokens"] == 10
    retrieval = [t for t in traces if "source" in t]
    assert retrieval == [
        {"source": "delegate", "input": '{"task":"x"}', "outputs": ["child says 42"]}
    ]


def test_without_the_marker_the_last_call_is_the_answer() -> None:
    interaction = _interaction(
        _call("p1", 0, [user("Q")], "partial"),
        _call("p2", 5, [user("Q")], "final"),
    )

    assert _payload(interaction)["interaction"]["output"] == "final"


def test_tokens_exclude_cache_which_goes_in_extra_keys() -> None:
    interaction = _interaction(_call("p1", 0, [user("Q")], "A", cache=(100, 7)))

    [trace] = _payload(interaction)["traces"]
    assert trace["input_tokens"] == 10
    assert trace["cache_read_input_tokens"] == 100
    assert trace["cache_write_input_tokens"] == 7


def test_all_calls_marked_as_subagent_is_skipped() -> None:
    interaction = _interaction(_call("c1", 0, [user("Q")], "A", metadata=_meta(**SUB)))

    assert _result(interaction) is InteractionSkip.NO_MAIN_CALL


def test_empty_interaction_is_skipped() -> None:
    interaction = _interaction(_call("p1", 0, [user(" ")], " "))

    assert _result(interaction) is InteractionSkip.EMPTY_INTERACTION


def test_end_user_is_pseudonymized_with_a_namespace() -> None:
    interaction = _interaction(_call("p1", 0, [user("Q")], "A"))

    end_user = _payload(
        interaction, send_plain_end_user=False, user_hash_secret="secret"
    )["interaction"]["end_user"]

    assert end_user == pseudonymize(END_USER_NAMESPACE, "u-1", "secret")
    assert end_user != "u-1"


def test_tags_keep_custom_metadata_but_not_reserved_keys() -> None:
    interaction = _interaction(
        _call("p1", 0, [user("Q")], "A", metadata=_meta(team_name="ops"))
    )

    tags = _payload(interaction)["interaction"]["tags"]
    assert tags["team_name"] == "ops"
    assert tags["model"] == "claude-haiku-5-5"
    assert not {"interaction_id", "conversation_id", "user_id", "agent_role"} & set(
        tags
    )


def test_input_skips_tool_result_messages() -> None:
    interaction = _interaction(
        _call("p1", 0, [user("Find it"), ASKED, RESULT], "Found."),
    )

    assert _payload(interaction)["interaction"]["input"] == "Find it"


def test_texts_are_truncated() -> None:
    long = "x" * (user_defined.MAX_CONTENT_CHARS + 10)
    interaction = _interaction(_call("p1", 0, [user(long)], long))

    payload = _payload(interaction)["interaction"]
    assert (
        len(payload["input"])
        == len(payload["output"])
        == user_defined.MAX_CONTENT_CHARS
    )


def test_segments_hook_is_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    interaction = _interaction(_call("p1", 0, [user("Q")], "A"))
    monkeypatch.setattr(
        user_defined, "build_segments", lambda *_: {"team": "Ops", "market": None}
    )
    assert _payload(interaction)["interaction"]["team"] == "Ops"

    monkeypatch.setattr(user_defined, "build_segments", lambda *_: {"role": "x"})
    with pytest.raises(ValueError, match="segment"):
        _payload(interaction)


def test_model_names_are_normalized() -> None:
    interaction = _interaction(
        _call(
            "p1",
            0,
            [user("Q")],
            "A",
            model_id="eu.anthropic.claude-haiku-4-5-20251001-v1:0",
        )
    )

    assert _payload(interaction)["interaction"]["tags"]["model"] == "claude-haiku-4-5"


def test_history_with_assistant_text_keeps_the_latest_question() -> None:
    interaction = _interaction(
        _call("p1", 0, [user("First"), assistant("Hi"), user("Second")], "A")
    )

    assert _payload(interaction)["interaction"]["input"] == "Second"


def _tool(tool_id: str, name: str) -> dict[str, Any]:
    return {"toolUse": {"toolUseId": tool_id, "name": name, "input": {"q": name}}}


def _results(*pairs: tuple[str, str]) -> dict[str, Any]:
    return {
        "role": "user",
        "content": [
            {"toolResult": {"toolUseId": tool_id, "content": [{"text": text}]}}
            for tool_id, text in pairs
        ],
    }


def test_tool_results_pair_with_the_call_of_their_own_round() -> None:
    """Some models reuse the same tool id in every round."""
    history = [
        user("Look twice"),
        {"role": "assistant", "content": [_tool("functions.search:0", "first")]},
        _results(("functions.search:0", "round one")),
        {"role": "assistant", "content": [_tool("functions.search:0", "second")]},
        _results(("functions.search:0", "round two")),
    ]
    interaction = _interaction(_call("p1", 0, history, "Done."))

    retrieval = [t for t in _payload(interaction)["traces"] if "source" in t]
    assert [(t["source"], t["outputs"]) for t in retrieval] == [
        ("first", ["round one"]),
        ("second", ["round two"]),
    ]


def test_tools_of_earlier_turns_are_not_part_of_this_interaction() -> None:
    history = [
        user("Old question"),
        {"role": "assistant", "content": [_tool("t0", "old_tool")]},
        _results(("t0", "old result")),
        assistant("Old answer"),
        user("New question"),
        {"role": "assistant", "content": [_tool("t1", "new_tool")]},
        _results(("t1", "new result")),
    ]
    payload = _payload(_interaction(_call("p1", 0, history, "New answer.")))

    assert payload["interaction"]["input"] == "New question"
    assert [t["source"] for t in payload["traces"] if "source" in t] == ["new_tool"]
    assert payload["interaction"]["tags"]["tools used"] == "new_tool"


def test_text_added_to_a_tool_result_message_is_not_a_new_turn() -> None:
    with_text = _results(("t1", "result"))
    with_text["content"].append({"text": "<reminder injected by the client>"})
    history = [user("Real question"), ASKED, with_text]

    assert (
        _payload(_interaction(_call("p1", 0, history, "A")))["interaction"]["input"]
        == "Real question"
    )


def test_models_tag_lists_every_model_only_when_there_are_several() -> None:
    assert (
        "models"
        not in _payload(_interaction(_call("p1", 0, [user("Q")], "A")))["interaction"][
            "tags"
        ]
    )
    tags = _payload(_agent_turn())["interaction"]["tags"]
    assert tags["model"] == "claude-haiku-5-5"
    assert tags["models"] == "claude-haiku-5-5,gpt-6.1-terra"
    assert tags["tools used"] == "delegate"
