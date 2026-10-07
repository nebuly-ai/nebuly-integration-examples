from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from langfuse_sync.config import Config
from langfuse_sync.coverage import Coverage
from langfuse_sync.nebuly_client import SendResult
from langfuse_sync.sync import (
    FirstRunRequiresFromDateError,
    GapAbortedError,
    run_sync,
)

if TYPE_CHECKING:
    from pathlib import Path


def _ts(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 7, 2, hour, minute, second, tzinfo=UTC)


def _trace(
    trace_id: str,
    *,
    hour: int = 10,
    minute: int = 0,
) -> dict[str, Any]:
    ts = _ts(hour, minute)
    return {
        "id": trace_id,
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "sessionId": f"session-{trace_id}",
        "userId": "user-1",
        "input": "hello",
        "output": "world",
        "tags": ["team:Engineering"],
    }


class FakeLangfuseClient:
    def __init__(
        self,
        traces_by_window: list[tuple[datetime, datetime, list[dict[str, Any]]]],
    ) -> None:
        self._windows = traces_by_window
        self.trace_calls: list[tuple[datetime, datetime]] = []

    async def get_traces(
        self, start: datetime, end: datetime, limit: int = 100
    ) -> list[dict[str, Any]]:
        self.trace_calls.append((start, end))
        for win_start, win_end, traces in self._windows:
            if win_start == start and win_end == end:
                return traces
        return []

    async def get_observations_by_trace_id(
        self, start: datetime, end: datetime, limit: int = 100
    ) -> dict[str, list[dict[str, Any]]]:
        return {}


class FakeNebulyClient:
    def __init__(
        self,
        *,
        fail_on: set[str] | None = None,
        too_large_on: set[str] | None = None,
    ) -> None:
        self.sent: list[str] = []
        self._fail_on = fail_on or set()
        self._too_large_on = too_large_on or set()

    async def send_interaction(self, payload: dict[str, Any]) -> SendResult:
        await asyncio.sleep(0)
        interaction = payload["interaction"]
        trace_id = str(interaction["conversation_id"]).removeprefix("session-")
        if trace_id in self._fail_on:
            raise httpx.HTTPStatusError(
                "error",
                request=httpx.Request("POST", "https://example.com"),
                response=httpx.Response(500),
            )
        if trace_id in self._too_large_on:
            return SendResult.TOO_LARGE
        self.sent.append(trace_id)
        return SendResult.SENT


def _config(
    tmp_path: Path,
    *,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    dry_run: bool = False,
    force: bool = False,
    max_concurrency: int = 5,
) -> Config:
    return Config(
        langfuse_public_key="pub",
        langfuse_secret_key="sec",
        langfuse_base_url="https://langfuse.example.com",
        nebuly_api_key="neb",
        nebuly_endpoint="https://example.com/events",
        anonymize=False,
        settle_lag_seconds=900,
        from_date=from_date,
        to_date=to_date,
        cache_dir=tmp_path,
        dry_run=dry_run,
        verbose=False,
        force=force,
        max_concurrency=max_concurrency,
    )


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_first_run_without_from_date_raises(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    with pytest.raises(FirstRunRequiresFromDateError):
        run_sync(_config(tmp_path, from_date=None))


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_rerun_with_no_new_traces_sends_nothing(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    Coverage(tmp_path).save(
        coverage_from=_ts(8),
        coverage_until=_ts(12),
    )
    fake_nebuly = FakeNebulyClient()
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient([])

    code = run_sync(_config(tmp_path, from_date=None, to_date=_ts(12)))

    assert code == 0
    assert fake_nebuly.sent == []


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_new_traces_after_watermark_are_sent(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    Coverage(tmp_path).save(
        coverage_from=_ts(8),
        coverage_until=_ts(12),
    )
    new_trace = _trace("new-1", hour=13)
    fake_nebuly = FakeNebulyClient()
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient(
        [(_ts(12), _ts(14), [new_trace])]
    )
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly

    code = run_sync(_config(tmp_path, from_date=None, to_date=_ts(14)))

    assert code == 0
    assert fake_nebuly.sent == ["new-1"]


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_boundary_trace_is_not_resent(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    Coverage(tmp_path).save(
        coverage_until=_ts(12),
        coverage_until_ids=["seen-1"],
    )
    traces = [_trace("seen-1", hour=12), _trace("fresh-2", hour=12, minute=5)]
    fake_nebuly = FakeNebulyClient()
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient(
        [(_ts(12), _ts(14), traces)]
    )
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly

    run_sync(_config(tmp_path, from_date=None, to_date=_ts(14)))

    assert fake_nebuly.sent == ["fresh-2"]


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_failure_mid_run_resumes_without_replay(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    traces = [_trace("a", hour=10), _trace("b", hour=10, minute=1)]
    window = (_ts(10), _ts(11), traces)
    fake_nebuly = FakeNebulyClient(fail_on={"b"})
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient([window])
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly

    code = run_sync(_config(tmp_path, from_date=_ts(10), to_date=_ts(11)))
    assert code == 1
    assert fake_nebuly.sent == ["a"]

    fake_nebuly_2 = FakeNebulyClient()
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient([window])
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly_2
    code = run_sync(_config(tmp_path, from_date=None, to_date=_ts(11)))
    assert code == 0
    assert fake_nebuly_2.sent == ["b"]


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_partial_failure_commits_only_contiguous_prefix(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    traces = [_trace(name, hour=10, minute=i) for i, name in enumerate("abcdef")]
    fake_nebuly = FakeNebulyClient(fail_on={"c"})
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient(
        [(_ts(10), _ts(11), traces)]
    )
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly

    code = run_sync(
        _config(tmp_path, from_date=_ts(10), to_date=_ts(11), max_concurrency=2)
    )

    assert code == 1
    assert "c" not in fake_nebuly.sent
    state = Coverage(tmp_path).load()
    assert state.coverage_until == _ts(10, 1)
    assert state.coverage_until_ids == frozenset({"b"})

    fake_nebuly_2 = FakeNebulyClient()
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient(
        [(_ts(10, 1), _ts(11), traces[1:])]
    )
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly_2

    code = run_sync(_config(tmp_path, from_date=None, to_date=_ts(11)))

    assert code == 0
    assert {"c", "e", "f"} <= set(fake_nebuly_2.sent)
    assert not {"a", "b"} & set(fake_nebuly_2.sent)


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_413_advances_watermark(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    traces = [_trace("big", hour=10)]
    fake_nebuly = FakeNebulyClient(too_large_on={"big"})
    langfuse_cls.side_effect = lambda *args, **kwargs: FakeLangfuseClient(
        [(_ts(10), _ts(11), traces)]
    )
    nebuly_cls.side_effect = lambda *args, **kwargs: fake_nebuly

    run_sync(_config(tmp_path, from_date=_ts(10), to_date=_ts(11)))

    state = Coverage(tmp_path).load()
    assert state.coverage_until_ids == frozenset({"big"})
    assert state.coverage_until == _ts(11)


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_gap_without_yes_aborts(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    Coverage(tmp_path).save(
        coverage_from=_ts(8),
        coverage_until=_ts(12),
    )
    with (
        patch("langfuse_sync.sync.sys.stdin.isatty", return_value=False),
        pytest.raises(GapAbortedError),
    ):
        run_sync(_config(tmp_path, from_date=_ts(14), to_date=_ts(16)))


@patch("langfuse_sync.sync.httpx.AsyncClient")
@patch("langfuse_sync.sync.LangfuseClient")
@patch("langfuse_sync.sync.NebulyClient")
def test_settle_lag_limits_requested_until(
    nebuly_cls: MagicMock,
    langfuse_cls: MagicMock,
    http_cls: MagicMock,
    tmp_path: Path,
) -> None:
    fixed_now = datetime(2026, 7, 2, 15, 0, tzinfo=UTC)
    fake_langfuse = FakeLangfuseClient([])
    fake_nebuly = FakeNebulyClient()
    langfuse_cls.return_value = fake_langfuse
    nebuly_cls.return_value = fake_nebuly

    config = _config(tmp_path, from_date=_ts(10), to_date=None)
    with patch("langfuse_sync.config.datetime") as dt_mod:
        dt_mod.now.return_value = fixed_now
        dt_mod.side_effect = datetime
        dt_mod.UTC = UTC
        dt_mod.timedelta = timedelta
        run_sync(config)

    expected_until = fixed_now - timedelta(seconds=900)
    assert fake_langfuse.trace_calls[0][1] == expected_until
