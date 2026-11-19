"""The command-line boundary both connectors share.

Logs go to stderr, the run summary goes to stdout as one JSON object. The
exit code is 0 when everything closed was delivered, 1 when a run failed,
left payloads rejected, or deliveries wait for a retry, 130 on
Ctrl-C. An unexpected error is logged with its type and where it happened,
never its message, which may hold request bodies or secrets.
"""

from __future__ import annotations

import json
import logging
import traceback
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Protocol

from .errors import ConnectorError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# Libraries that could log HTTP headers or bodies.
_QUIET = ("httpx", "httpcore", "boto3", "botocore", "urllib3")


class _Settings(Protocol):
    @property
    def verbose(self) -> bool: ...


def configure_logging(*, verbose: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    for name in _QUIET:
        logging.getLogger(name).setLevel(logging.WARNING)
    logging.getLogger("bedrock_sync").setLevel(
        logging.DEBUG if verbose else logging.INFO
    )


def run_command[C: _Settings, R](
    parse: Callable[[], C],
    sync: Callable[[C], R],
    *,
    incomplete: Callable[[R], bool],
) -> int:
    """Parse the settings, run one sync and print its summary."""
    configure_logging(verbose=False)
    try:
        config = parse()
        configure_logging(verbose=config.verbose)
        report = sync(config)
    except KeyboardInterrupt:
        logger.error("Interrupted: the state keeps what was saved")  # noqa: TRY400
        return 130
    except ConnectorError as exc:
        logger.error("%s", exc)  # noqa: TRY400 - the message is the useful part
        return 1
    except Exception as exc:  # noqa: BLE001 - may hold request bodies or secrets
        # The message may hold request bodies or secrets: log only the type
        # and where it happened, which is code, never data.
        logger.error(  # noqa: TRY400 - omit the exception message on purpose
            "Sync failed (%s at %s); the local state is kept. Run again with "
            "--verbose for the full stack",
            type(exc).__name__,
            _location(exc),
        )
        logger.debug(
            "Stack, without the error message:\n%s",
            format_exception_stack(exc),
        )
        return 1
    print(json.dumps(_as_dict(report), sort_keys=True))  # noqa: T201 - CLI output
    return int(incomplete(report))


def _location(exc: BaseException) -> str:
    """The innermost frame in this package, else the innermost one."""
    frames = traceback.extract_tb(exc.__traceback__)
    if not frames:
        return "unknown location"
    ours = [f for f in frames if "bedrock_sync/" in f.filename.replace("\\", "/")]
    return _frame((ours or frames)[-1])


def _frame(frame: traceback.FrameSummary) -> str:
    """File, line and function only: source lines may hold literals."""
    path = frame.filename.replace("\\", "/")
    start = path.rfind("bedrock_sync/")
    shown = path[start:] if start >= 0 else path.rsplit("/", 1)[-1]
    return f"{shown}:{frame.lineno} in {frame.name}"


def format_exception_stack(exc: BaseException) -> str:
    """Show stack locations without exception messages or source lines."""
    return "\n".join(
        f"  {_frame(frame)}" for frame in traceback.extract_tb(exc.__traceback__)
    )


def _as_dict(report: Any) -> dict[str, Any]:  # noqa: ANN401 - any dataclass
    return asdict(report)
