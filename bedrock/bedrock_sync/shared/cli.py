"""The command-line boundary both connectors share.

Logs go to stderr, the run summary goes to stdout as one JSON object. The
exit code is 0 when everything closed was delivered, 1 when a run failed,
left payloads rejected or uncertain, or deliveries wait for a retry, 130 on
Ctrl-C.
"""

from __future__ import annotations

import json
import logging
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
        logger.error(  # noqa: TRY400 - omit the exception details on purpose
            "Sync failed (%s); the local state is kept", type(exc).__name__
        )
        return 1
    print(json.dumps(_as_dict(report), sort_keys=True))  # noqa: T201 - CLI output
    return int(incomplete(report))


def _as_dict(report: Any) -> dict[str, Any]:  # noqa: ANN401 - any dataclass
    return asdict(report)
