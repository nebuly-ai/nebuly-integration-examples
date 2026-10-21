from __future__ import annotations

import logging
import sys

from .config import Config
from .export import run_export


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    log = logging.getLogger("entra_export")
    try:
        config = Config.from_env_and_args(argv)
    except RuntimeError as exc:
        sys.exit(str(exc))

    if config.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(
        logging.DEBUG if config.verbose else logging.WARNING,
    )

    try:
        count = run_export(config)
    except RuntimeError as exc:
        sys.exit(str(exc))

    if config.dry_run:
        log.info("Dry run: %d users. No file written.", count)
        return
    log.info("Wrote %d users to %s", count, config.output)
    log.info(
        "Point Claude Compliance at this file with ENTRA_EXTRACT_PATH=%s "
        "before the first sync.",
        config.output,
    )


if __name__ == "__main__":
    main()
