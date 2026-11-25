import sys

from .config import Config
from .sync import run_sync


def main() -> None:
    config = Config.from_env_and_args()
    summary = run_sync(config)
    if summary.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
