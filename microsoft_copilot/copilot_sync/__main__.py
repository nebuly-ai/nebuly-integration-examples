import asyncio

from .config import Config
from .sync import run_sync


def main() -> None:
    asyncio.run(run_sync(Config.from_env_and_args()))


if __name__ == "__main__":
    main()
