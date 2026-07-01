from .config import Config
from .sync import run_sync


def main() -> None:
    config = Config.from_env_and_args()
    run_sync(config)


if __name__ == "__main__":
    main()
