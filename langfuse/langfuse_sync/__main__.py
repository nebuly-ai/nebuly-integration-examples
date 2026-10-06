from langfuse_sync.config import Config
from langfuse_sync.sync import run_sync


def main() -> None:
    config = Config.from_env_and_args()
    raise SystemExit(run_sync(config))


if __name__ == "__main__":
    main()
