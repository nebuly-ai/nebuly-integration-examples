"""``python -m bedrock_sync`` only says which connector to run."""

import sys

USAGE = """Choose a connector:
  python -m bedrock_sync.runtime    Bedrock model invocation logs on S3
  python -m bedrock_sync.agentcore  AgentCore observability on CloudWatch
Add --help to either for its options."""

if __name__ == "__main__":
    print(USAGE, file=sys.stderr)  # noqa: T201 - CLI output
    raise SystemExit(2)
