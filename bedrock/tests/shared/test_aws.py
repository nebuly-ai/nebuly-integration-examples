"""The AWS session both connectors use."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.stub import Stubber

from bedrock_sync.shared import aws

if TYPE_CHECKING:
    import pytest


def _config(**changes: object) -> Any:  # noqa: ANN401 - a stand-in config
    values = {"region": "eu-north-1", "aws_role_arn": None, "aws_external_id": None}
    return SimpleNamespace(**{**values, **changes})


def test_without_a_role_the_standard_chain_is_used() -> None:
    session = aws.aws_session(_config(), session_name="test", boto_config=BotoConfig())
    assert session.region_name == "eu-north-1"


def test_role_credentials_are_renewed_before_they_expire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sts = boto3.client(
        "sts",
        region_name="eu-north-1",
        aws_access_key_id="base",
        aws_secret_access_key="base",
    )
    real_session = boto3.Session
    base = real_session(region_name="eu-north-1")
    monkeypatch.setattr(base, "client", lambda *_a, **_k: sts)

    def fake_session(**kwargs: Any) -> boto3.Session:  # noqa: ANN401
        # The first session reaches STS; the role session is a real one.
        return real_session(**kwargs) if "botocore_session" in kwargs else base

    monkeypatch.setattr("bedrock_sync.shared.aws.boto3.Session", fake_session)
    config = _config(
        aws_role_arn="arn:aws:iam::123456789012:role/reader", aws_external_id="ext-1"
    )
    expected = {
        "RoleArn": config.aws_role_arn,
        "RoleSessionName": "test-session",
        "ExternalId": "ext-1",
    }

    def credentials(key: str, minutes: int) -> dict[str, Any]:
        return {
            "Credentials": {
                "AccessKeyId": key,
                "SecretAccessKey": "secret-key-value",
                "SessionToken": "token",
                "Expiration": datetime.now(UTC) + timedelta(minutes=minutes),
            }
        }

    with Stubber(sts) as stubber:
        # The first credentials are about to expire, so botocore renews them.
        for key, minutes in (("AKIAFIRST0000000", 1), ("AKIASECOND000000", 60)):
            stubber.add_response("assume_role", credentials(key, minutes), expected)
        session = aws.aws_session(
            config, session_name="test-session", boto_config=BotoConfig()
        )
        credentials_now = session.get_credentials()
        assert credentials_now is not None
        frozen = credentials_now.get_frozen_credentials()
        stubber.assert_no_pending_responses()
    assert frozen.access_key == "AKIASECOND000000"
    assert session.region_name == "eu-north-1"
