"""AWS session of both connectors: the standard credential chain, or a role.

Without ``AWS_ROLE_ARN`` boto3 resolves credentials as usual (AWS_PROFILE,
environment keys, SSO, instance role). With it, the session assumes the role
and renews its credentials before they expire, so a run longer than the role
session (1 hour by default) does not fail midway.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

if TYPE_CHECKING:
    from botocore.config import Config as BotoConfig

    from .config import CommonConfig


def aws_session(
    config: CommonConfig, *, session_name: str, boto_config: BotoConfig
) -> boto3.Session:
    base = boto3.Session(region_name=config.region)
    if not config.aws_role_arn:
        return base
    sts = base.client("sts", config=boto_config)
    request = {"RoleArn": config.aws_role_arn, "RoleSessionName": session_name}
    if config.aws_external_id:
        request["ExternalId"] = config.aws_external_id

    def refresh() -> dict[str, str]:
        credentials = sts.assume_role(**request)["Credentials"]  # type: ignore[arg-type]
        return {
            "access_key": credentials["AccessKeyId"],
            "secret_key": credentials["SecretAccessKey"],
            "token": credentials["SessionToken"],
            "expiry_time": credentials["Expiration"].isoformat(),
        }

    renewable = RefreshableCredentials.create_from_metadata(
        metadata=refresh(), refresh_using=refresh, method="sts-assume-role"
    )
    botocore_session = get_session()
    # botocore has no public setter for refreshable credentials.
    botocore_session._credentials = renewable  # type: ignore[attr-defined]  # noqa: SLF001
    return boto3.Session(botocore_session=botocore_session, region_name=config.region)
