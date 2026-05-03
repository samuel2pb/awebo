"""AWS auth provider.

Resolves credentials in the standard boto3 chain (profile -> env -> instance
metadata) and optionally assumes a role on top. The STS call, when needed,
happens once during :meth:`resolve` and the result is cached on the provider
instance so callers can share it with both PyIceberg and DuckDB without
re-fetching (no D+1).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo.auth.base import AuthProvider, Credentials
from awebo.exceptions import AuthError

if TYPE_CHECKING:
    from awebo.config import AwsSettings

log = logging.getLogger(__name__)


class AwsAuthProvider(AuthProvider):
    cloud = "aws"

    def __init__(self, settings: AwsSettings) -> None:
        self._settings = settings
        self._cached: Credentials | None = None

    def resolve(self) -> Credentials:
        if self._cached is not None:
            return self._cached

        log.debug("resolving AWS credentials (region=%s)", self._settings.region)

        import boto3
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            session = boto3.Session(
                aws_access_key_id=self._settings.access_key_id,
                aws_secret_access_key=self._settings.secret_access_key,
                aws_session_token=self._settings.session_token,
                region_name=self._settings.region,
                profile_name=self._settings.profile,
            )

            if self._settings.role_arn:
                log.debug("assuming role %s", self._settings.role_arn)
                sts = session.client("sts")
                resp = sts.assume_role(
                    RoleArn=self._settings.role_arn,
                    RoleSessionName="awebo-lakehouse",
                )
                creds = resp["Credentials"]
                self._cached = Credentials(
                    region=self._settings.region,
                    access_key_id=creds["AccessKeyId"],
                    secret_access_key=creds["SecretAccessKey"],
                    session_token=creds["SessionToken"],
                )
                log.debug("credentials resolved via assumed role")
                return self._cached

            chain = session.get_credentials()
            if chain is None:
                raise AuthError("no AWS credentials available in default chain")
            frozen = chain.get_frozen_credentials()
            self._cached = Credentials(
                region=self._settings.region,
                access_key_id=frozen.access_key,
                secret_access_key=frozen.secret_key,
                session_token=frozen.token,
            )
            log.debug("credentials resolved via default chain")
            return self._cached
        except (BotoCoreError, ClientError) as exc:
            raise AuthError(f"AWS credential resolution failed: {exc}") from exc
