from __future__ import annotations

import pytest

from awebo.auth.aws import AwsAuthProvider
from awebo.config import AwsSettings


def test_resolve_uses_static_keys() -> None:
    settings = AwsSettings(
        region="us-east-1",
        access_key_id="AKIA",
        secret_access_key="SECRET",
    )
    provider = AwsAuthProvider(settings)
    creds = provider.resolve()
    assert creds.region == "us-east-1"
    assert creds.access_key_id == "AKIA"
    assert creds.secret_access_key == "SECRET"


def test_resolve_caches_credentials() -> None:
    settings = AwsSettings(
        region="us-east-1",
        access_key_id="AKIA",
        secret_access_key="SECRET",
    )
    provider = AwsAuthProvider(settings)
    first = provider.resolve()
    second = provider.resolve()
    assert first is second  # identity, not just equality — proves caching


@pytest.mark.integration
def test_resolve_assume_role_uses_moto() -> None:
    pytest.importorskip("moto")
    from moto import mock_aws

    with mock_aws():
        settings = AwsSettings(
            region="us-east-1",
            access_key_id="AKIA",
            secret_access_key="SECRET",
            role_arn="arn:aws:iam::123456789012:role/awebo-test",
        )
        provider = AwsAuthProvider(settings)
        creds = provider.resolve()
        assert creds.access_key_id is not None
        assert creds.session_token is not None
