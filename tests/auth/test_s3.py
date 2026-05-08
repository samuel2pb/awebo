from __future__ import annotations

from awebo.auth.base import S3Credentials
from awebo.auth.s3 import S3AuthProvider
from awebo.config import S3Settings


def test_resolve_populates_s3_credentials() -> None:
    provider = S3AuthProvider(
        S3Settings(
            endpoint="http://localhost:9000",
            path_style=True,
            access_key_id="minio",
            secret_access_key="minio123",
            region="us-east-1",
        )
    )
    creds = provider.resolve()
    assert creds.access_key_id == "minio"
    assert creds.region == "us-east-1"
    assert creds.s3 is not None
    assert isinstance(creds.s3, S3Credentials)
    assert creds.s3.endpoint == "http://localhost:9000"
    assert creds.s3.path_style is True


def test_resolve_caches() -> None:
    provider = S3AuthProvider(S3Settings(region="r"))
    a = provider.resolve()
    b = provider.resolve()
    assert a is b


def test_resolve_no_endpoint_s3_still_present() -> None:
    provider = S3AuthProvider(S3Settings(region="r"))
    creds = provider.resolve()
    assert creds.s3 is not None
    assert creds.s3.endpoint is None
    assert creds.s3.path_style is False
