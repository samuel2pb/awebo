from __future__ import annotations

from awebo.auth.s3 import S3AuthProvider
from awebo.config import S3Settings


def test_resolve_packs_endpoint_into_extra() -> None:
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
    assert creds.extra is not None
    assert creds.extra["s3.endpoint"] == "http://localhost:9000"
    assert creds.extra["s3.path-style-access"] == "true"


def test_resolve_caches() -> None:
    provider = S3AuthProvider(S3Settings(region="r"))
    a = provider.resolve()
    b = provider.resolve()
    assert a is b


def test_resolve_no_endpoint_no_extra() -> None:
    provider = S3AuthProvider(S3Settings(region="r"))
    creds = provider.resolve()
    assert creds.extra is None
