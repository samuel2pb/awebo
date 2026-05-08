from __future__ import annotations

from awebo.auth.base import Credentials, S3Credentials
from awebo.catalog.base import AttachSpec
from awebo.catalog.rest import RestCatalogBackend
from awebo.config import CatalogSettings, RestSettings


def _backend(token: str | None = None) -> RestCatalogBackend:
    catalog = CatalogSettings(type="rest", name="lake", warehouse="s3://wh/")
    rest = RestSettings(uri="http://localhost:8181", warehouse="s3://wh/", token=token)
    return RestCatalogBackend(catalog, rest)


def test_attach_sql_minio_path_style() -> None:
    """REST emits only an S3 secret — no ATTACH."""
    backend = _backend()
    creds = Credentials(
        region="us-east-1",
        access_key_id="minio",
        secret_access_key="minio123",
        s3=S3Credentials(
            endpoint="http://localhost:9000",
            path_style=True,
            access_key_id="minio",
            secret_access_key="minio123",
            region="us-east-1",
        ),
    )
    spec = backend.duckdb_attach_sql(creds)
    assert isinstance(spec, AttachSpec)
    assert len(spec.secrets) == 1
    secret = spec.secrets[0]
    assert secret.startswith("CREATE OR REPLACE SECRET")
    assert "ENDPOINT 'localhost:9000'" in secret
    assert "URL_STYLE 'path'" in secret
    assert "USE_SSL FALSE" in secret
    assert "KEY_ID 'minio'" in secret
    assert "REGION 'us-east-1'" in secret
    assert spec.attach is None


def test_attach_sql_https_endpoint_uses_ssl() -> None:
    backend = _backend()
    creds = Credentials(
        region="us-east-1",
        s3=S3Credentials(endpoint="https://s3.example.com"),
    )
    spec = backend.duckdb_attach_sql(creds)
    assert len(spec.secrets) == 1
    assert "ENDPOINT 's3.example.com'" in spec.secrets[0]
    assert "USE_SSL TRUE" in spec.secrets[0]


def test_attach_sql_empty_when_no_s3_creds() -> None:
    backend = _backend()
    spec = backend.duckdb_attach_sql(Credentials())
    assert isinstance(spec, AttachSpec)
    assert spec.secrets == []
    assert spec.attach is None


def test_attach_sql_token_does_not_leak_into_duckdb() -> None:
    """REST bearer tokens are pyiceberg-only."""
    backend = _backend(token="tok123")
    spec = backend.duckdb_attach_sql(Credentials())
    for s in spec.secrets:
        assert "tok123" not in s
