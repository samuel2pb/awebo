from __future__ import annotations

from awebo.auth.base import Credentials
from awebo.catalog.rest import RestCatalogBackend
from awebo.config import CatalogSettings, RestSettings


def _backend(token: str | None = None) -> RestCatalogBackend:
    catalog = CatalogSettings(type="rest", name="lake", warehouse="s3://wh/")
    rest = RestSettings(uri="http://localhost:8181", warehouse="s3://wh/", token=token)
    return RestCatalogBackend(catalog, rest)


def test_attach_sql_minio_path_style() -> None:
    """REST emits only an S3 secret — no ATTACH (DuckDB iceberg 1.5.x has
    no REST endpoint type). The secret carries enough info for
    ``iceberg_scan('s3://...')`` reads to authenticate against MinIO.
    """
    backend = _backend()
    creds = Credentials(
        region="us-east-1",
        access_key_id="minio",
        secret_access_key="minio123",
        extra={"s3.endpoint": "http://localhost:9000", "s3.path-style-access": "true"},
    )
    stmts = backend.duckdb_attach_sql(creds)
    assert len(stmts) == 1
    secret = stmts[0]
    assert secret.startswith("CREATE OR REPLACE SECRET")
    assert "ENDPOINT 'localhost:9000'" in secret
    assert "URL_STYLE 'path'" in secret
    assert "USE_SSL FALSE" in secret
    assert "KEY_ID 'minio'" in secret
    assert "REGION 'us-east-1'" in secret
    # Critical: no ATTACH statement — DuckDB iceberg 1.5.x rejects
    # ENDPOINT_TYPE 'iceberg_rest'.
    assert not any("ATTACH" in s for s in stmts)


def test_attach_sql_https_endpoint_uses_ssl() -> None:
    backend = _backend()
    creds = Credentials(
        region="us-east-1",
        extra={"s3.endpoint": "https://s3.example.com"},
    )
    secret = backend.duckdb_attach_sql(creds)[0]
    assert "ENDPOINT 's3.example.com'" in secret
    assert "USE_SSL TRUE" in secret


def test_attach_sql_returns_nothing_when_no_s3_creds() -> None:
    backend = _backend()
    assert backend.duckdb_attach_sql(Credentials()) == []


def test_attach_sql_token_does_not_leak_into_duckdb() -> None:
    """REST bearer tokens are pyiceberg-only — no ATTACH means the token
    must not appear in any DuckDB statement.
    """
    backend = _backend(token="tok123")
    stmts = backend.duckdb_attach_sql(Credentials())
    assert all("tok123" not in s for s in stmts)
