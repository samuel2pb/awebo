from __future__ import annotations

from awebo.auth.base import Credentials
from awebo.catalog.rest import RestCatalogBackend
from awebo.config import CatalogSettings, RestSettings


def _backend(token: str | None = None) -> RestCatalogBackend:
    catalog = CatalogSettings(type="rest", name="lake", warehouse="s3://wh/")
    rest = RestSettings(uri="http://localhost:8181", warehouse="s3://wh/", token=token)
    return RestCatalogBackend(catalog, rest)


def test_attach_sql_minio_path_style() -> None:
    backend = _backend()
    creds = Credentials(
        region="us-east-1",
        access_key_id="minio",
        secret_access_key="minio123",
        extra={"s3.endpoint": "http://localhost:9000", "s3.path-style-access": "true"},
    )
    stmts = backend.duckdb_attach_sql(creds)
    assert len(stmts) == 2
    secret, attach = stmts
    assert secret.startswith("CREATE OR REPLACE SECRET")
    assert "ENDPOINT 'localhost:9000'" in secret
    assert "URL_STYLE 'path'" in secret
    assert "USE_SSL FALSE" in secret
    assert "KEY_ID 'minio'" in secret
    assert "REGION 'us-east-1'" in secret
    assert attach.startswith("ATTACH 'http://localhost:8181'")
    assert "TYPE iceberg" in attach
    assert "ENDPOINT_TYPE 'iceberg_rest'" in attach


def test_attach_sql_https_endpoint_uses_ssl() -> None:
    backend = _backend()
    creds = Credentials(
        region="us-east-1",
        extra={"s3.endpoint": "https://s3.example.com"},
    )
    secret = backend.duckdb_attach_sql(creds)[0]
    assert "ENDPOINT 's3.example.com'" in secret
    assert "USE_SSL TRUE" in secret


def test_attach_sql_omits_secret_when_empty() -> None:
    backend = _backend()
    creds = Credentials()
    stmts = backend.duckdb_attach_sql(creds)
    assert len(stmts) == 1
    assert stmts[0].startswith("ATTACH")


def test_attach_includes_token_when_set() -> None:
    backend = _backend(token="tok123")
    stmts = backend.duckdb_attach_sql(Credentials())
    attach = stmts[-1]
    assert "TOKEN 'tok123'" in attach
