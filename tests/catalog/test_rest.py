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
    """Object-store creds become one S3 secret; the catalog is attached."""
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
    assert spec.attach == (
        "ATTACH 's3://wh/' AS \"lake\" "
        "(TYPE iceberg, URI 'http://localhost:8181', AUTHORIZATION_TYPE 'none');"
    )


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


def test_attach_sql_without_creds_or_auth_attaches_anonymously() -> None:
    spec = _backend().duckdb_attach_sql(Credentials())
    assert spec.secrets == []
    assert spec.attach is not None
    assert "AUTHORIZATION_TYPE 'none'" in spec.attach


def test_bearer_token_becomes_iceberg_secret() -> None:
    spec = _backend(token="tok123").duckdb_attach_sql(Credentials())
    assert spec.secrets == [
        "CREATE OR REPLACE SECRET \"awebo_lake_iceberg\" (TYPE iceberg, TOKEN 'tok123');"
    ]
    assert spec.attach is not None
    assert 'SECRET "awebo_lake_iceberg"' in spec.attach
    assert "AUTHORIZATION_TYPE" not in spec.attach


def test_oauth_credential_becomes_client_credentials_secret() -> None:
    catalog = CatalogSettings(type="rest", name="lake")
    rest = RestSettings(
        uri="https://cat.example.com/",
        warehouse="wh",
        oauth_credential="client:s3cr'et",
    )
    spec = RestCatalogBackend(catalog, rest).duckdb_attach_sql(Credentials())
    (secret,) = spec.secrets
    assert "CLIENT_ID 'client'" in secret
    assert "CLIENT_SECRET 's3cr''et'" in secret
    assert "OAUTH2_SERVER_URI 'https://cat.example.com/v1/oauth/tokens'" in secret


def test_secret_values_are_quoted() -> None:
    creds = Credentials(access_key_id="AK", secret_access_key="se'cret")
    spec = _backend().duckdb_attach_sql(creds)
    assert "SECRET 'se''cret'" in spec.secrets[0]
