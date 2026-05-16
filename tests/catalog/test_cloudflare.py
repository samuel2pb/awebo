"""Tests for CloudflareCatalogBackend — URL derivation and DuckDB secret generation."""

from __future__ import annotations

from awebo.auth.base import Credentials, S3Credentials
from awebo.catalog.cloudflare import CloudflareCatalogBackend
from awebo.config import CatalogSettings, CloudflareSettings


def _cf_backend(
    jurisdiction: str | None = None,
    catalog_endpoint: str | None = None,
) -> CloudflareCatalogBackend:
    catalog = CatalogSettings(
        type="cloudflare",
        name="lake",
        warehouse="s3://my-lake/",
    )
    cf = CloudflareSettings(
        account_id="abc123",
        r2_access_key_id="r2key",
        r2_secret_access_key="r2secret",
        bucket="my-lake",
        jurisdiction=jurisdiction,
        catalog_endpoint=catalog_endpoint,
    )
    return CloudflareCatalogBackend(catalog, cf)


def _r2_creds(
    endpoint: str = "https://abc123.r2.cloudflarestorage.com",
) -> Credentials:
    return Credentials(
        region="auto",
        access_key_id="r2key",
        secret_access_key="r2secret",
        s3=S3Credentials(
            endpoint=endpoint,
            path_style=False,
            access_key_id="r2key",
            secret_access_key="r2secret",
            region="auto",
        ),
    )


def test_duckdb_secret_has_r2_endpoint() -> None:
    backend = _cf_backend()
    spec = backend.duckdb_attach_sql(_r2_creds())
    assert len(spec.secrets) == 1
    secret = spec.secrets[0]
    assert "ENDPOINT 'abc123.r2.cloudflarestorage.com'" in secret
    assert "USE_SSL TRUE" in secret
    assert "KEY_ID 'r2key'" in secret
    assert "SECRET 'r2secret'" in secret
    assert spec.attach is None


def test_duckdb_secret_no_path_style() -> None:
    backend = _cf_backend()
    spec = backend.duckdb_attach_sql(_r2_creds())
    assert "URL_STYLE" not in spec.secrets[0]


def test_eu_jurisdiction_endpoint() -> None:
    backend = _cf_backend(jurisdiction="eu")
    assert backend._r2_endpoint == "https://abc123.eu.r2.cloudflarestorage.com"


def test_catalog_endpoint_override() -> None:
    backend = _cf_backend(catalog_endpoint="https://custom.example.com")
    assert backend._rest_uri == "https://custom.example.com"


def test_catalog_endpoint_defaults_to_r2() -> None:
    backend = _cf_backend()
    assert backend._rest_uri == "https://catalog.cloudflarestorage.com/abc123/my-lake"


def test_type_is_cloudflare() -> None:
    backend = _cf_backend()
    assert backend.type == "cloudflare"
