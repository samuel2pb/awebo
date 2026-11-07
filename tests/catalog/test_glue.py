from __future__ import annotations

from awebo.auth.base import Credentials
from awebo.catalog.base import AttachSpec
from awebo.catalog.glue import GlueCatalogBackend
from awebo.config import CatalogSettings


def _settings() -> CatalogSettings:
    return CatalogSettings(type="glue", name="lake", warehouse="s3://b/wh")


def test_constructor_accepts_none_warehouse() -> None:
    """Warehouse validation lives in load_config(), not the backend constructor."""
    backend = GlueCatalogBackend(CatalogSettings(type="glue", name="lake", warehouse=None))
    assert backend.name == "lake"


def test_duckdb_attach_sql_returns_attach_spec() -> None:
    backend = GlueCatalogBackend(_settings())
    creds = Credentials(
        region="us-east-1",
        access_key_id="AKIA",
        secret_access_key="SECRET",
    )
    spec = backend.duckdb_attach_sql(creds)
    assert isinstance(spec, AttachSpec)
    assert len(spec.secrets) == 1
    assert spec.secrets[0].startswith("CREATE OR REPLACE SECRET")
    assert "AKIA" in spec.secrets[0]
    assert spec.attach is not None
    assert "TYPE iceberg" in spec.attach
    assert "ENDPOINT_TYPE 'glue'" in spec.attach


def test_duckdb_attach_sql_handles_missing_session_token() -> None:
    backend = GlueCatalogBackend(_settings())
    creds = Credentials(region="us-east-1", access_key_id="AKIA", secret_access_key="S")
    spec = backend.duckdb_attach_sql(creds)
    assert "SESSION_TOKEN" not in spec.secrets[0]


def test_china_region_attaches_by_explicit_sigv4_uri() -> None:
    """ENDPOINT_TYPE 'glue' builds a *.amazonaws.com host, wrong for cn-* regions."""
    spec = GlueCatalogBackend(_settings()).duckdb_attach_sql(Credentials(region="cn-north-1"))
    assert spec.attach is not None
    assert "URI 'glue.cn-north-1.amazonaws.com.cn/iceberg'" in spec.attach
    assert "AUTHORIZATION_TYPE 'sigv4'" in spec.attach
    assert "ENDPOINT_TYPE" not in spec.attach


def test_new_tables_land_under_the_warehouse() -> None:
    backend = GlueCatalogBackend(_settings())
    assert backend.table_location("sales", "orders") == "s3://b/wh/sales.db/orders"


def test_no_warehouse_lets_the_catalog_choose_the_location() -> None:
    backend = GlueCatalogBackend(CatalogSettings(type="glue", name="lake"))
    assert backend.table_location("sales", "orders") is None
