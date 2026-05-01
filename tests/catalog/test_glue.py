from __future__ import annotations

import pytest

from awebo.auth.base import Credentials
from awebo.catalog.glue import GlueCatalogBackend
from awebo.config import CatalogSettings
from awebo.exceptions import CatalogError


def _settings() -> CatalogSettings:
    return CatalogSettings(type="glue", name="lake", warehouse="s3://b/wh")


def test_requires_warehouse() -> None:
    with pytest.raises(CatalogError, match="warehouse"):
        GlueCatalogBackend(CatalogSettings(type="glue", name="lake", warehouse=None))


def test_duckdb_attach_sql_is_batched() -> None:
    backend = GlueCatalogBackend(_settings())
    creds = Credentials(
        region="us-east-1",
        access_key_id="AKIA",
        secret_access_key="SECRET",
    )
    statements = backend.duckdb_attach_sql(creds)
    # No D+1: exactly two statements (secret + attach), independent of cred fields.
    assert len(statements) == 2
    assert statements[0].startswith("CREATE OR REPLACE SECRET")
    assert "TYPE iceberg" in statements[1]
    assert "ENDPOINT_TYPE 'glue'" in statements[1]
    assert "AKIA" in statements[0]


def test_duckdb_attach_sql_handles_missing_session_token() -> None:
    backend = GlueCatalogBackend(_settings())
    creds = Credentials(region="us-east-1", access_key_id="AKIA", secret_access_key="S")
    statements = backend.duckdb_attach_sql(creds)
    assert "SESSION_TOKEN" not in statements[0]
