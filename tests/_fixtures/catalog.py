"""Local SQLite-backed Iceberg catalog used by pipeline tests.

Wraps PyIceberg's ``SqlCatalog`` against an on-disk sqlite metastore plus a
local-filesystem warehouse. Implements the awebo :class:`CatalogBackend`
contract so the production ``IcebergWriter`` exercises the exact same code
path tests do.

DuckDB ATTACH is a no-op here: tests verify writes by re-loading the table
via PyIceberg directly. The production Glue path is still covered by
moto-backed integration tests.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import load_catalog

from awebo.catalog.base import AttachSpec, CatalogBackend

if TYPE_CHECKING:
    from pathlib import Path

    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials


class LocalIcebergCatalogBackend(CatalogBackend):
    type = "local"

    def __init__(self, name: str, warehouse: Path) -> None:
        self.name = name
        self._warehouse = warehouse
        self._db_path = warehouse / "catalog.db"
        warehouse.mkdir(parents=True, exist_ok=True)
        self._pyiceberg: PyIcebergCatalog | None = None

    def pyiceberg(self, credentials: Credentials) -> PyIcebergCatalog:
        del credentials
        if self._pyiceberg is None:
            self._pyiceberg = load_catalog(
                self.name,
                **{
                    "type": "sql",
                    "uri": f"sqlite:///{self._db_path}",
                    "warehouse": f"file://{self._warehouse}",
                },
            )
        return self._pyiceberg

    def duckdb_attach_sql(self, credentials: Credentials) -> AttachSpec:
        del credentials
        return AttachSpec()
