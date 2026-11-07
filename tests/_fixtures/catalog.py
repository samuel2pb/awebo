"""Local Iceberg catalog used by pipeline tests.

PyIceberg's SQLite-backed ``SqlCatalog`` over a local-filesystem warehouse,
served to DuckDB through the in-process REST catalog in
:mod:`tests._fixtures.rest_catalog`. DuckDB attaches it with the production
:class:`RestCatalogBackend` SQL, so the writer under test is the real one.

``pyiceberg()`` returns the ``SqlCatalog`` itself: tests read writes back
through it, independently of DuckDB.
"""

from __future__ import annotations

import weakref
from typing import TYPE_CHECKING

from pyiceberg.catalog.sql import SqlCatalog

from awebo.catalog.base import AttachSpec, CatalogBackend
from awebo.catalog.rest import RestCatalogBackend
from awebo.config import CatalogSettings, RestSettings
from tests._fixtures.rest_catalog import RestCatalogServer

if TYPE_CHECKING:
    from pathlib import Path

    from awebo.auth.base import Credentials


class LocalIcebergCatalogBackend(CatalogBackend):
    type = "local"

    def __init__(self, name: str, warehouse: Path) -> None:
        self.name = name
        warehouse.mkdir(parents=True, exist_ok=True)
        self._sql_catalog = SqlCatalog(
            name,
            uri=f"sqlite:///{warehouse / 'catalog.db'}",
            warehouse=f"file://{warehouse}",
        )
        self._server = RestCatalogServer(self._sql_catalog)
        weakref.finalize(self, self._server.close)
        self._rest = RestCatalogBackend(
            CatalogSettings(type="rest", name=name),
            RestSettings(uri=self._server.uri, warehouse=name),
        )

    def pyiceberg(self, credentials: Credentials) -> SqlCatalog:
        del credentials
        return self._sql_catalog

    def duckdb_attach_sql(self, credentials: Credentials) -> AttachSpec:
        return self._rest.duckdb_attach_sql(credentials)

    def close(self) -> None:
        self._server.close()
