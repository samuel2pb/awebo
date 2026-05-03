"""DuckDB query engine wrapper.

One DuckDB connection per Lakehouse. The iceberg + httpfs extensions are
expected to be pre-installed in deployment images (Docker layer or wheel
prebake) but we still issue ``LOAD`` to make sure they are available in this
session. Catalog ATTACH statements are batched in a single ``executemany``
to honour the no-D+1 rule.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo.exceptions import QueryError

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

    from awebo.auth.base import Credentials
    from awebo.catalog.base import CatalogBackend

log = logging.getLogger(__name__)


class QueryEngine:
    """Thin wrapper around a DuckDB connection."""

    def __init__(self, temp_dir: str | None = None) -> None:
        import duckdb

        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(":memory:")
        # Iceberg + httpfs assumed pre-installed in image; LOAD is cheap.
        self._conn.execute("LOAD iceberg;")
        self._conn.execute("LOAD httpfs;")
        if temp_dir:
            self._conn.execute(f"PRAGMA temp_directory='{temp_dir}';")
        log.debug("DuckDB engine initialized")

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def attach_catalog(self, catalog: CatalogBackend, credentials: Credentials) -> None:
        """Run all catalog setup SQL in one batch."""
        statements = catalog.duckdb_attach_sql(credentials)
        log.debug("attaching catalog (%d statements)", len(statements))
        try:
            for stmt in statements:
                self._conn.execute(stmt)
        except Exception as exc:
            raise QueryError(f"catalog ATTACH failed: {exc}") from exc

    def execute(self, sql: str) -> duckdb.DuckDBPyRelation:
        log.debug("execute: %.100s", sql)
        try:
            return self._conn.sql(sql)
        except Exception as exc:
            raise QueryError(str(exc)) from exc

    def register_arrow(
        self,
        alias: str,
        table: pa.Table | pa.RecordBatchReader,
    ) -> None:
        try:
            self._conn.register(alias, table)
        except Exception as exc:
            raise QueryError(f"failed to register {alias!r}: {exc}") from exc

    def close(self) -> None:
        log.debug("closing DuckDB connection")
        self._conn.close()
