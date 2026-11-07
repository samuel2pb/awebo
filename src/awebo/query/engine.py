"""DuckDB query engine wrapper.

One DuckDB connection per Lakehouse. It owns both the read path (``SELECT``
over attached Iceberg catalogs and external files) and, since DuckDB 2.0, the
write path (``CREATE TABLE`` / ``INSERT`` / ``DELETE`` / ``ALTER TABLE``
against the attached Iceberg catalog).

The iceberg + httpfs extensions (and their ``avro`` dependency) are expected
to be pre-installed in deployment images. The engine ``LOAD``s them and never
``INSTALL``s on the hot path. Catalog secrets + ATTACH run once, in order.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

from awebo.exceptions import QueryError

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    import duckdb
    import polars as pl
    import pyarrow as pa

    from awebo.auth.base import Credentials
    from awebo.catalog.base import CatalogBackend

type SqlParam = bool | int | float | str | None
type ScanSource = "pa.Table | pa.RecordBatchReader | pl.DataFrame | pl.LazyFrame"

log = logging.getLogger(__name__)

_EXTENSIONS: tuple[str, ...] = ("httpfs", "iceberg")


class QueryEngine:
    """Thin wrapper around a DuckDB connection."""

    def __init__(self, temp_dir: str | None = None, memory_limit: str | None = None) -> None:
        import duckdb

        config: dict[str, str | bool | int | float | list[str]] = {}
        if temp_dir:
            config["temp_directory"] = temp_dir
        if memory_limit:
            config["memory_limit"] = memory_limit
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(":memory:", config=config)
        for ext in _EXTENSIONS:
            self._conn.load_extension(ext)
        log.debug("DuckDB %s engine initialized", duckdb.__version__)

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def attach_catalog(self, catalog: CatalogBackend, credentials: Credentials) -> None:
        """Run catalog secrets, then the catalog ATTACH."""
        spec = catalog.duckdb_attach_sql(credentials)
        log.debug(
            "attaching catalog (secrets=%d attach=%s)",
            len(spec.secrets),
            spec.attach is not None,
        )
        try:
            for secret in spec.secrets:
                self._conn.sql(secret)
            if spec.attach is not None:
                self._conn.sql(spec.attach)
        except Exception as exc:
            raise QueryError(f"catalog ATTACH failed: {exc}") from exc

    def execute(
        self, sql: str, params: Sequence[SqlParam] | None = None
    ) -> duckdb.DuckDBPyRelation:
        """Return a lazy relation for ``sql``.

        DuckDB 2.0 relations re-run their query on every read; statements
        (DDL / DML) run at call time.
        """
        log.debug("execute: %.100s", sql)
        try:
            return self._conn.sql(sql, params=params)
        except Exception as exc:
            raise QueryError(str(exc)) from exc

    def fetch_rows(
        self, sql: str, params: Sequence[SqlParam] | None = None
    ) -> list[tuple[object, ...]]:
        """Run ``sql`` and return every row as a tuple."""
        rows: list[tuple[object, ...]] = self.execute(sql, params).fetchall()
        return rows

    def register(self, alias: str, source: ScanSource) -> None:
        """Expose an in-memory Arrow / Polars object to SQL as ``alias``.

        DuckDB scans it zero-copy and pulls ``RecordBatchReader`` / ``LazyFrame``
        sources on demand, so nothing is materialised up front.
        """
        try:
            self._conn.register(alias, source)
        except Exception as exc:
            raise QueryError(f"failed to register {alias!r}: {exc}") from exc

    def register_arrow(self, alias: str, table: pa.Table | pa.RecordBatchReader) -> None:
        self.register(alias, table)

    def unregister(self, alias: str) -> None:
        self._conn.unregister(alias)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Run the block in one DuckDB transaction; roll back on any error.

        Against an attached Iceberg catalog, every statement in the block
        lands in a single Iceberg commit.
        """
        self._conn.begin()
        try:
            yield
        except BaseException:
            self._conn.rollback()
            raise
        self._conn.commit()

    def close(self) -> None:
        log.debug("closing DuckDB connection")
        self._conn.close()
