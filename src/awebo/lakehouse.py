"""User-facing Lakehouse facade.

This module is intentionally thin. All real work lives in the feature
packages (``catalog``, ``auth``, ``query``, ``io``, ``streaming``); the
:class:`Lakehouse` is just the wiring that loads config once, resolves
credentials once, attaches the catalog to DuckDB once, and exposes the
ergonomic verbs ``sql``, ``read_file``, ``to_lake``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

from awebo.auth.aws import AwsAuthProvider
from awebo.auth.s3 import S3AuthProvider
from awebo.catalog.glue import GlueCatalogBackend
from awebo.catalog.rest import RestCatalogBackend
from awebo.config import LakehouseConfig, load_config
from awebo.exceptions import ConfigError
from awebo.io.reader import FileReader, ReaderOptions
from awebo.io.writer import IcebergWriter, WriteMode, WriteSource
from awebo.query.engine import QueryEngine
from awebo.query.result import QueryResult

if TYPE_CHECKING:
    from types import TracebackType

    from pyiceberg.catalog import Catalog as PyIcebergCatalog
    from pyiceberg.partitioning import PartitionSpec as PyIcebergPartitionSpec
    from pyiceberg.schema import Schema as PyIcebergSchema

    from awebo.auth.base import AuthProvider, Credentials
    from awebo.catalog.base import CatalogBackend


class Lakehouse:
    """Single entry point for awebo.

    Example
    -------
    >>> from awebo import Lakehouse
    >>> lake = Lakehouse()
    >>> df = lake.sql("select * from lake.sales.orders").to_polars()
    >>> lake.read_file("s3://bucket/raw.parquet", alias="raw")
    >>> lake.to_lake(df, database="sales", table="orders")
    """

    def __init__(
        self,
        config: LakehouseConfig | None = None,
        *,
        catalog: CatalogBackend | None = None,
        auth: AuthProvider | None = None,
    ) -> None:
        """Wire a Lakehouse.

        ``catalog`` and ``auth`` exist as injection seams for tests and for
        callers that already manage their own backends (e.g. a long-running
        server reusing one ``CatalogBackend`` across many lakehouses). When
        injected, they bypass the env-var-driven build, but ``config`` is
        still honoured for streaming / temp-dir tunables.
        """
        self._config = config or load_config()
        self._auth: AuthProvider = auth or self._build_auth(self._config)
        self._credentials: Credentials = self._auth.resolve()
        self._catalog: CatalogBackend = catalog or self._build_catalog(self._config)

        self._engine = QueryEngine(temp_dir=self._config.temp_dir)
        self._engine.attach_catalog(self._catalog, self._credentials)

        self._reader = FileReader(self._engine)
        self._writer = IcebergWriter(
            engine=self._engine,
            catalog=self._catalog,
            credentials=self._credentials,
            streaming_threshold_mb=self._config.streaming_threshold_mb,
        )

    # ---------------------------------------------------------------- public
    def sql(self, query: str) -> QueryResult:
        """Execute a SQL query against DuckDB and return a lazy result."""
        return QueryResult(self._engine.execute(query))

    def read_file(
        self,
        path: str,
        alias: str,
        options: ReaderOptions | None = None,
    ) -> None:
        """Register an external file (parquet/csv/json/...) as a DuckDB view.

        Format is auto-detected from the path suffix. Use ``options`` to pass
        DuckDB reader kwargs (e.g. ``{"header": True}``).
        """
        self._reader.register(path, alias, options)

    def to_lake(
        self,
        data: WriteSource,
        database: str,
        table: str,
        *,
        mode: WriteMode = "append",
        schema: PyIcebergSchema | None = None,
        partition_spec: PyIcebergPartitionSpec | None = None,
    ) -> None:
        """Write ``data`` to an Iceberg table, creating it if missing.

        ``data`` may be a Polars DataFrame, a PyArrow Table / RecordBatchReader,
        or a SQL string (which is executed and its result streamed straight
        to the table).

        ``mode`` selects the write semantics:

        * ``"append"`` (default) — add rows to the table.
        * ``"overwrite"`` — replace the entire table contents. Streaming sources
          (SQL strings, ``RecordBatchReader``, large DataFrames above the
          streaming threshold) reject this mode with :class:`WriteError`,
          since whole-table overwrite requires a single materialised payload.

        ``schema`` and ``partition_spec`` are honoured **only on table create**.
        If the table already exists they are ignored — schema evolution is out
        of scope. Pass them through directly from ``pyiceberg.schema.Schema``
        and ``pyiceberg.partitioning.PartitionSpec``.
        """
        self._writer.write(
            data,
            database,
            table,
            mode=mode,
            schema=schema,
            partition_spec=partition_spec,
        )

    def pyiceberg_catalog(self) -> PyIcebergCatalog:
        """Return the underlying PyIceberg :class:`Catalog`.

        Escape hatch for advanced operations (manifest tables, snapshot
        management, schema introspection) that don't map onto
        ``sql`` / ``read_file`` / ``to_lake``. Most users should not need this.

        Caveats:

        * The handle is cached by the catalog backend; awebo reuses it
          internally, so mutations through this handle bypass awebo's
          streaming + write-error wrapping.
        * Holding a reference past :meth:`close` is undefined.
        """
        return self._catalog.pyiceberg(self._credentials)

    def close(self) -> None:
        self._engine.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # --------------------------------------------------------------- helpers
    @staticmethod
    def _build_auth(config: LakehouseConfig) -> AuthProvider:
        if config.aws is not None:
            return AwsAuthProvider(config.aws)
        if config.s3 is not None:
            return S3AuthProvider(config.s3)
        raise ConfigError("no auth provider could be derived from config")

    @staticmethod
    def _build_catalog(config: LakehouseConfig) -> CatalogBackend:
        if config.catalog.type == "glue":
            return GlueCatalogBackend(config.catalog)
        if config.catalog.type == "rest":
            if config.rest is None:
                raise ConfigError("rest catalog requires AWEBO_REST_URI / AWEBO_REST_WAREHOUSE")
            return RestCatalogBackend(config.catalog, config.rest)
        raise ConfigError(f"unsupported catalog type: {config.catalog.type!r}")
