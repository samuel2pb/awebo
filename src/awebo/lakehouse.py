"""User-facing Lakehouse facade.

This module is intentionally thin. All real work lives in the feature
packages (``catalog``, ``auth``, ``query``, ``io``, ``streaming``); the
:class:`Lakehouse` is just the wiring that loads config once, resolves
credentials once, attaches the catalog to DuckDB once, and exposes the
ergonomic verbs ``sql``, ``read_file``, ``to_lake``.
"""

from __future__ import annotations

import logging
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
    from awebo.lineage.emitter import LineageEmitter

log = logging.getLogger(__name__)


class LakehouseBuilder:
    """Explicit composition of Lakehouse components.

    Use when you need fine-grained control over which auth provider and
    catalog backend are wired. For the common case, ``Lakehouse(config)``
    still works as a shortcut.
    """

    def __init__(self, config: LakehouseConfig | None = None) -> None:
        self._config = config or load_config()
        self._auth: AuthProvider | None = None
        self._catalog: CatalogBackend | None = None
        self._emitter: LineageEmitter | None = None

    def with_auth(self, auth: AuthProvider) -> Self:
        self._auth = auth
        return self

    def with_catalog(self, catalog: CatalogBackend) -> Self:
        self._catalog = catalog
        return self

    def with_emitter(self, emitter: LineageEmitter) -> Self:
        self._emitter = emitter
        return self

    def build(self) -> Lakehouse:
        if self._auth is None:
            raise ValueError("auth provider required — call .with_auth() before .build()")
        if self._catalog is None:
            raise ValueError("catalog backend required — call .with_catalog() before .build()")
        return Lakehouse(
            self._config,
            catalog=self._catalog,
            auth=self._auth,
            emitter=self._emitter,
        )


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
        emitter: LineageEmitter | None = None,
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

        if emitter is None:
            from awebo.lineage.factory import create_emitter

            self._emitter: LineageEmitter = create_emitter(self._config.lineage)
        else:
            self._emitter = emitter
        self._reader = FileReader(self._engine)
        self._writer = IcebergWriter(
            engine=self._engine,
            catalog=self._catalog,
            credentials=self._credentials,
            streaming_threshold_mb=self._config.streaming_threshold_mb,
            emitter=self._emitter,
            lineage_namespace=self._config.lineage.namespace,
        )

        if self._config.trace_memory:
            from awebo._observability import start_memory_tracing

            start_memory_tracing()
        log.info(
            "lakehouse ready: catalog=%s name=%s threshold=%dMB",
            self._config.catalog.type,
            self._config.catalog.name,
            self._config.streaming_threshold_mb,
        )

    # ---------------------------------------------------------------- public
    def sql(self, query: str) -> QueryResult:
        """Execute a SQL query against DuckDB and return a lazy result.

        Catalog-qualified queries (``select ... from <name>.<ns>.<tbl>``) work
        on Glue. They do **not** work on REST today: the DuckDB iceberg
        extension (1.5.x) does not yet implement REST ATTACH, so awebo skips
        ATTACH for REST and you must read REST-backed tables via
        ``iceberg_scan('s3://...')`` or :meth:`pyiceberg_catalog`.
        """
        from awebo._observability import otel_span

        with otel_span("awebo.sql", query=query[:80]):
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
        schema_evolution: bool = True,
    ) -> None:
        """Write ``data`` to an Iceberg table, creating it if missing.

        ``data`` may be a Polars DataFrame, a PyArrow Table / RecordBatchReader,
        or a SQL string (which is executed and its result streamed straight
        to the table).

        ``mode`` selects the write semantics:

        * ``"append"`` (default) — add rows to the table.
        * ``"overwrite"`` — replace table contents. When the target table is
          partitioned with identity transforms, only the partitions present
          in the incoming data are replaced (*dynamic partition overwrite*).
          Unpartitioned tables get a full replacement. In-memory sources use
          ``dynamic_partition_overwrite`` directly. SQL strings use a
          partition-scoped transactional path (``SELECT DISTINCT`` pre-query
          + ``delete`` + chunked ``append``). ``RecordBatchReader`` (one-shot)
          falls back to full replacement.

        ``schema`` and ``partition_spec`` are honoured on table create.
        When the table already exists:

        * ``schema_evolution`` (default ``True``) merges the incoming schema
          into the table via ``union_by_name`` (adds new columns, widens
          compatible types). Set to ``False`` to reject schema mismatches.
        * ``partition_spec`` triggers partition evolution — fields are added
          or removed to match the desired spec.
        """
        from awebo._observability import otel_span

        with otel_span("awebo.write", table=f"{database}.{table}", mode=mode):
            self._writer.write(
                data,
                database,
                table,
                mode=mode,
                schema=schema,
                partition_spec=partition_spec,
                schema_evolution=schema_evolution,
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

    def health_check(self) -> dict[str, str]:
        """Quick connectivity diagnostic for DuckDB and the catalog."""
        status: dict[str, str] = {}
        try:
            self._engine.execute("SELECT 1")
            status["duckdb"] = "ok"
        except Exception as exc:
            status["duckdb"] = f"error: {exc}"
        try:
            self._catalog.pyiceberg(self._credentials).list_namespaces()
            status["catalog"] = "ok"
        except Exception as exc:
            status["catalog"] = f"error: {exc}"
        return status

    def close(self) -> None:
        log.debug("closing lakehouse")
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
            assert config.rest is not None  # guaranteed by load_config()
            return RestCatalogBackend(config.catalog, config.rest)
        raise ConfigError(f"unsupported catalog type: {config.catalog.type!r}")
