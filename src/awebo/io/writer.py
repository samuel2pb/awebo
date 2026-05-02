"""Iceberg writer with auto streaming.

``Lakehouse.to_lake(data, database, table)`` accepts:

* a Polars DataFrame
* a PyArrow Table
* a PyArrow RecordBatchReader
* a SQL string (executed against DuckDB; result streamed straight to Iceberg)

If the source materialised size exceeds ``streaming_threshold_mb``, the
writer switches to a streaming path: pulls Arrow record batches and feeds
them to PyIceberg in bounded chunks. Each chunk turns into a single
:meth:`pyiceberg.table.Table.append` call so memory stays under the chunk
ceiling regardless of source size. SQL writes are *always* streamed.

Write modes
-----------

``mode="append"`` (default) is the streaming-friendly path.

``mode="overwrite"`` replaces table contents. When the target table is
partitioned (identity transforms), only the partitions present in the
incoming data are replaced (*dynamic partition overwrite*). Unpartitioned
tables get a full replacement. In-memory sources (``pa.Table``,
``pl.DataFrame``) use PyIceberg's ``dynamic_partition_overwrite``
directly. SQL strings use a transactional path: a cheap
``SELECT DISTINCT`` pre-query extracts partition values, then ``delete``
+ chunked ``append`` inside a single PyIceberg ``Transaction`` — DuckDB
handles disk spill so the full dataset is never materialised.
``RecordBatchReader`` (one-shot) falls back to full replacement.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from awebo.exceptions import WriteError
from awebo.streaming.batches import (
    estimate_size_mb,
    iter_batches_from_arrow,
    iter_batches_from_polars,
)

if TYPE_CHECKING:
    import polars as pl
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.partitioning import PartitionSpec as PyIcebergPartitionSpec
    from pyiceberg.schema import Schema as PyIcebergSchema
    from pyiceberg.table import Table as PyIcebergTable

    from awebo.auth.base import Credentials
    from awebo.catalog.base import CatalogBackend
    from awebo.query.engine import QueryEngine

type WriteSource = "pl.DataFrame | pa.Table | pa.RecordBatchReader | str"
type WriteMode = Literal["append", "overwrite"]

DEFAULT_APPEND_CHUNK_ROWS = 200_000

_VALID_MODES: frozenset[str] = frozenset({"append", "overwrite"})

log = logging.getLogger(__name__)


class IcebergWriter:
    def __init__(
        self,
        engine: QueryEngine,
        catalog: CatalogBackend,
        credentials: Credentials,
        streaming_threshold_mb: int,
        append_chunk_rows: int = DEFAULT_APPEND_CHUNK_ROWS,
    ) -> None:
        self._engine = engine
        self._catalog = catalog
        self._credentials = credentials
        self._threshold_mb = streaming_threshold_mb
        self._append_chunk_rows = append_chunk_rows

    def write(
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
        if mode not in _VALID_MODES:
            raise WriteError(f"invalid write mode {mode!r}; expected 'append' or 'overwrite'")

        identifier = f"{database}.{table}"
        self._catalog.ensure_namespace(self._credentials, database)
        py_catalog = self._catalog.pyiceberg(self._credentials)

        if mode == "overwrite":
            self._overwrite(
                data,
                py_catalog,
                identifier,
                schema=schema,
                partition_spec=partition_spec,
                schema_evolution=schema_evolution,
            )
            return

        reader, arrow_schema = self._coerce_to_reader(data)

        try:
            tbl = self._load_or_create(
                py_catalog,
                identifier,
                arrow_schema,
                schema=schema,
                partition_spec=partition_spec,
                schema_evolution=schema_evolution,
            )
            self._stream_append(tbl, reader, arrow_schema)
        except Exception as exc:
            raise WriteError(f"iceberg append to {identifier!r} failed: {exc}") from exc

    # --------------------------------------------------------------- overwrite

    def _overwrite(
        self,
        data: WriteSource,
        py_catalog: object,
        identifier: str,
        *,
        schema: PyIcebergSchema | None,
        partition_spec: PyIcebergPartitionSpec | None,
        schema_evolution: bool,
    ) -> None:
        needs_streaming = self._needs_streaming(data)

        if needs_streaming:
            self._streaming_overwrite(
                data,
                py_catalog,
                identifier,
                schema=schema,
                partition_spec=partition_spec,
                schema_evolution=schema_evolution,
            )
        else:
            arrow_table = self._materialise(data)
            try:
                tbl = self._load_or_create(
                    py_catalog,
                    identifier,
                    arrow_table.schema,
                    schema=schema,
                    partition_spec=partition_spec,
                    schema_evolution=schema_evolution,
                )
                if tbl.spec().fields:
                    tbl.dynamic_partition_overwrite(arrow_table)
                else:
                    tbl.overwrite(arrow_table)
            except WriteError:
                raise
            except Exception as exc:
                raise WriteError(f"iceberg overwrite of {identifier!r} failed: {exc}") from exc

    def _streaming_overwrite(
        self,
        data: WriteSource,
        py_catalog: object,
        identifier: str,
        *,
        schema: PyIcebergSchema | None,
        partition_spec: PyIcebergPartitionSpec | None,
        schema_evolution: bool,
    ) -> None:
        """Overwrite via Transaction: delete + chunked append.

        For SQL on partitioned tables, a cheap ``SELECT DISTINCT`` pre-query
        extracts partition values so only touched partitions are deleted.
        RecordBatchReaders and SQL on unpartitioned tables fall back to
        ``AlwaysTrue()`` (full replacement). DuckDB handles disk spill for
        SQL sources; the full dataset is never materialised in memory.
        """
        try:
            probe_reader, arrow_schema = self._coerce_to_reader(data)
            tbl = self._load_or_create(
                py_catalog,
                identifier,
                arrow_schema,
                schema=schema,
                partition_spec=partition_spec,
                schema_evolution=schema_evolution,
            )
            del probe_reader

            delete_filter = self._build_streaming_delete_filter(data, tbl)

            reader, _ = self._coerce_to_reader(data)
            txn = tbl.transaction()
            txn.delete(delete_filter=delete_filter)
            self._txn_stream_append(txn, reader, arrow_schema)
            txn.commit_transaction()

        except WriteError:
            raise
        except Exception as exc:
            raise WriteError(
                f"iceberg streaming overwrite of {identifier!r} failed: {exc}"
            ) from exc

    def _build_streaming_delete_filter(
        self,
        data: WriteSource,
        tbl: PyIcebergTable,
    ) -> BooleanExpression:
        """Build delete filter for streaming overwrite.

        SQL + partitioned (identity) → scoped filter from ``SELECT DISTINCT``.
        Everything else → ``AlwaysTrue()`` (full replacement).
        """
        from pyiceberg.expressions import AlwaysTrue

        if not isinstance(data, str) or not tbl.spec().fields:
            return AlwaysTrue()

        identity_cols = self._identity_partition_columns(tbl)
        if not identity_cols:
            return AlwaysTrue()

        return self._partition_filter_from_sql(data, identity_cols)

    def _identity_partition_columns(self, tbl: PyIcebergTable) -> list[str]:
        """Return source column names for identity-transform partition fields."""
        from pyiceberg.transforms import IdentityTransform

        cols: list[str] = []
        for field in tbl.spec().fields:
            if isinstance(field.transform, IdentityTransform):
                source = tbl.schema().find_field(field.source_id)
                cols.append(source.name)
        return cols

    def _partition_filter_from_sql(
        self,
        sql: str,
        partition_cols: list[str],
    ) -> BooleanExpression:
        """Run ``SELECT DISTINCT partition_cols FROM (sql)`` and build an expression."""
        from pyiceberg.expressions import AlwaysTrue, And, EqualTo, In, Or

        col_list = ", ".join(f'"{c}"' for c in partition_cols)
        distinct_sql = f"SELECT DISTINCT {col_list} FROM ({sql})"
        relation = self._engine.execute(distinct_sql)
        distinct_tbl = relation.arrow().read_all()

        if distinct_tbl.num_rows == 0:
            return AlwaysTrue()

        if len(partition_cols) == 1:
            col = partition_cols[0]
            values = tuple(distinct_tbl.column(col).to_pylist())
            if len(values) == 1:
                return EqualTo(term=col, literal=values[0])  # type: ignore[arg-type,call-arg]
            return In(term=col, literals=values)  # type: ignore[arg-type,call-arg]

        row_filters: list[BooleanExpression] = []
        for row in distinct_tbl.to_pylist():
            col_eqs: list[BooleanExpression] = [
                EqualTo(term=col, literal=row[col])  # type: ignore[arg-type,call-arg]
                for col in partition_cols
            ]
            combined: BooleanExpression = col_eqs[0]
            for eq in col_eqs[1:]:
                combined = And(left=combined, right=eq)
            row_filters.append(combined)

        result: BooleanExpression = row_filters[0]
        for rf in row_filters[1:]:
            result = Or(left=result, right=rf)
        return result

    def _txn_stream_append(
        self,
        txn: object,
        reader: pa.RecordBatchReader,
        arrow_schema: pa.Schema,
    ) -> None:
        """Stream batches into a transaction. Memory stays bounded."""
        buffer: list[pa.RecordBatch] = []
        rows = 0
        for batch in reader:
            buffer.append(batch)
            rows += batch.num_rows
            if rows >= self._append_chunk_rows:
                txn.append(pa.Table.from_batches(buffer, schema=arrow_schema))  # type: ignore[attr-defined]
                buffer = []
                rows = 0
        if buffer:
            txn.append(pa.Table.from_batches(buffer, schema=arrow_schema))  # type: ignore[attr-defined]

    # ------------------------------------------------------------ load / create

    def _load_or_create(
        self,
        py_catalog: object,
        identifier: str,
        arrow_schema: pa.Schema,
        *,
        schema: PyIcebergSchema | None,
        partition_spec: PyIcebergPartitionSpec | None,
        schema_evolution: bool = True,
    ) -> PyIcebergTable:
        load_table = py_catalog.load_table  # type: ignore[attr-defined]
        create_table = py_catalog.create_table  # type: ignore[attr-defined]
        try:
            tbl: PyIcebergTable = load_table(identifier)
            if schema_evolution:
                self._evolve_schema(tbl, arrow_schema)
            if partition_spec is not None:
                self._evolve_partition_spec(tbl, partition_spec)
            return tbl
        except Exception:
            create_kwargs: dict[str, object] = {"identifier": identifier}
            create_kwargs["schema"] = schema if schema is not None else arrow_schema
            if partition_spec is not None:
                create_kwargs["partition_spec"] = partition_spec
            created: PyIcebergTable = create_table(**create_kwargs)
            return created

    def _evolve_schema(self, tbl: PyIcebergTable, arrow_schema: pa.Schema) -> None:
        """Merge incoming Arrow schema into the table (adds columns, widens types)."""
        try:
            with tbl.update_schema(allow_incompatible_changes=False) as update:
                update.union_by_name(arrow_schema)
        except Exception as exc:
            raise WriteError(f"schema evolution failed: {exc}") from exc

    def _evolve_partition_spec(
        self,
        tbl: PyIcebergTable,
        desired_spec: PyIcebergPartitionSpec,
    ) -> None:
        """Evolve partition spec if it differs from the current one."""
        current_spec = tbl.spec()
        current_fields = {f.name for f in current_spec.fields}
        desired_fields = {f.name for f in desired_spec.fields}

        if current_fields == desired_fields:
            return

        try:
            with tbl.update_spec() as update:
                for field in current_spec.fields:
                    if field.name not in desired_fields:
                        update.remove_field(field.name)
                for field in desired_spec.fields:
                    if field.name not in current_fields:
                        source = tbl.schema().find_field(field.source_id)
                        update.add_field(source.name, field.transform, field.name)
        except Exception as exc:
            raise WriteError(f"partition evolution failed: {exc}") from exc

    # ----------------------------------------------------------- helpers

    def _needs_streaming(self, data: WriteSource) -> bool:
        """Return True when the source must use the streaming overwrite path.

        Only SQL strings and RecordBatchReaders qualify — pa.Table and
        pl.DataFrame are already in memory and can safely use
        ``dynamic_partition_overwrite`` regardless of size.
        """
        if isinstance(data, str):
            return True
        return isinstance(data, pa.RecordBatchReader)

    def _materialise(self, data: WriteSource) -> pa.Table:
        """Reduce an in-memory source to :class:`pa.Table`."""
        import polars as pl

        if isinstance(data, pa.Table):
            return data
        if isinstance(data, pl.DataFrame):
            return data.to_arrow()
        raise WriteError(f"unexpected source for materialise: {type(data).__name__}")

    def _stream_append(
        self,
        tbl: PyIcebergTable,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        buffer: list[pa.RecordBatch] = []
        rows = 0
        for batch in reader:
            buffer.append(batch)
            rows += batch.num_rows
            if rows >= self._append_chunk_rows:
                tbl.append(pa.Table.from_batches(buffer, schema=schema))
                buffer = []
                rows = 0
        if buffer:
            tbl.append(pa.Table.from_batches(buffer, schema=schema))

    def _coerce_to_reader(self, data: WriteSource) -> tuple[pa.RecordBatchReader, pa.Schema]:
        import polars as pl

        if isinstance(data, str):
            relation = self._engine.execute(data)
            reader = relation.to_arrow_reader(batch_size=100_000)
            return reader, reader.schema

        if isinstance(data, pa.RecordBatchReader):
            return data, data.schema

        if isinstance(data, pa.Table):
            schema = data.schema
            if estimate_size_mb(data) > self._threshold_mb:
                streamed = pa.RecordBatchReader.from_batches(schema, iter_batches_from_arrow(data))
                return streamed, schema
            return data.to_reader(), schema

        if isinstance(data, pl.DataFrame):
            arrow_tbl = data.to_arrow()
            schema = arrow_tbl.schema
            if estimate_size_mb(arrow_tbl) > self._threshold_mb:
                streamed = pa.RecordBatchReader.from_batches(schema, iter_batches_from_polars(data))
                return streamed, schema
            return arrow_tbl.to_reader(), schema

        raise WriteError(f"unsupported write source type: {type(data).__name__}")
