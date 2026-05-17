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
partitioned by identity-only transforms, only the partitions present in
the incoming data are replaced (*dynamic partition overwrite*).
Unpartitioned tables get a full replacement. Mixed or non-identity
partition specs (e.g. ``year(ts)``) — which PyIceberg's
``dynamic_partition_overwrite`` rejects — fall back to
``overwrite(df, overwrite_filter=expr)``; the filter is derived from
identity columns present in the data so transform-only partitions are
not deleted wholesale. SQL strings use a transactional path: a cheap
``SELECT DISTINCT`` pre-query extracts partition values, then ``delete``
+ chunked ``append`` inside a single PyIceberg ``Transaction`` — DuckDB
handles disk spill so the full dataset is never materialised.
``RecordBatchReader`` (one-shot) falls back to full replacement.
"""

from __future__ import annotations

import logging
import time
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
    from awebo.lineage.emitter import LineageEmitter
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
        emitter: LineageEmitter | None = None,
        lineage_namespace: str = "awebo",
    ) -> None:
        self._engine = engine
        self._catalog = catalog
        self._credentials = credentials
        self._threshold_mb = streaming_threshold_mb
        self._append_chunk_rows = append_chunk_rows
        self._lineage_namespace = lineage_namespace
        if emitter is None:
            from awebo.lineage.emitter import NoopEmitter

            self._emitter: LineageEmitter = NoopEmitter()
        else:
            self._emitter = emitter

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
        run_id = self._new_run_id()
        self._emit_start(identifier, run_id)
        _t0 = time.perf_counter()

        try:
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
                self._log_write_done(mode, identifier, _t0)
                self._emit_complete(identifier, run_id)
                return

            reader, arrow_schema = self._coerce_to_reader(data)
            tbl = self._load_or_create(
                py_catalog,
                identifier,
                arrow_schema,
                schema=schema,
                partition_spec=partition_spec,
                schema_evolution=schema_evolution,
            )
            self._stream_append(tbl, reader, arrow_schema)
            self._log_write_done(mode, identifier, _t0)
            self._emit_complete(identifier, run_id)
        except WriteError:
            self._emit_fail(identifier, run_id)
            raise
        except Exception as exc:
            self._emit_fail(identifier, run_id)
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
        log.debug(
            "overwrite %s: streaming=%s source=%s",
            identifier,
            needs_streaming,
            type(data).__name__,
        )

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
            if arrow_table.num_rows == 0:
                log.warning("empty arrow table — skipping overwrite of %s", identifier)
                return
            try:
                tbl = self._load_or_create(
                    py_catalog,
                    identifier,
                    arrow_table.schema,
                    schema=schema,
                    partition_spec=partition_spec,
                    schema_evolution=schema_evolution,
                )
                spec_fields = tbl.spec().fields
                if not spec_fields:
                    tbl.overwrite(arrow_table)
                elif self._all_identity_partitions(tbl):
                    tbl.dynamic_partition_overwrite(arrow_table)
                else:
                    expr = self._partition_filter_from_arrow(arrow_table, tbl)
                    tbl.overwrite(arrow_table, overwrite_filter=expr)
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
            rows_written = self._txn_stream_append(txn, reader, arrow_schema)
            if rows_written == 0:
                log.warning("empty source — skipping streaming overwrite of %s", identifier)
                return
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

    def _all_identity_partitions(self, tbl: PyIcebergTable) -> bool:
        """True iff the partition spec is non-empty and entirely identity."""
        from pyiceberg.transforms import IdentityTransform

        fields = tbl.spec().fields
        return bool(fields) and all(isinstance(f.transform, IdentityTransform) for f in fields)

    def _partition_filter_from_arrow(
        self,
        arrow_table: pa.Table,
        tbl: PyIcebergTable,
    ) -> BooleanExpression:
        """Build an overwrite filter from identity-partition values in arrow.

        When the partition spec mixes identity and transform fields, pyiceberg
        cannot dynamic-overwrite. We narrow the delete to the identity slice
        present in the incoming data; transform-only fields stay implicit
        (their values follow from the data being written).
        """
        from pyiceberg.expressions import AlwaysTrue

        identity_cols = self._identity_partition_columns(tbl)
        if not identity_cols:
            return AlwaysTrue()

        present = [c for c in identity_cols if c in arrow_table.schema.names]
        if not present:
            return AlwaysTrue()

        distinct_tbl = arrow_table.select(present).group_by(present).aggregate([])
        return self._expression_from_distinct(distinct_tbl, present)

    def _expression_from_distinct(
        self,
        distinct_tbl: pa.Table,
        partition_cols: list[str],
    ) -> BooleanExpression:
        from pyiceberg.expressions import AlwaysTrue, And, EqualTo, In, Or

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
    ) -> int:
        """Stream batches into a transaction. Returns total rows written."""
        buffer: list[pa.RecordBatch] = []
        rows = 0
        total_rows = 0
        for batch in reader:
            buffer.append(batch)
            rows += batch.num_rows
            if rows >= self._append_chunk_rows:
                txn.append(pa.Table.from_batches(buffer, schema=arrow_schema))  # type: ignore[attr-defined]
                total_rows += rows
                buffer = []
                rows = 0
        if buffer:
            txn.append(pa.Table.from_batches(buffer, schema=arrow_schema))  # type: ignore[attr-defined]
            total_rows += rows
        return total_rows

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
        loaded = False
        try:
            tbl: PyIcebergTable = load_table(identifier)
            loaded = True
            log.debug("loaded existing table %s", identifier)
            if schema_evolution:
                self._evolve_schema(tbl, arrow_schema)
            if partition_spec is not None:
                self._evolve_partition_spec(tbl, partition_spec, desired_schema=schema)
            return tbl
        except Exception:
            if loaded:
                # Schema/partition evolution failed on an existing table —
                # propagate; do NOT fall through to create_table which would
                # error with TableAlreadyExists.
                raise
            log.debug("creating table %s", identifier)
            create_kwargs: dict[str, object] = {"identifier": identifier}
            create_kwargs["schema"] = schema if schema is not None else arrow_schema
            if partition_spec is not None:
                create_kwargs["partition_spec"] = partition_spec
            created: PyIcebergTable = create_table(**create_kwargs)
            if schema_evolution:
                self._evolve_schema(created, arrow_schema)
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
        desired_schema: PyIcebergSchema | None = None,
    ) -> None:
        """Evolve partition spec if it differs from the current one.

        ``desired_spec.fields[i].source_id`` is the field id in ``desired_schema``
        (the contract-derived schema), not necessarily in the live table — when a
        partition column has just been added via schema evolution, Iceberg
        auto-assigns its id and the contract's value is stale. We resolve the
        source column by NAME against the live table schema.
        """
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
                    if field.name in current_fields:
                        continue
                    source_name: str | None = None
                    if desired_schema is not None:
                        try:
                            source_name = desired_schema.find_field(field.source_id).name
                        except Exception:
                            source_name = None
                    if source_name is None:
                        # Fall back to id-based lookup against the live schema.
                        source_name = tbl.schema().find_field(field.source_id).name
                    # Confirm the source column exists in the live (post-evolve)
                    # schema by NAME; this is the one that holds the auto-
                    # assigned id we actually need.
                    tbl.schema().find_field(source_name)
                    update.add_field(source_name, field.transform, field.name)
        except Exception as exc:
            raise WriteError(f"partition evolution failed: {exc}") from exc

    @staticmethod
    def _log_write_done(mode: str, identifier: str, t0: float) -> None:
        from awebo._observability import log_peak_memory

        log_peak_memory(log)
        log.info("%s %s [%.3fs]", mode, identifier, time.perf_counter() - t0)

    # ----------------------------------------------------------- lineage

    @staticmethod
    def _new_run_id() -> str:
        from openlineage.client.uuid import generate_new_uuid

        return str(generate_new_uuid())

    def _emit_start(self, identifier: str, run_id: str) -> None:
        try:
            from awebo.lineage.events import write_start

            self._emitter.emit(write_start(self._lineage_namespace, identifier, run_id))
        except Exception:
            log.warning("lineage START emission failed for %s", identifier, exc_info=True)

    def _emit_complete(self, identifier: str, run_id: str) -> None:
        try:
            from awebo.lineage.events import write_complete

            self._emitter.emit(write_complete(self._lineage_namespace, identifier, run_id))
        except Exception:
            log.warning("lineage COMPLETE emission failed for %s", identifier, exc_info=True)

    def _emit_fail(self, identifier: str, run_id: str) -> None:
        try:
            from awebo.lineage.events import write_fail

            self._emitter.emit(write_fail(self._lineage_namespace, identifier, run_id))
        except Exception:
            log.warning("lineage FAIL emission failed for %s", identifier, exc_info=True)

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
