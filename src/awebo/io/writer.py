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

``mode="overwrite"`` replaces the entire table contents in a single
:meth:`pyiceberg.table.Table.overwrite` call. Because PyIceberg's
``overwrite`` takes a fully-materialised :class:`pyarrow.Table`, this writer
rejects the streaming sources (``RecordBatchReader``, SQL strings, and
materialised payloads above ``streaming_threshold_mb``) when combined with
``mode="overwrite"``. Callers who need to overwrite a large dataset should
materialise it themselves and call ``to_lake`` with a regular Arrow Table.
"""

from __future__ import annotations

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
    from pyiceberg.partitioning import PartitionSpec as PyIcebergPartitionSpec
    from pyiceberg.schema import Schema as PyIcebergSchema
    from pyiceberg.table import Table as PyIcebergTable

    from awebo.auth.base import Credentials
    from awebo.catalog.base import CatalogBackend
    from awebo.query.engine import QueryEngine

type WriteSource = "pl.DataFrame | pa.Table | pa.RecordBatchReader | str"
type WriteMode = Literal["append", "overwrite"]

# Cap on rows accumulated before flushing one append() call. Tuned so a
# typical 50-byte row stays under ~10MB per flush.
DEFAULT_APPEND_CHUNK_ROWS = 200_000

_VALID_MODES: frozenset[str] = frozenset({"append", "overwrite"})


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
    ) -> None:
        if mode not in _VALID_MODES:
            raise WriteError(f"invalid write mode {mode!r}; expected 'append' or 'overwrite'")

        identifier = f"{database}.{table}"
        self._catalog.ensure_namespace(self._credentials, database)
        py_catalog = self._catalog.pyiceberg(self._credentials)

        if mode == "overwrite":
            arrow_table = self._materialise_for_overwrite(data)
            try:
                tbl = self._load_or_create(
                    py_catalog,
                    identifier,
                    arrow_table.schema,
                    schema=schema,
                    partition_spec=partition_spec,
                )
                tbl.overwrite(arrow_table)
            except WriteError:
                raise
            except Exception as exc:
                raise WriteError(f"iceberg overwrite of {identifier!r} failed: {exc}") from exc
            return

        reader, arrow_schema = self._coerce_to_reader(data)

        try:
            tbl = self._load_or_create(
                py_catalog,
                identifier,
                arrow_schema,
                schema=schema,
                partition_spec=partition_spec,
            )
            self._stream_append(tbl, reader, arrow_schema)
        except Exception as exc:
            raise WriteError(f"iceberg append to {identifier!r} failed: {exc}") from exc

    def _load_or_create(
        self,
        py_catalog: object,
        identifier: str,
        arrow_schema: pa.Schema,
        *,
        schema: PyIcebergSchema | None,
        partition_spec: PyIcebergPartitionSpec | None,
    ) -> PyIcebergTable:
        # py_catalog is a pyiceberg Catalog; typed as object in this signature
        # only because importing the protocol eagerly would defeat the
        # TYPE_CHECKING-guarded imports above.
        load_table = py_catalog.load_table  # type: ignore[attr-defined]
        create_table = py_catalog.create_table  # type: ignore[attr-defined]
        try:
            tbl: PyIcebergTable = load_table(identifier)
            return tbl
        except Exception:
            create_kwargs: dict[str, object] = {"identifier": identifier}
            create_kwargs["schema"] = schema if schema is not None else arrow_schema
            if partition_spec is not None:
                create_kwargs["partition_spec"] = partition_spec
            created: PyIcebergTable = create_table(**create_kwargs)
            return created

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

    def _materialise_for_overwrite(self, data: WriteSource) -> pa.Table:
        """Reduce ``data`` to a single :class:`pa.Table` for overwrite.

        Streaming sources are rejected: pyiceberg's ``Table.overwrite`` takes
        a materialised Arrow table, and silently buffering an unbounded
        :class:`RecordBatchReader` would defeat the streaming-threshold
        contract. Callers wanting overwrite at scale must materialise.
        """
        import polars as pl

        if isinstance(data, str):
            raise WriteError("overwrite mode does not support SQL-string sources (always streamed)")
        if isinstance(data, pa.RecordBatchReader):
            raise WriteError(
                "overwrite mode does not support RecordBatchReader sources; "
                "materialise to a pa.Table first"
            )
        if isinstance(data, pa.Table):
            if estimate_size_mb(data) > self._threshold_mb:
                raise WriteError(
                    "overwrite mode rejects payloads above streaming threshold "
                    f"({self._threshold_mb} MB); materialise smaller batches or use append"
                )
            return data
        if isinstance(data, pl.DataFrame):
            arrow_tbl = data.to_arrow()
            if estimate_size_mb(arrow_tbl) > self._threshold_mb:
                raise WriteError(
                    "overwrite mode rejects payloads above streaming threshold "
                    f"({self._threshold_mb} MB); materialise smaller batches or use append"
                )
            return arrow_tbl
        raise WriteError(f"unsupported write source type: {type(data).__name__}")

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
