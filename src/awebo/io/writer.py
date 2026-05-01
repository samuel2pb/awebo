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
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from awebo.exceptions import WriteError
from awebo.streaming.batches import (
    estimate_size_mb,
    iter_batches_from_arrow,
    iter_batches_from_polars,
)

if TYPE_CHECKING:
    import polars as pl
    from pyiceberg.table import Table as PyIcebergTable

    from awebo.auth.base import Credentials
    from awebo.catalog.base import CatalogBackend
    from awebo.query.engine import QueryEngine

type WriteSource = "pl.DataFrame | pa.Table | pa.RecordBatchReader | str"

# Cap on rows accumulated before flushing one append() call. Tuned so a
# typical 50-byte row stays under ~10MB per flush.
DEFAULT_APPEND_CHUNK_ROWS = 200_000


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

    def write(self, data: WriteSource, database: str, table: str) -> None:
        identifier = f"{database}.{table}"
        self._catalog.ensure_namespace(self._credentials, database)
        py_catalog = self._catalog.pyiceberg(self._credentials)

        reader, schema = self._coerce_to_reader(data)

        try:
            try:
                tbl = py_catalog.load_table(identifier)
            except Exception:
                tbl = py_catalog.create_table(identifier, schema=schema)

            self._stream_append(tbl, reader, schema)
        except Exception as exc:
            raise WriteError(f"iceberg append to {identifier!r} failed: {exc}") from exc

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
