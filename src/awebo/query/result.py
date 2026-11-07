"""Lazy query result object.

``Lakehouse.sql()`` returns a :class:`QueryResult` rather than executing
eagerly into a particular dataframe. The user picks the materialisation:

* ``.to_polars()`` -> :class:`polars.DataFrame` (DuckDB's native Polars export)
* ``.to_arrow()``  -> :class:`pyarrow.Table`
* ``.show()``      -> prints a formatted preview
* ``.batches()``   -> streaming :class:`pyarrow.RecordBatchReader`

DuckDB 2.0 relations are SQL text: every conversion re-runs the query.
Calling ``batches`` keeps things streaming so larger-than-memory results
don't blow up.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb
    import polars as pl
    import pyarrow as pa

DEFAULT_BATCH_SIZE = 131_072
"""DuckDB 2.0's own default Arrow batch size."""


class QueryResult:
    """Wraps a DuckDB relation with ergonomic materialisation methods."""

    def __init__(self, relation: duckdb.DuckDBPyRelation) -> None:
        self._relation = relation

    def to_polars(self) -> pl.DataFrame:
        frame: pl.DataFrame = self._relation.pl()
        return frame

    def to_arrow(self) -> pa.Table:
        table: pa.Table = self._relation.to_arrow_table()
        return table

    def batches(self, batch_size: int = DEFAULT_BATCH_SIZE) -> pa.RecordBatchReader:
        """Stream results as Arrow record batches without full materialisation."""
        reader: pa.RecordBatchReader = self._relation.to_arrow_reader(batch_size=batch_size)
        return reader

    def show(self, limit: int = 20) -> None:
        self._relation.limit(limit).show()

    def __repr__(self) -> str:
        return f"<QueryResult sql={self._relation.sql_query()!r}>"
