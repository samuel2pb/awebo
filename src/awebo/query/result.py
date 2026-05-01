"""Lazy query result object.

``Lakehouse.sql()`` returns a :class:`QueryResult` rather than executing
eagerly into a particular dataframe. The user picks the materialisation:

* ``.to_polars()`` -> :class:`polars.DataFrame`
* ``.to_arrow()``  -> :class:`pyarrow.Table`
* ``.show()``      -> prints a formatted preview
* ``.batches()``   -> streaming :class:`pyarrow.RecordBatchReader`

Calling any conversion method materialises the relation; calling ``batches``
keeps things streaming so larger-than-memory results don't blow up.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb
    import polars as pl
    import pyarrow as pa


class QueryResult:
    """Wraps a DuckDB relation with ergonomic materialisation methods."""

    def __init__(self, relation: duckdb.DuckDBPyRelation) -> None:
        self._relation = relation

    def to_polars(self) -> pl.DataFrame:
        import polars as pl

        df = pl.from_arrow(self._relation.to_arrow_table())
        if isinstance(df, pl.Series):
            return df.to_frame()
        return df

    def to_arrow(self) -> pa.Table:
        return self._relation.to_arrow_table()

    def batches(self, batch_size: int = 100_000) -> pa.RecordBatchReader:
        """Stream results as Arrow record batches without full materialisation."""
        return self._relation.to_arrow_reader(batch_size=batch_size)

    def show(self, limit: int = 20) -> None:
        self._relation.limit(limit).show()

    def __repr__(self) -> str:
        return f"<QueryResult relation={self._relation!r}>"
