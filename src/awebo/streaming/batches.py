"""Arrow record-batch helpers used by the streaming write path.

The writer decides whether to stream by comparing
:func:`estimate_size_mb` against the configured threshold. If streaming is
needed, the writer pulls batches via :func:`iter_batches_from_arrow` /
:func:`iter_batches_from_polars` so memory stays bounded regardless of input
size.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    import polars as pl
    import pyarrow as pa

DEFAULT_BATCH_ROWS = 100_000


def estimate_size_mb(table: pa.Table) -> float:
    """Cheap upper bound on table size — uses Arrow's ``nbytes`` attribute."""
    return float(table.nbytes) / (1024 * 1024)


def iter_batches_from_arrow(
    table: pa.Table, batch_rows: int = DEFAULT_BATCH_ROWS
) -> Iterator[pa.RecordBatch]:
    yield from table.to_batches(max_chunksize=batch_rows)


def iter_batches_from_polars(
    df: pl.DataFrame, batch_rows: int = DEFAULT_BATCH_ROWS
) -> Iterator[pa.RecordBatch]:
    n = df.height
    for start in range(0, n, batch_rows):
        chunk = df.slice(start, batch_rows)
        yield from chunk.to_arrow().to_batches()
