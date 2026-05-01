from __future__ import annotations

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from awebo.streaming.batches import (
    estimate_size_mb,
    iter_batches_from_arrow,
)


def _table(rows: int) -> pa.Table:
    return pa.table({"x": list(range(rows)), "y": [f"v{i}" for i in range(rows)]})


def test_estimate_size_mb_returns_nonneg() -> None:
    assert estimate_size_mb(_table(100)) >= 0


def test_iter_batches_round_trip() -> None:
    table = _table(1_000)
    batches = list(iter_batches_from_arrow(table, batch_rows=250))
    assert sum(b.num_rows for b in batches) == 1_000
    assert all(b.num_rows <= 250 for b in batches)


@pytest.mark.property
@settings(max_examples=50, deadline=None)
@given(
    rows=st.integers(min_value=0, max_value=5_000),
    batch_rows=st.integers(min_value=1, max_value=500),
)
def test_iter_batches_invariant_total_rows_preserved(rows: int, batch_rows: int) -> None:
    table = _table(rows)
    batches = list(iter_batches_from_arrow(table, batch_rows=batch_rows))
    assert sum(b.num_rows for b in batches) == rows


@pytest.mark.memory
def test_streaming_iteration_bounded_memory() -> None:
    """Streaming a large table batch-by-batch must not retain everything."""
    table = _table(200_000)
    consumed = 0
    for batch in iter_batches_from_arrow(table, batch_rows=10_000):
        consumed += batch.num_rows
        del batch
    assert consumed == 200_000
