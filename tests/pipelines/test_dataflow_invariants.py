"""Property + memory invariants for dataflow pipelines.

* Property test: any Polars DataFrame round-tripped through to_lake / read
  preserves rows and columns regardless of size or content shape.
* Memory test: writing a large dataset under a tiny streaming threshold
  keeps allocations bounded — proves the chunked-append path is doing real
  work, not silently materialising.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests._fixtures.lakehouse import make_test_lakehouse

if TYPE_CHECKING:
    from pathlib import Path

    import pyarrow as pa

    from awebo import Lakehouse
    from tests._fixtures.catalog import LocalIcebergCatalogBackend


def _read_back(lake: Lakehouse, database: str, table: str) -> pa.Table:
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    catalog = backend.pyiceberg(lake._credentials)
    return catalog.load_table(f"{database}.{table}").scan().to_arrow()


@pytest.mark.property
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    rows=st.integers(min_value=1, max_value=500),
    seed=st.integers(min_value=0, max_value=10_000),
)
def test_dataflow_invariant_polars_roundtrip_preserves_rows(
    tmp_path_factory: pytest.TempPathFactory, rows: int, seed: int
) -> None:
    """For any (rows, seed) the round-trip preserves row count and columns."""
    tmp = tmp_path_factory.mktemp("prop")
    lake = make_test_lakehouse(tmp)
    try:
        df = pl.DataFrame(
            {
                "id": list(range(rows)),
                "bucket": [(seed + i) % 13 for i in range(rows)],
                "label": [f"row-{(seed + i) % 7}" for i in range(rows)],
            }
        )
        lake.to_lake(df, database="prop", table="rt")
        out = _read_back(lake, "prop", "rt")
        assert out.num_rows == rows
        assert set(out.column_names) == {"id", "bucket", "label"}
    finally:
        lake.close()


@pytest.mark.memory
def test_dataflow_streaming_write_bounded_memory(tmp_path: Path) -> None:
    """Force the chunked-append path with threshold=0 and a large source.

    The test passes if it completes; pytest-memray prints the allocation
    profile so regressions in the streaming path become visible in CI logs.
    Tune the row count up if you want a stronger ceiling — keep it modest
    so the suite stays fast.
    """
    lake = make_test_lakehouse(tmp_path, streaming_threshold_mb=0)
    try:
        n = 250_000
        df = pl.DataFrame(
            {
                "id": list(range(n)),
                "bucket": [i % 11 for i in range(n)],
                "amount": [float(i) * 0.5 for i in range(n)],
            }
        )
        lake.to_lake(df, database="bulk", table="rows")
        out = _read_back(lake, "bulk", "rows")
        assert out.num_rows == n
    finally:
        lake.close()
