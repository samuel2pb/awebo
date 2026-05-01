"""Pipeline-suite fixtures.

A single ``lake`` fixture per test, wired against a temp local Iceberg
catalog. ``sample_parquet`` and ``sample_csv`` fixtures drop ready-made
input files into the same temp dir so each test can build a focused
end-to-end pipeline.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests._fixtures.lakehouse import make_test_lakehouse

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from awebo import Lakehouse


@pytest.fixture
def lake(tmp_path: Path) -> Iterator[Lakehouse]:
    instance = make_test_lakehouse(tmp_path)
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture
def streaming_lake(tmp_path: Path) -> Iterator[Lakehouse]:
    """Lakehouse with a tiny streaming threshold to force chunked writes."""
    instance = make_test_lakehouse(tmp_path, streaming_threshold_mb=0)
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    table = pa.table(
        {
            "id": list(range(1, 11)),
            "category": ["a", "b", "a", "c", "b", "a", "c", "b", "a", "c"],
            "value": [10.0, 20.5, 30.0, 12.5, 22.0, 33.0, 14.0, 25.0, 36.5, 18.0],
        }
    )
    path = tmp_path / "raw.parquet"
    pq.write_table(table, path)
    return path


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    path = tmp_path / "raw.csv"
    path.write_text(
        "id,category,value\n1,a,10.0\n2,b,20.5\n3,a,30.0\n4,c,12.5\n5,b,22.0\n",
        encoding="utf-8",
    )
    return path


@pytest.fixture
def big_polars_df() -> pl.DataFrame:
    """A DataFrame guaranteed to exceed ``streaming_threshold_mb=0``."""
    n = 50_000
    return pl.DataFrame(
        {
            "id": list(range(n)),
            "bucket": [i % 7 for i in range(n)],
            "amount": [float(i) * 1.25 for i in range(n)],
        }
    )
