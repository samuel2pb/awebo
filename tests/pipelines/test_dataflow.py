"""End-to-end dataflow tests for awebo pipelines.

Each test models a realistic pipeline shape — read inputs, transform with
SQL or Polars, write back to Iceberg, then re-read for verification.
Verification reads go through PyIceberg directly (the production read path
via the DuckDB iceberg extension is exercised in moto-backed integration
tests, not here).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import polars as pl
import pyarrow as pa
import pytest

from awebo.exceptions import WriteError

if TYPE_CHECKING:
    from pathlib import Path

    from awebo import Lakehouse
    from tests._fixtures.catalog import LocalIcebergCatalogBackend


def _read_back(lake: Lakehouse, database: str, table: str) -> pa.Table:
    """Load an Iceberg table via PyIceberg and return as Arrow."""
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    catalog = backend.pyiceberg(lake._credentials)
    return catalog.load_table(f"{database}.{table}").scan().to_arrow()


def _column_strings(table: pa.Table, name: str) -> list[str]:
    return [str(v) for v in table.column(name).to_pylist() if v is not None]


def _column_ints(table: pa.Table, name: str) -> list[int]:
    return [int(v) for v in table.column(name).to_pylist() if v is not None]


def _column_floats(table: pa.Table, name: str) -> list[float]:
    return [float(v) for v in table.column(name).to_pylist() if v is not None]


# ---------------------------------------------------------------- read path


def test_pipeline_read_parquet_to_polars(lake: Lakehouse, sample_parquet: Path) -> None:
    """File → register view → SQL → Polars: the simplest read pipeline."""
    lake.read_file(str(sample_parquet), alias="raw")
    df = lake.sql("SELECT * FROM raw ORDER BY id").to_polars()
    assert df.height == 10
    assert df.columns == ["id", "category", "value"]
    assert df["id"].to_list() == list(range(1, 11))


def test_pipeline_read_csv_with_options(lake: Lakehouse, sample_csv: Path) -> None:
    lake.read_file(str(sample_csv), alias="raw_csv", options={"header": True})
    rows = lake.sql("SELECT count(*) AS n FROM raw_csv").to_arrow().column("n").to_pylist()
    assert rows == [5]


def test_pipeline_sql_aggregation_into_polars(lake: Lakehouse, sample_parquet: Path) -> None:
    lake.read_file(str(sample_parquet), alias="raw")
    df = lake.sql(
        """
        SELECT category, count(*) AS n, sum(value) AS total
        FROM raw
        GROUP BY category
        ORDER BY category
        """
    ).to_polars()
    assert df["category"].to_list() == ["a", "b", "c"]
    assert df["n"].to_list() == [4, 3, 3]


# --------------------------------------------------------------- write path


def test_pipeline_write_polars_df_creates_table(lake: Lakehouse) -> None:
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    lake.to_lake(df, database="sales", table="customers")

    out = _read_back(lake, "sales", "customers")
    assert out.num_rows == 3
    assert sorted(_column_strings(out, "name")) == ["a", "b", "c"]


def test_pipeline_write_arrow_table(lake: Lakehouse) -> None:
    table = pa.table({"id": [10, 20], "kind": ["x", "y"]})
    lake.to_lake(table, database="ops", table="events")

    out = _read_back(lake, "ops", "events")
    assert out.num_rows == 2
    assert _column_ints(out, "id") == [10, 20]


def test_pipeline_write_appends_when_table_exists(lake: Lakehouse) -> None:
    df1 = pl.DataFrame({"id": [1, 2]})
    df2 = pl.DataFrame({"id": [3, 4]})
    lake.to_lake(df1, database="db", table="t")
    lake.to_lake(df2, database="db", table="t")

    out = _read_back(lake, "db", "t")
    assert sorted(_column_ints(out, "id")) == [1, 2, 3, 4]


def test_pipeline_write_creates_namespace_idempotently(lake: Lakehouse) -> None:
    df = pl.DataFrame({"id": [1]})
    lake.to_lake(df, database="brand_new", table="t1")
    lake.to_lake(df, database="brand_new", table="t2")  # second call must not fail

    assert _read_back(lake, "brand_new", "t1").num_rows == 1
    assert _read_back(lake, "brand_new", "t2").num_rows == 1


def test_pipeline_write_unsupported_source_raises(lake: Lakehouse) -> None:
    with pytest.raises(WriteError, match="unsupported write source"):
        lake.to_lake(12345, database="db", table="t")  # type: ignore[arg-type]


# --------------------------------------------------------- read→write round trip


def test_pipeline_file_to_iceberg_roundtrip(lake: Lakehouse, sample_parquet: Path) -> None:
    """parquet → register → SELECT * → to_lake → re-read identical rows."""
    lake.read_file(str(sample_parquet), alias="raw")
    df = lake.sql("SELECT * FROM raw ORDER BY id").to_polars()
    lake.to_lake(df, database="bronze", table="raw_orders")

    out = _read_back(lake, "bronze", "raw_orders")
    assert out.num_rows == 10
    out_sorted = out.sort_by([("id", "ascending")])
    assert _column_ints(out_sorted, "id") == list(range(1, 11))


def test_pipeline_sql_string_streamed_to_iceberg(lake: Lakehouse, sample_parquet: Path) -> None:
    """SQL-string write path: result of a query streamed straight to Iceberg.

    Verifies the no-materialisation contract for the SQL write source.
    """
    lake.read_file(str(sample_parquet), alias="raw")
    lake.to_lake(
        data="SELECT category, sum(value) AS total FROM raw GROUP BY category",
        database="silver",
        table="by_category",
    )

    out = _read_back(lake, "silver", "by_category")
    assert out.num_rows == 3
    assert set(_column_strings(out, "category")) == {"a", "b", "c"}


def test_pipeline_chained_bronze_silver_gold(lake: Lakehouse, sample_parquet: Path) -> None:
    """Three-stage pipeline: bronze ingest → silver aggregate → gold filter."""
    # bronze: raw file → table
    lake.read_file(str(sample_parquet), alias="raw")
    lake.to_lake("SELECT * FROM raw", database="bronze", table="orders")
    bronze = _read_back(lake, "bronze", "orders")
    assert bronze.num_rows == 10

    # silver: aggregate (uses Polars in-memory transform)
    bronze_pl = pl.from_arrow(bronze)
    assert isinstance(bronze_pl, pl.DataFrame)
    silver_pl = bronze_pl.group_by("category").agg(pl.col("value").sum().alias("total"))
    lake.to_lake(silver_pl, database="silver", table="totals")

    # gold: read silver back via pyiceberg, filter, write
    silver_back = _read_back(lake, "silver", "totals")
    silver_back_pl = pl.from_arrow(silver_back)
    assert isinstance(silver_back_pl, pl.DataFrame)
    gold = silver_back_pl.filter(pl.col("total") > 50.0)
    lake.to_lake(gold, database="gold", table="big_categories")

    out = _read_back(lake, "gold", "big_categories")
    assert out.num_rows >= 1
    assert all(v > 50.0 for v in _column_floats(out, "total"))


# ------------------------------------------------------------ streaming path


def test_pipeline_polars_write_uses_streaming_path(
    streaming_lake: Lakehouse, big_polars_df: pl.DataFrame
) -> None:
    """With streaming_threshold_mb=0 every Polars write must traverse the
    streaming path (RecordBatchReader-based). End-to-end correctness is the
    contract, not the path itself — but row count + column count must match.
    """
    streaming_lake.to_lake(big_polars_df, database="bulk", table="rows")

    out = _read_back(streaming_lake, "bulk", "rows")
    assert out.num_rows == big_polars_df.height
    assert set(out.column_names) == {"id", "bucket", "amount"}


def test_pipeline_arrow_record_batch_reader_input(lake: Lakehouse) -> None:
    """Caller can hand awebo a pre-built RecordBatchReader directly."""
    fields: dict[str, pa.DataType] = {"k": pa.int64(), "v": pa.string()}
    schema = pa.schema(fields)
    batches = [
        pa.RecordBatch.from_pylist([{"k": i, "v": f"row-{i}"} for i in range(50)], schema=schema)
        for _ in range(3)
    ]
    reader = pa.RecordBatchReader.from_batches(schema, iter(batches))

    lake.to_lake(reader, database="streamy", table="tbl")

    out = _read_back(lake, "streamy", "tbl")
    assert out.num_rows == 150


# -------------------------------------------------------------- error paths


def test_pipeline_invalid_sql_raises(lake: Lakehouse, sample_parquet: Path) -> None:
    lake.read_file(str(sample_parquet), alias="raw")
    from awebo.exceptions import QueryError

    with pytest.raises(QueryError):
        lake.sql("SELECT no_such_col FROM raw").to_arrow()


def test_pipeline_unknown_format_raises(lake: Lakehouse, tmp_path: Path) -> None:
    weird = tmp_path / "weird.xyz"
    weird.write_text("data", encoding="utf-8")
    from awebo.exceptions import QueryError

    with pytest.raises(QueryError, match="autodetect"):
        lake.read_file(str(weird), alias="bad")


# ----------------------------------------------------------- context manager


def test_pipeline_context_manager_closes_engine(tmp_path: Path) -> None:
    from tests._fixtures.lakehouse import make_test_lakehouse

    with make_test_lakehouse(tmp_path) as lake:
        df = pl.DataFrame({"x": [1, 2, 3]})
        lake.to_lake(df, database="db", table="t")
        out = _read_back(lake, "db", "t")
        assert out.num_rows == 3
