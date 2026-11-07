"""Tests for to_lake mode, schema/partition_spec arguments, schema evolution,
partition evolution, dynamic partition overwrite, and the pyiceberg_catalog
escape hatch.

Backed by the same offline ``lake`` fixture used by the rest of the pipeline
suite, so the production writer path is exercised end-to-end.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform, YearTransform
from pyiceberg.types import DateType, LongType, NestedField, StringType, TimestamptzType

from awebo.exceptions import WriteError

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable

    from awebo import Lakehouse
    from tests._fixtures.catalog import LocalIcebergCatalogBackend


def _read_back(lake: Lakehouse, database: str, table: str) -> pa.Table:
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    catalog = backend.pyiceberg(lake._credentials)
    return catalog.load_table(f"{database}.{table}").scan().to_arrow()


def _load_table(lake: Lakehouse, database: str, table: str) -> PyIcebergTable:
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    catalog = backend.pyiceberg(lake._credentials)
    return catalog.load_table(f"{database}.{table}")


def _partitioned_on(tbl: PyIcebergTable) -> set[tuple[str, str]]:
    """(source column, transform) pairs of the current spec.

    Partition *field names* are chosen by the engine that writes the spec
    (DuckDB names them like ``identity_year_1``), so tests compare what the
    table partitions on instead.
    """
    schema = tbl.schema()
    return {(schema.find_field(f.source_id).name, str(f.transform)) for f in tbl.spec().fields}


# -------------------------------------------------------------- overwrite mode


def test_overwrite_replaces_rows(lake: Lakehouse) -> None:
    first = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    second = pl.DataFrame({"id": [10, 20], "name": ["x", "y"]})

    lake.to_lake(first, database="db", table="t", mode="append")
    lake.to_lake(second, database="db", table="t", mode="overwrite")

    out = _read_back(lake, "db", "t")
    assert out.num_rows == 2
    assert sorted(int(v) for v in out.column("id").to_pylist() if v is not None) == [10, 20]


def test_overwrite_creates_table_when_missing(lake: Lakehouse) -> None:
    df = pl.DataFrame({"id": [1, 2]})
    lake.to_lake(df, database="db", table="fresh", mode="overwrite")
    assert _read_back(lake, "db", "fresh").num_rows == 2


def test_overwrite_accepts_record_batch_reader(lake: Lakehouse) -> None:
    lake.to_lake(pl.DataFrame({"id": [1, 2]}), database="db", table="rbr")
    schema = pa.schema({"id": pa.int64()})
    batch = pa.RecordBatch.from_pylist([{"id": 10}], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, iter([batch]))
    lake.to_lake(reader, database="db", table="rbr", mode="overwrite")
    out = _read_back(lake, "db", "rbr")
    assert out.num_rows == 1
    assert out.column("id").to_pylist() == [10]


def test_overwrite_accepts_large_payload(spill_lake: Lakehouse) -> None:
    """Overwrite under a tight DuckDB memory limit."""
    df = pl.DataFrame({"id": list(range(100))})
    spill_lake.to_lake(df, database="db", table="t", mode="overwrite")
    assert _read_back(spill_lake, "db", "t").num_rows == 100


def test_streaming_overwrite_replaces_data(spill_lake: Lakehouse) -> None:
    """Overwrite under a tight memory limit replaces existing rows in one transaction."""
    first = pl.DataFrame({"id": [1, 2, 3]})
    spill_lake.to_lake(first, database="db", table="strov")

    second = pl.DataFrame({"id": [10, 20]})
    spill_lake.to_lake(second, database="db", table="strov", mode="overwrite")

    out = _read_back(spill_lake, "db", "strov")
    assert out.num_rows == 2
    assert sorted(int(v) for v in out.column("id").to_pylist() if v is not None) == [10, 20]


def test_invalid_mode_raises(lake: Lakehouse) -> None:
    with pytest.raises(WriteError, match="invalid write mode"):
        lake.to_lake(
            pl.DataFrame({"id": [1]}),
            database="db",
            table="t",
            mode="merge",  # type: ignore[arg-type]
        )


# ------------------------------------------------- dynamic partition overwrite


def test_dynamic_partition_overwrite_identity(lake: Lakehouse) -> None:
    """Overwrite on identity-partitioned table replaces only touched partitions."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year")
    )

    initial = pl.DataFrame({"year": [2023, 2023, 2024, 2024], "value": [1, 2, 3, 4]})
    lake.to_lake(initial, database="db", table="part", schema=iceberg_schema, partition_spec=spec)

    update_2024 = pl.DataFrame({"year": [2024, 2024], "value": [30, 40]})
    lake.to_lake(update_2024, database="db", table="part", mode="overwrite")

    out = _read_back(lake, "db", "part")
    rows = out.to_pydict()
    year_2023 = [rows["value"][i] for i in range(out.num_rows) if rows["year"][i] == 2023]
    year_2024 = [rows["value"][i] for i in range(out.num_rows) if rows["year"][i] == 2024]

    assert sorted(year_2023) == [1, 2], "2023 partition should be untouched"
    assert sorted(year_2024) == [30, 40], "2024 partition should be replaced"


def test_dynamic_partition_overwrite_multi_partition(lake: Lakehouse) -> None:
    """Dynamic overwrite with multiple identity partition fields."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="region", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="region"),
    )

    initial = pl.DataFrame(
        {
            "year": [2023, 2023, 2024],
            "region": ["US", "EU", "US"],
            "value": [1, 2, 3],
        }
    )
    lake.to_lake(initial, database="db", table="mp", schema=iceberg_schema, partition_spec=spec)

    update = pl.DataFrame({"year": [2024], "region": ["US"], "value": [99]})
    lake.to_lake(update, database="db", table="mp", mode="overwrite")

    out = _read_back(lake, "db", "mp")
    rows_by_key = {(r["year"], r["region"]): r["value"] for r in out.to_pylist()}
    assert rows_by_key[(2023, "US")] == 1, "2023/US untouched"
    assert rows_by_key[(2023, "EU")] == 2, "2023/EU untouched"
    assert rows_by_key[(2024, "US")] == 99, "2024/US replaced"


def test_sql_streaming_overwrite_partition_scoped(lake: Lakehouse) -> None:
    """SQL overwrite on partitioned table only deletes touched partitions."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year")
    )

    initial = pl.DataFrame({"year": [2023, 2023, 2024, 2024], "value": [1, 2, 3, 4]})
    lake.to_lake(
        initial, database="db", table="sqlpart", schema=iceberg_schema, partition_spec=spec
    )

    lake.to_lake(
        "SELECT 2024 AS year, 30 AS value UNION ALL SELECT 2024, 40",
        database="db",
        table="sqlpart",
        mode="overwrite",
    )

    out = _read_back(lake, "db", "sqlpart")
    rows = out.to_pydict()
    year_2023 = [rows["value"][i] for i in range(out.num_rows) if rows["year"][i] == 2023]
    year_2024 = [rows["value"][i] for i in range(out.num_rows) if rows["year"][i] == 2024]

    assert sorted(year_2023) == [1, 2], "2023 partition should be untouched"
    assert sorted(year_2024) == [30, 40], "2024 partition should be replaced"


def test_day_transform_overwrite_scoped_to_partition(lake: Lakehouse) -> None:
    """Overwrite on day()-partitioned table replaces only touched day partitions."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="ts", field_type=TimestamptzType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="day")
    )
    ts_type = pa.timestamp("us", tz="UTC")
    initial = pa.table(
        {
            "ts": pa.array(
                [datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 6, 15, tzinfo=UTC)],
                type=ts_type,
            ),
            "value": [1, 2],
        }
    )
    lake.to_lake(
        initial, database="db", table="daypart", schema=iceberg_schema, partition_spec=spec
    )

    replacement = pa.table(
        {
            "ts": pa.array(
                [datetime(2024, 6, 15, tzinfo=UTC)],
                type=ts_type,
            ),
            "value": [99],
        }
    )
    lake.to_lake(replacement, database="db", table="daypart", mode="overwrite")

    out = _read_back(lake, "db", "daypart")
    rows = {r["value"] for r in out.to_pylist()}
    assert out.num_rows == 2, "day-transform overwrite should be scoped, not full replacement"
    assert 1 in rows, "2024-01-01 partition should be untouched"
    assert 99 in rows, "2024-06-15 partition should be replaced"


def test_dynamic_partition_overwrite_three_column_key(lake: Lakehouse) -> None:
    """Composite partition with 3 identity columns."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="region", field_type=StringType(), required=False),
        NestedField(field_id=3, name="category", field_type=StringType(), required=False),
        NestedField(field_id=4, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="region"),
        PartitionField(source_id=3, field_id=1002, transform=IdentityTransform(), name="category"),
    )

    initial = pl.DataFrame(
        {
            "year": [2023, 2023, 2024],
            "region": ["US", "EU", "US"],
            "category": ["A", "B", "A"],
            "value": [1, 2, 3],
        }
    )
    lake.to_lake(initial, database="db", table="tri", schema=iceberg_schema, partition_spec=spec)

    update = pl.DataFrame({"year": [2024], "region": ["US"], "category": ["A"], "value": [99]})
    lake.to_lake(update, database="db", table="tri", mode="overwrite")

    out = _read_back(lake, "db", "tri")
    rows_by_key = {(r["year"], r["region"], r["category"]): r["value"] for r in out.to_pylist()}
    assert rows_by_key[(2023, "US", "A")] == 1, "untouched"
    assert rows_by_key[(2023, "EU", "B")] == 2, "untouched"
    assert rows_by_key[(2024, "US", "A")] == 99, "replaced"


def test_sql_overwrite_multi_column_partition(lake: Lakehouse) -> None:
    """SQL overwrite on table with 2 identity partitions builds correct filter."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="region", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="region"),
    )

    initial = pl.DataFrame(
        {
            "year": [2023, 2023, 2024],
            "region": ["US", "EU", "US"],
            "value": [1, 2, 3],
        }
    )
    lake.to_lake(initial, database="db", table="sqlmp", schema=iceberg_schema, partition_spec=spec)

    lake.to_lake(
        "SELECT 2024 AS year, 'US' AS region, 99 AS value",
        database="db",
        table="sqlmp",
        mode="overwrite",
    )

    out = _read_back(lake, "db", "sqlmp")
    rows_by_key = {(r["year"], r["region"]): r["value"] for r in out.to_pylist()}
    assert rows_by_key[(2023, "US")] == 1, "2023/US untouched"
    assert rows_by_key[(2023, "EU")] == 2, "2023/EU untouched"
    assert rows_by_key[(2024, "US")] == 99, "2024/US replaced"


# ------------------------------------ non-identity / mixed partition overwrite


def test_sql_overwrite_year_transform_scoped(lake: Lakehouse) -> None:
    """SQL overwrite on year()-partitioned table replaces only touched year partitions."""
    from datetime import date

    iceberg_schema = Schema(
        NestedField(field_id=1, name="trade_date", field_type=DateType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=YearTransform(), name="trade_year"),
    )

    initial = pl.DataFrame(
        {
            "trade_date": [date(2023, 6, 1), date(2024, 6, 1)],
            "value": [1, 2],
        }
    )
    lake.to_lake(
        initial, database="db", table="sqlyear", schema=iceberg_schema, partition_spec=spec
    )

    lake.to_lake(
        "SELECT DATE '2024-06-01' AS trade_date, 99 AS value",
        database="db",
        table="sqlyear",
        mode="overwrite",
    )

    out = _read_back(lake, "db", "sqlyear")
    rows_by_value = {r["value"] for r in out.to_pylist()}
    assert out.num_rows == 2, "SQL year-transform overwrite should be scoped"
    assert 1 in rows_by_value, "2023 partition should be untouched"
    assert 99 in rows_by_value, "2024 partition should be replaced"


def test_year_transform_overwrite_scoped_to_partition(lake: Lakehouse) -> None:
    """Overwrite on year()-partitioned table replaces only touched year partitions."""
    from datetime import date

    iceberg_schema = Schema(
        NestedField(field_id=1, name="trade_date", field_type=DateType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=YearTransform(), name="trade_year"),
    )

    initial = pl.DataFrame(
        {
            "trade_date": [date(2023, 6, 1), date(2024, 6, 1)],
            "value": [1, 2],
        }
    )
    lake.to_lake(
        initial, database="db", table="yearonly", schema=iceberg_schema, partition_spec=spec
    )

    update = pl.DataFrame({"trade_date": [date(2024, 6, 1)], "value": [99]})
    lake.to_lake(update, database="db", table="yearonly", mode="overwrite")

    out = _read_back(lake, "db", "yearonly")
    rows_by_value = {r["value"] for r in out.to_pylist()}
    assert out.num_rows == 2, "year-transform overwrite should be scoped, not full replacement"
    assert 1 in rows_by_value, "2023 partition should be untouched"
    assert 99 in rows_by_value, "2024 partition should be replaced"


def test_overwrite_mixed_identity_and_year_partition(lake: Lakehouse) -> None:
    """Mixed spec: identity logical_year + year(trade_date) — overwrite by
    identity slice (year-by-year backfill pattern for exchange price history)."""
    from datetime import date

    iceberg_schema = Schema(
        NestedField(field_id=1, name="trade_date", field_type=DateType(), required=False),
        NestedField(field_id=2, name="logical_year", field_type=LongType(), required=False),
        NestedField(field_id=3, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=YearTransform(), name="trade_year"),
        PartitionField(
            source_id=2, field_id=1001, transform=IdentityTransform(), name="logical_year"
        ),
    )

    initial = pl.DataFrame(
        {
            "trade_date": [date(2023, 6, 1), date(2024, 6, 1)],
            "logical_year": [2023, 2024],
            "value": [1, 2],
        }
    )
    lake.to_lake(initial, database="db", table="mixed", schema=iceberg_schema, partition_spec=spec)

    update = pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 1)],
            "logical_year": [2024],
            "value": [99],
        }
    )
    lake.to_lake(update, database="db", table="mixed", mode="overwrite")

    out = _read_back(lake, "db", "mixed")
    rows_by_year = {r["logical_year"]: r["value"] for r in out.to_pylist()}
    assert rows_by_year[2023] == 1, "2023 partition untouched"
    assert rows_by_year[2024] == 99, "2024 partition replaced"


# ---------------------------------------- empty arrow overwrite safety (#25)


def test_empty_arrow_overwrite_preserves_mixed_partitioned_table(lake: Lakehouse) -> None:
    """Empty arrow on overwrite must not wipe existing partitions (mixed spec)."""
    from datetime import date

    iceberg_schema = Schema(
        NestedField(field_id=1, name="trade_date", field_type=DateType(), required=False),
        NestedField(field_id=2, name="fiscal_year", field_type=LongType(), required=False),
        NestedField(field_id=3, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=YearTransform(), name="trade_year"),
        PartitionField(
            source_id=2, field_id=1001, transform=IdentityTransform(), name="fiscal_year"
        ),
    )

    initial = pl.DataFrame(
        {
            "trade_date": [date(2023, 6, 1), date(2024, 6, 1)],
            "fiscal_year": [2023, 2024],
            "value": [1, 2],
        }
    )
    lake.to_lake(
        initial, database="db", table="empty_mixed", schema=iceberg_schema, partition_spec=spec
    )

    empty = pa.table(
        {
            "trade_date": pa.array([], type=pa.date32()),
            "fiscal_year": pa.array([], type=pa.int64()),
            "value": pa.array([], type=pa.int64()),
        }
    )
    lake.to_lake(empty, database="db", table="empty_mixed", mode="overwrite")

    out = _read_back(lake, "db", "empty_mixed")
    assert out.num_rows == 2, "empty overwrite must not delete existing rows"


def test_empty_arrow_overwrite_preserves_unpartitioned_table(lake: Lakehouse) -> None:
    """Empty arrow on overwrite must not wipe an unpartitioned table."""
    initial = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    lake.to_lake(initial, database="db", table="empty_unpart")

    empty = pa.table(
        {"id": pa.array([], type=pa.int64()), "name": pa.array([], type=pa.large_string())}
    )
    lake.to_lake(empty, database="db", table="empty_unpart", mode="overwrite")

    out = _read_back(lake, "db", "empty_unpart")
    assert out.num_rows == 3, "empty overwrite must not delete existing rows"


def test_empty_polars_overwrite_preserves_data(lake: Lakehouse) -> None:
    """Empty Polars DataFrame on overwrite must not wipe existing data."""
    initial = pl.DataFrame({"id": [1, 2, 3]})
    lake.to_lake(initial, database="db", table="empty_pl")

    empty = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
    lake.to_lake(empty, database="db", table="empty_pl", mode="overwrite")

    out = _read_back(lake, "db", "empty_pl")
    assert out.num_rows == 3, "empty overwrite must not delete existing rows"


def test_empty_sql_streaming_overwrite_preserves_data(lake: Lakehouse) -> None:
    """Empty SQL result on streaming overwrite must not wipe existing data."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year")
    )

    initial = pl.DataFrame({"year": [2023, 2024], "value": [1, 2]})
    lake.to_lake(
        initial, database="db", table="empty_sql", schema=iceberg_schema, partition_spec=spec
    )

    lake.to_lake(
        "SELECT 2024 AS year, 99 AS value WHERE false",
        database="db",
        table="empty_sql",
        mode="overwrite",
    )

    out = _read_back(lake, "db", "empty_sql")
    assert out.num_rows == 2, "empty SQL overwrite must not delete existing rows"


def test_empty_record_batch_reader_overwrite_preserves_data(lake: Lakehouse) -> None:
    """Empty RecordBatchReader on overwrite must not wipe existing data."""
    initial = pl.DataFrame({"id": [1, 2, 3]})
    lake.to_lake(initial, database="db", table="empty_rbr")

    schema = pa.schema([pa.field("id", pa.int64())])
    empty_reader = pa.RecordBatchReader.from_batches(schema, iter([]))
    lake.to_lake(empty_reader, database="db", table="empty_rbr", mode="overwrite")

    out = _read_back(lake, "db", "empty_rbr")
    assert out.num_rows == 3, "empty overwrite must not delete existing rows"


def test_empty_arrow_overwrite_preserves_transform_only_partitioned_table(lake: Lakehouse) -> None:
    """Empty arrow on overwrite must not wipe a transform-only partitioned table."""
    from datetime import date

    iceberg_schema = Schema(
        NestedField(field_id=1, name="trade_date", field_type=DateType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=YearTransform(), name="trade_year"),
    )

    initial = pl.DataFrame({"trade_date": [date(2023, 6, 1), date(2024, 6, 1)], "value": [1, 2]})
    lake.to_lake(
        initial, database="db", table="empty_yr", schema=iceberg_schema, partition_spec=spec
    )

    empty = pa.table(
        {"trade_date": pa.array([], type=pa.date32()), "value": pa.array([], type=pa.int64())}
    )
    lake.to_lake(empty, database="db", table="empty_yr", mode="overwrite")

    out = _read_back(lake, "db", "empty_yr")
    assert out.num_rows == 2, "empty overwrite must not delete existing rows"


# ----------------------------------------------------- explicit schema/partition


def test_explicit_schema_and_partition_spec_on_create(lake: Lakehouse) -> None:
    iceberg_schema = Schema(
        NestedField(field_id=1, name="period_year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="company", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=IdentityTransform(), name="period_year"
        )
    )

    df = pl.DataFrame(
        {
            "period_year": [2024, 2024, 2025],
            "company": ["A", "B", "C"],
            "value": [10, 20, 30],
        }
    )
    lake.to_lake(
        df,
        database="bronze",
        table="filings",
        schema=iceberg_schema,
        partition_spec=spec,
    )

    tbl = _load_table(lake, "bronze", "filings")
    assert _partitioned_on(tbl) == {("period_year", "identity")}
    assert _read_back(lake, "bronze", "filings").num_rows == 3


def test_schema_arg_ignored_when_table_exists(lake: Lakehouse) -> None:
    df = pl.DataFrame({"id": [1, 2]})
    lake.to_lake(df, database="db", table="existing")

    iceberg_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    )
    lake.to_lake(
        pl.DataFrame({"id": [3, 4]}),
        database="db",
        table="existing",
        schema=iceberg_schema,
    )

    out = _read_back(lake, "db", "existing")
    assert sorted(int(v) for v in out.column("id").to_pylist() if v is not None) == [1, 2, 3, 4]


# ---------------------------------------------------------- schema evolution


def test_schema_evolution_adds_column(lake: Lakehouse) -> None:
    lake.to_lake(pl.DataFrame({"id": [1, 2]}), database="db", table="evo")

    df2 = pl.DataFrame({"id": [3], "name": ["c"]})
    lake.to_lake(df2, database="db", table="evo", schema_evolution=True)

    tbl = _load_table(lake, "db", "evo")
    field_names = [f.name for f in tbl.schema().fields]
    assert "name" in field_names
    out = _read_back(lake, "db", "evo")
    assert out.num_rows == 3


def test_schema_evolution_widens_type(lake: Lakehouse) -> None:
    """int32 → int64 widening via schema evolution."""
    arrow_int32 = pa.table({"val": pa.array([1, 2], type=pa.int32())})
    lake.to_lake(arrow_int32, database="db", table="widen")

    arrow_int64 = pa.table({"val": pa.array([3], type=pa.int64())})
    lake.to_lake(arrow_int64, database="db", table="widen", schema_evolution=True)

    out = _read_back(lake, "db", "widen")
    assert out.num_rows == 3


def test_schema_evolution_disabled_rejects_new_columns(lake: Lakehouse) -> None:
    """With schema_evolution=False, new columns are rejected."""
    lake.to_lake(pl.DataFrame({"id": [1]}), database="db", table="noevo")

    with pytest.raises(WriteError):
        lake.to_lake(
            pl.DataFrame({"id": [2], "extra": ["x"]}),
            database="db",
            table="noevo",
            schema_evolution=False,
        )


# -------------------------------------------------------- partition evolution


def test_partition_evolution_adds_field(lake: Lakehouse) -> None:
    """Passing partition_spec on existing table evolves the spec."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="region", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=LongType(), required=False),
    )
    initial_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
    )
    lake.to_lake(
        pl.DataFrame({"year": [2024], "region": ["US"], "value": [1]}),
        database="db",
        table="evo_part",
        schema=iceberg_schema,
        partition_spec=initial_spec,
    )

    evolved_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="region"),
    )
    lake.to_lake(
        pl.DataFrame({"year": [2025], "region": ["EU"], "value": [2]}),
        database="db",
        table="evo_part",
        partition_spec=evolved_spec,
    )

    tbl = _load_table(lake, "db", "evo_part")
    assert _partitioned_on(tbl) == {("year", "identity"), ("region", "identity")}


def test_partition_evolution_noop_when_same(lake: Lakehouse) -> None:
    """No error when partition_spec matches current spec."""
    iceberg_schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
    )
    lake.to_lake(
        pl.DataFrame({"year": [2024], "value": [1]}),
        database="db",
        table="same_part",
        schema=iceberg_schema,
        partition_spec=spec,
    )
    lake.to_lake(
        pl.DataFrame({"year": [2025], "value": [2]}),
        database="db",
        table="same_part",
        partition_spec=spec,
    )
    out = _read_back(lake, "db", "same_part")
    assert out.num_rows == 2


# ------------------------------------------------------- escape hatch


def test_pyiceberg_catalog_returns_handle(lake: Lakehouse) -> None:
    cat = lake.pyiceberg_catalog()
    assert cat is not None
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    assert cat is backend.pyiceberg(lake._credentials)


def test_pyiceberg_catalog_supports_direct_use(lake: Lakehouse) -> None:
    df = pl.DataFrame({"id": [1, 2, 3]})
    lake.to_lake(df, database="logs", table="runs")

    cat = lake.pyiceberg_catalog()
    tbl = cat.load_table("logs.runs")
    arrow = tbl.scan().to_arrow()
    assert arrow.num_rows == 3


# ------------------------------------------------------- health check


def test_health_check_returns_status(lake: Lakehouse) -> None:
    status = lake.health_check()
    assert status["duckdb"] == "ok"
    assert status["catalog"] == "ok"
