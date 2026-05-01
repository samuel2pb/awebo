"""Tests for to_lake mode + schema/partition_spec arguments and the
pyiceberg_catalog escape hatch.

Backed by the same offline ``lake`` fixture used by the rest of the pipeline
suite, so the production writer path is exercised end-to-end.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

from awebo.exceptions import WriteError

if TYPE_CHECKING:
    from awebo import Lakehouse
    from tests._fixtures.catalog import LocalIcebergCatalogBackend


def _read_back(lake: Lakehouse, database: str, table: str) -> pa.Table:
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    catalog = backend.pyiceberg(lake._credentials)
    return catalog.load_table(f"{database}.{table}").scan().to_arrow()


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


def test_overwrite_rejects_record_batch_reader(lake: Lakehouse) -> None:
    schema = pa.schema({"id": pa.int64()})
    batch = pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, iter([batch]))

    with pytest.raises(WriteError, match="RecordBatchReader"):
        lake.to_lake(reader, database="db", table="t", mode="overwrite")


def test_overwrite_rejects_sql_string(lake: Lakehouse) -> None:
    lake.to_lake(pl.DataFrame({"id": [1]}), database="db", table="src")
    with pytest.raises(WriteError, match="SQL-string"):
        lake.to_lake("SELECT * FROM db.src", database="db", table="dst", mode="overwrite")


def test_invalid_mode_raises(lake: Lakehouse) -> None:
    with pytest.raises(WriteError, match="invalid write mode"):
        lake.to_lake(
            pl.DataFrame({"id": [1]}),
            database="db",
            table="t",
            mode="merge",  # type: ignore[arg-type]
        )


def test_overwrite_above_threshold_rejected(streaming_lake: Lakehouse) -> None:
    # streaming_lake has threshold=0 so any payload is "above threshold".
    df = pl.DataFrame({"id": list(range(100))})
    with pytest.raises(WriteError, match="streaming threshold"):
        streaming_lake.to_lake(df, database="db", table="t", mode="overwrite")


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

    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    tbl = backend.pyiceberg(lake._credentials).load_table("bronze.filings")
    assert tbl.spec().fields[0].name == "period_year"
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


# ------------------------------------------------------- escape hatch


def test_pyiceberg_catalog_returns_handle(lake: Lakehouse) -> None:
    cat = lake.pyiceberg_catalog()
    assert cat is not None
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    # Same cached handle as the writer uses.
    assert cat is backend.pyiceberg(lake._credentials)


def test_pyiceberg_catalog_supports_direct_use(lake: Lakehouse) -> None:
    df = pl.DataFrame({"id": [1, 2, 3]})
    lake.to_lake(df, database="logs", table="runs")

    cat = lake.pyiceberg_catalog()
    tbl = cat.load_table("logs.runs")
    arrow = tbl.scan().to_arrow()
    assert arrow.num_rows == 3
