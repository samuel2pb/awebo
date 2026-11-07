"""Pipeline behaviour enabled by DuckDB 2.0's native Iceberg read + write path.

Every test goes through the public :class:`Lakehouse` verbs against the
REST-served local catalog, and verifies through PyIceberg (``_read_back``)
or through catalog-qualified SQL.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, cast

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, IdentityTransform, TruncateTransform
from pyiceberg.types import DateType, LongType, NestedField, StringType

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable

    from awebo import Lakehouse
    from tests._fixtures.catalog import LocalIcebergCatalogBackend


def _load(lake: Lakehouse, database: str, table: str) -> PyIcebergTable:
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    return backend.pyiceberg(lake._credentials).load_table(f"{database}.{table}")


def _rows(lake: Lakehouse, database: str, table: str) -> list[dict[str, object]]:
    return cast(
        "list[dict[str, object]]", _load(lake, database, table).scan().to_arrow().to_pylist()
    )


def test_written_tables_are_queryable_by_catalog_qualified_sql(lake: Lakehouse) -> None:
    lake.to_lake(pl.DataFrame({"id": [1, 2, 3]}), database="sales", table="orders")

    out = lake.sql("SELECT sum(id) AS total FROM test_lake.sales.orders").to_polars()

    assert out["total"].to_list() == [6]


def test_sql_reads_an_earlier_snapshot_with_time_travel(lake: Lakehouse) -> None:
    lake.to_lake(pl.DataFrame({"id": [1]}), database="db", table="tt")
    first_snapshot = _load(lake, "db", "tt").current_snapshot()
    assert first_snapshot is not None
    lake.to_lake(pl.DataFrame({"id": [2]}), database="db", table="tt")

    past = lake.sql(
        f"SELECT id FROM test_lake.db.tt AT (VERSION => {first_snapshot.snapshot_id})"
    ).to_arrow()

    assert past.column("id").to_pylist() == [1]


def test_overwrite_is_one_atomic_catalog_commit(lake: Lakehouse) -> None:
    """DELETE + INSERT land in a single catalog commit: readers never see the gap."""
    lake.to_lake(pl.DataFrame({"id": [1, 2]}), database="db", table="snap")
    commits_before = len(_load(lake, "db", "snap").metadata.metadata_log)

    lake.to_lake(pl.DataFrame({"id": [3]}), database="db", table="snap", mode="overwrite")

    tbl = _load(lake, "db", "snap")
    assert len(tbl.metadata.metadata_log) == commits_before + 1
    assert [r["id"] for r in tbl.scan().to_arrow().to_pylist()] == [3]


def test_overwrite_scopes_bucket_and_truncate_partitions(lake: Lakehouse) -> None:
    schema = Schema(
        NestedField(field_id=1, name="account", field_type=LongType(), required=False),
        NestedField(field_id=2, name="code", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=BucketTransform(4), name="acct_b"),
        PartitionField(source_id=2, field_id=1001, transform=TruncateTransform(2), name="code_t"),
    )
    # bucket(4): accounts 1 -> 0, 3 -> 3 (different buckets)
    initial = pl.DataFrame(
        {"account": [1, 1, 3], "code": ["AB-1", "CD-1", "AB-9"], "value": [1, 2, 3]}
    )
    lake.to_lake(initial, database="db", table="bt", schema=schema, partition_spec=spec)

    lake.to_lake(
        pl.DataFrame({"account": [1], "code": ["AB-7"], "value": [10]}),
        database="db",
        table="bt",
        mode="overwrite",
    )

    by_code = {r["code"]: r["value"] for r in _rows(lake, "db", "bt")}
    assert by_code == {"AB-7": 10, "CD-1": 2, "AB-9": 3}


def test_record_batch_reader_overwrite_is_partition_scoped(lake: Lakehouse) -> None:
    schema = Schema(
        NestedField(field_id=1, name="year", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year")
    )
    lake.to_lake(
        pl.DataFrame({"year": [2023, 2024], "value": [1, 2]}),
        database="db",
        table="rbr_part",
        schema=schema,
        partition_spec=spec,
    )
    update = pa.table({"year": pa.array([2024], pa.int64()), "value": pa.array([20], pa.int64())})

    lake.to_lake(update.to_reader(), database="db", table="rbr_part", mode="overwrite")

    assert {r["year"]: r["value"] for r in _rows(lake, "db", "rbr_part")} == {2023: 1, 2024: 20}


def test_overwrite_scopes_null_partition_values(lake: Lakehouse) -> None:
    schema = Schema(
        NestedField(field_id=1, name="region", field_type=StringType(), required=False),
        NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="region")
    )
    lake.to_lake(
        pl.DataFrame({"region": ["US", None], "value": [1, 2]}),
        database="db",
        table="nulls",
        schema=schema,
        partition_spec=spec,
    )

    lake.to_lake(
        pl.DataFrame(
            {"region": [None], "value": [20]}, schema={"region": pl.String, "value": pl.Int64}
        ),
        database="db",
        table="nulls",
        mode="overwrite",
    )

    assert {r["region"]: r["value"] for r in _rows(lake, "db", "nulls")} == {"US": 1, None: 20}


def test_table_properties_are_set_on_create_and_synced(lake: Lakehouse) -> None:
    lake.to_lake(
        pl.DataFrame({"id": [1]}),
        database="db",
        table="props",
        properties={"write.parquet.compression-codec": "zstd"},
    )
    lake.to_lake(
        pl.DataFrame({"id": [2]}),
        database="db",
        table="props",
        properties={"write.parquet.compression-codec": "zstd", "owner": "hyoko"},
    )

    props = _load(lake, "db", "props").properties
    assert props["write.parquet.compression-codec"] == "zstd"
    assert props["owner"] == "hyoko"


def test_polars_lazyframe_is_a_write_source(lake: Lakehouse) -> None:
    lazy = pl.LazyFrame({"id": [1, 2, 3, 4]}).filter(pl.col("id") > 2)

    lake.to_lake(lazy, database="db", table="lazy")

    assert sorted(cast("int", r["id"]) for r in _rows(lake, "db", "lazy")) == [3, 4]


def test_writes_into_tables_created_by_pyiceberg(lake: Lakehouse) -> None:
    """Tables created by awebo < 0.2 (PyIceberg) keep working, ids and all."""
    schema = Schema(
        NestedField(field_id=1, name="trade_date", field_type=DateType(), required=False),
        NestedField(field_id=2, name="ticker", field_type=StringType(), required=False),
        NestedField(field_id=3, name="px", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="ticker")
    )
    catalog = lake.pyiceberg_catalog()
    catalog.create_namespace("legacy")
    catalog.create_table("legacy.prices", schema=schema, partition_spec=spec)
    lake.to_lake(
        pl.DataFrame(
            {"trade_date": [date(2024, 1, 2)] * 2, "ticker": ["PETR4", "VALE3"], "px": [1, 2]}
        ),
        database="legacy",
        table="prices",
    )

    lake.to_lake(
        pl.DataFrame({"trade_date": [date(2024, 1, 3)], "ticker": ["PETR4"], "px": [9]}),
        database="legacy",
        table="prices",
        mode="overwrite",
        partition_spec=spec,
    )

    assert {r["ticker"]: r["px"] for r in _rows(lake, "legacy", "prices")} == {
        "PETR4": 9,
        "VALE3": 2,
    }
    assert [f.name for f in _load(lake, "legacy", "prices").spec().fields] == ["ticker"]


def test_sql_source_may_end_with_a_semicolon(lake: Lakehouse) -> None:
    lake.to_lake("SELECT 1 AS id;\n", database="db", table="semi")

    assert [r["id"] for r in _rows(lake, "db", "semi")] == [1]


def test_day_overwrite_uses_utc_partitions_whatever_the_session_zone(lake: Lakehouse) -> None:
    """Iceberg temporal transforms are UTC; the session TimeZone must not move boundaries."""
    from datetime import UTC, datetime

    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import TimestamptzType

    lake._engine.connection.sql("SET TimeZone = 'America/Sao_Paulo'")
    schema = Schema(
        NestedField(field_id=1, name="ts", field_type=TimestamptzType(), required=False),
        NestedField(field_id=2, name="v", field_type=LongType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="d")
    )
    initial = pa.table(
        {
            "ts": pa.array(
                [datetime(2024, 1, 1, 12, tzinfo=UTC), datetime(2024, 1, 2, 1, tzinfo=UTC)],
                pa.timestamp("us", tz="UTC"),
            ),
            "v": pa.array([1, 2], pa.int64()),
        }
    )
    lake.to_lake(initial, database="db", table="utc", schema=schema, partition_spec=spec)

    update = pa.table(
        {
            "ts": pa.array([datetime(2024, 1, 1, 20, tzinfo=UTC)], pa.timestamp("us", tz="UTC")),
            "v": pa.array([99], pa.int64()),
        }
    )
    lake.to_lake(update, database="db", table="utc", mode="overwrite")

    assert sorted(cast("int", r["v"]) for r in _rows(lake, "db", "utc")) == [2, 99]


def test_lossy_type_mismatch_is_rejected(lake: Lakehouse) -> None:
    from awebo.exceptions import WriteError

    lake.to_lake(
        pa.table({"id": pa.array([1], pa.int64()), "s": pa.array(["a"])}),
        database="db",
        table="strict",
    )

    for bad in (
        pa.table({"id": pa.array([1.5]), "s": pa.array(["b"])}),
        pa.table({"id": pa.array([2], pa.int64()), "s": pa.array([5], pa.int64())}),
    ):
        with pytest.raises(WriteError, match="incompatible"):
            lake.to_lake(bad, database="db", table="strict")

    assert [r["id"] for r in _rows(lake, "db", "strict")] == [1]


def test_narrower_and_all_null_columns_are_accepted(lake: Lakehouse) -> None:
    lake.to_lake(
        pa.table({"id": pa.array([1], pa.int64()), "s": pa.array(["a"])}),
        database="db",
        table="lenient",
    )

    lake.to_lake(
        pa.table({"id": pa.array([2], pa.int32()), "s": pa.array([None], pa.null())}),
        database="db",
        table="lenient",
    )

    assert sorted(cast("int", r["id"]) for r in _rows(lake, "db", "lenient")) == [1, 2]


def test_required_schema_fields_stay_required(lake: Lakehouse) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    lake.to_lake(
        pl.DataFrame({"id": [1], "name": ["a"]}), database="db", table="req", schema=schema
    )

    fields = {f.name: f.required for f in _load(lake, "db", "req").schema().fields}
    assert fields == {"id": True, "name": False}
