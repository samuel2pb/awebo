"""End-to-end pipeline tests for lineage emission and quality checks.

These tests exercise the lineage and quality features through the full
Lakehouse facade — not component isolation. Data flows through
``to_lake()`` / ``sql()`` exactly as production, with a SpyEmitter
capturing emitted OpenLineage events for assertion.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import polars as pl
import pyarrow as pa
import pytest

from awebo.exceptions import WriteError
from awebo.quality.runner import DuckDBSQLRunner
from awebo.quality.types import QualityCheck, QualitySuite

if TYPE_CHECKING:
    from pathlib import Path

    from openlineage.client.run import RunEvent

    from awebo import Lakehouse
    from tests._fixtures.catalog import LocalIcebergCatalogBackend


# ---------------------------------------------------------------- spy emitter


class SpyEmitter:
    """Collects emitted RunEvents for test assertions."""

    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)

    def event_types(self) -> list[str]:
        return [e.eventType.name for e in self.events]


# ------------------------------------------------------------------ helpers


def _make_lake_with_spy(tmp_path: Path) -> tuple[Lakehouse, SpyEmitter]:
    from tests._fixtures.lakehouse import make_test_lakehouse

    spy = SpyEmitter()
    lake = make_test_lakehouse(tmp_path, emitter=spy)
    return lake, spy


def _read_back(lake: Lakehouse, database: str, table: str) -> pa.Table:
    backend = cast("LocalIcebergCatalogBackend", lake._catalog)
    catalog = backend.pyiceberg(lake._credentials)
    return catalog.load_table(f"{database}.{table}").scan().to_arrow()


# --------------------------------------------------------- lineage via facade


def test_pipeline_write_emits_start_and_complete(tmp_path: Path) -> None:
    """Lakehouse.to_lake() must emit START then COMPLETE on success."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        lake.to_lake(df, database="sales", table="orders")

        assert len(spy.events) == 2
        assert spy.event_types() == ["START", "COMPLETE"]

        start, complete = spy.events
        assert start.job.name == "write_sales.orders"
        assert complete.job.name == "write_sales.orders"
        assert start.run.runId == complete.run.runId
    finally:
        lake.close()


def test_pipeline_write_failure_emits_start_and_fail(tmp_path: Path) -> None:
    """Lakehouse.to_lake() must emit START then FAIL on write error."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        with pytest.raises(WriteError):
            lake.to_lake(12345, database="db", table="t")  # type: ignore[arg-type]

        assert len(spy.events) == 2
        assert spy.event_types() == ["START", "FAIL"]
        assert spy.events[0].run.runId == spy.events[1].run.runId
    finally:
        lake.close()


def test_pipeline_multiple_writes_emit_independent_runs(tmp_path: Path) -> None:
    """Each to_lake() call gets its own run_id pair."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        lake.to_lake(pl.DataFrame({"x": [1]}), database="db", table="t1")
        lake.to_lake(pl.DataFrame({"y": [2]}), database="db", table="t2")

        assert len(spy.events) == 4
        assert spy.event_types() == ["START", "COMPLETE", "START", "COMPLETE"]

        run1 = spy.events[0].run.runId
        run2 = spy.events[2].run.runId
        assert run1 != run2
        assert spy.events[0].run.runId == spy.events[1].run.runId
        assert spy.events[2].run.runId == spy.events[3].run.runId
    finally:
        lake.close()


def test_pipeline_append_write_emits_lineage(tmp_path: Path) -> None:
    """Append to existing table still emits lineage pair."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        df1 = pl.DataFrame({"id": [1, 2]})
        df2 = pl.DataFrame({"id": [3, 4]})
        lake.to_lake(df1, database="db", table="t")
        lake.to_lake(df2, database="db", table="t")

        assert len(spy.events) == 4
        assert spy.event_types() == ["START", "COMPLETE", "START", "COMPLETE"]

        out = _read_back(lake, "db", "t")
        assert out.num_rows == 4
    finally:
        lake.close()


def test_pipeline_sql_string_write_emits_lineage(
    tmp_path: Path,
) -> None:
    """SQL-string write path also emits lineage."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        seed = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
        lake.to_lake(seed, database="raw", table="seed")

        parquet_path = _write_iceberg_as_parquet(lake, "raw", "seed", tmp_path)
        lake.read_file(str(parquet_path), alias="seed_view")
        spy.events.clear()

        lake.to_lake(
            data="SELECT * FROM seed_view",
            database="derived",
            table="copy",
        )

        assert spy.event_types() == ["START", "COMPLETE"]
        assert spy.events[0].job.name == "write_derived.copy"
    finally:
        lake.close()


# ---------------------------------------------------------- quality via facade


def test_pipeline_quality_checks_pass_on_clean_data(tmp_path: Path) -> None:
    """Quality suite against Iceberg data written through Lakehouse."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        df = pl.DataFrame(
            {"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]},
        )
        lake.to_lake(df, database="bronze", table="metrics")

        lake.read_file(
            str(_write_iceberg_as_parquet(lake, "bronze", "metrics", tmp_path)),
            alias="metrics",
        )

        suite = QualitySuite(
            name="bronze_checks",
            checks=[
                QualityCheck(
                    name="no_negative_values",
                    description="value must be non-negative",
                    expression="SELECT * FROM {table} WHERE value < 0",
                    severity="error",
                ),
                QualityCheck(
                    name="no_null_ids",
                    description="id must not be null",
                    expression="SELECT * FROM {table} WHERE id IS NULL",
                    severity="error",
                ),
            ],
        )

        runner = DuckDBSQLRunner(
            engine=lake._engine,
            emitter=spy,
            lineage_namespace="awebo",
        )
        result = runner.run(suite, table="metrics")

        assert result.passed is True
        assert len(result.results) == 2
        assert all(r.passed for r in result.results)
    finally:
        lake.close()


def test_pipeline_quality_checks_detect_bad_data(tmp_path: Path) -> None:
    """Quality error check fails when rows violate constraint."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        df = pl.DataFrame(
            {"id": [1, 2, 3], "value": [-5.0, 20.0, -1.0]},
        )
        lake.to_lake(df, database="bronze", table="bad_metrics")

        lake.read_file(
            str(_write_iceberg_as_parquet(lake, "bronze", "bad_metrics", tmp_path)),
            alias="bad_metrics",
        )

        suite = QualitySuite(
            name="validation",
            checks=[
                QualityCheck(
                    name="no_negatives",
                    description="value >= 0",
                    expression="SELECT * FROM {table} WHERE value < 0",
                    severity="error",
                ),
            ],
        )

        runner = DuckDBSQLRunner(
            engine=lake._engine,
            emitter=spy,
            lineage_namespace="awebo",
        )
        result = runner.run(suite, table="bad_metrics")

        assert result.passed is False
        assert result.results[0].passed is False
        assert result.results[0].failure_rows == 2
    finally:
        lake.close()


def test_pipeline_quality_warning_does_not_fail_suite(tmp_path: Path) -> None:
    """Warning-severity check failure does not fail overall suite."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        df = pl.DataFrame({"id": [1, 2], "value": [-1.0, 10.0]})
        lake.to_lake(df, database="db", table="t")

        lake.read_file(
            str(_write_iceberg_as_parquet(lake, "db", "t", tmp_path)),
            alias="warn_data",
        )

        suite = QualitySuite(
            name="soft_checks",
            checks=[
                QualityCheck(
                    name="prefer_positive",
                    description="values should be positive",
                    expression="SELECT * FROM {table} WHERE value < 0",
                    severity="warning",
                ),
            ],
        )

        runner = DuckDBSQLRunner(
            engine=lake._engine,
            emitter=spy,
            lineage_namespace="awebo",
        )
        result = runner.run(suite, table="warn_data")

        assert result.passed is True
        assert result.results[0].passed is False
    finally:
        lake.close()


def test_pipeline_quality_emits_lineage_facet(tmp_path: Path) -> None:
    """Quality runner emits DataQualityAssertions facet through spy emitter."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        df = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
        lake.to_lake(df, database="db", table="checked")
        spy.events.clear()

        lake.read_file(
            str(_write_iceberg_as_parquet(lake, "db", "checked", tmp_path)),
            alias="checked",
        )

        suite = QualitySuite(
            name="lineage_test",
            checks=[
                QualityCheck(
                    name="all_positive",
                    description="all values positive",
                    expression="SELECT * FROM {table} WHERE value <= 0",
                    severity="error",
                ),
            ],
        )

        runner = DuckDBSQLRunner(
            engine=lake._engine,
            emitter=spy,
            lineage_namespace="awebo",
        )
        runner.run(suite, table="checked")

        assert len(spy.events) == 1
        quality_event = spy.events[0]
        assert quality_event.eventType.name == "COMPLETE"
        assert quality_event.job.name == "quality_lineage_test"
        assert quality_event.inputs is not None
        assert len(quality_event.inputs) == 1
    finally:
        lake.close()


def test_pipeline_full_flow_write_quality_lineage(
    tmp_path: Path,
) -> None:
    """Complete flow: write → quality check → verify both lineage streams."""
    lake, spy = _make_lake_with_spy(tmp_path)
    try:
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "category": ["a", "b", "a", "c", "b"],
                "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
            },
        )
        lake.to_lake(df, database="bronze", table="transactions")

        assert spy.event_types() == ["START", "COMPLETE"]
        write_run_id = spy.events[0].run.runId

        lake.read_file(
            str(_write_iceberg_as_parquet(lake, "bronze", "transactions", tmp_path)),
            alias="transactions",
        )

        suite = QualitySuite(
            name="txn_checks",
            checks=[
                QualityCheck(
                    name="positive_amounts",
                    description="amount > 0",
                    expression="SELECT * FROM {table} WHERE amount <= 0",
                    severity="error",
                ),
                QualityCheck(
                    name="known_categories",
                    description="category in (a, b, c)",
                    expression="SELECT * FROM {table} WHERE category NOT IN ('a','b','c')",
                    severity="warning",
                ),
            ],
        )

        runner = DuckDBSQLRunner(
            engine=lake._engine,
            emitter=spy,
            lineage_namespace="awebo",
        )
        result = runner.run(suite, table="transactions")

        assert result.passed is True
        assert all(r.passed for r in result.results)

        assert len(spy.events) == 3
        assert spy.events[0].run.runId == write_run_id
        quality_event = spy.events[2]
        assert quality_event.job.name == "quality_txn_checks"

        out = _read_back(lake, "bronze", "transactions")
        assert out.num_rows == 5
    finally:
        lake.close()


# ---------------------------------------------------------------- helpers


def _write_iceberg_as_parquet(
    lake: Lakehouse,
    database: str,
    table: str,
    tmp_path: Path,
) -> Path:
    """Read back Iceberg table via PyIceberg and write as parquet for DuckDB view."""
    import pyarrow.parquet as pq

    arrow = _read_back(lake, database, table)
    path = tmp_path / f"{database}_{table}.parquet"
    pq.write_table(arrow, path)
    return path
