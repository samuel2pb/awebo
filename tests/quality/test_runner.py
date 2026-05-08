"""Tests for DuckDBSQLRunner — runs SQL quality checks against DuckDB."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest

from awebo.lineage.emitter import NoopEmitter
from awebo.quality.runner import DuckDBSQLRunner
from awebo.quality.types import QualityCheck, QualitySuite
from awebo.query.engine import QueryEngine

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture
def runner_env(tmp_path: Path) -> Iterator[tuple[DuckDBSQLRunner, QueryEngine]]:
    engine = QueryEngine(temp_dir=str(tmp_path / "duckdb_tmp"))

    arrow_tbl = pl.DataFrame({"id": [1, 2, 3], "value": [10, None, 30]}).to_arrow()
    engine.register_arrow("test_data", arrow_tbl)

    runner = DuckDBSQLRunner(engine=engine, emitter=NoopEmitter())
    try:
        yield runner, engine
    finally:
        engine.close()


def test_check_passes_when_zero_rows(
    runner_env: tuple[DuckDBSQLRunner, QueryEngine],
) -> None:
    runner, _ = runner_env
    suite = QualitySuite(
        name="basic",
        checks=[
            QualityCheck(
                name="all_ids_positive",
                description="All IDs should be positive",
                query="SELECT * FROM {table} WHERE id <= 0",
                severity="error",
            ),
        ],
    )
    result = runner.run(suite, "test_data")

    assert result.passed is True
    assert len(result.results) == 1
    assert result.results[0].passed is True
    assert result.results[0].failure_rows == 0


def test_check_fails_when_rows_returned(
    runner_env: tuple[DuckDBSQLRunner, QueryEngine],
) -> None:
    runner, _ = runner_env
    suite = QualitySuite(
        name="nulls",
        checks=[
            QualityCheck(
                name="no_null_values",
                description="Value column should have no nulls",
                query="SELECT * FROM {table} WHERE value IS NULL",
                severity="error",
            ),
        ],
    )
    result = runner.run(suite, "test_data")

    assert result.passed is False
    assert result.results[0].passed is False
    assert result.results[0].failure_rows == 1


def test_warning_severity_does_not_fail_suite(
    runner_env: tuple[DuckDBSQLRunner, QueryEngine],
) -> None:
    runner, _ = runner_env
    suite = QualitySuite(
        name="mixed",
        checks=[
            QualityCheck(
                name="no_nulls",
                description="Warn on nulls",
                query="SELECT * FROM {table} WHERE value IS NULL",
                severity="warning",
            ),
        ],
    )
    result = runner.run(suite, "test_data")

    assert result.passed is True
    assert result.results[0].passed is False


def test_error_severity_fails_suite(
    runner_env: tuple[DuckDBSQLRunner, QueryEngine],
) -> None:
    runner, _ = runner_env
    suite = QualitySuite(
        name="strict",
        checks=[
            QualityCheck(
                name="no_nulls",
                description="Error on nulls",
                query="SELECT * FROM {table} WHERE value IS NULL",
                severity="error",
            ),
            QualityCheck(
                name="ids_positive",
                description="IDs positive",
                query="SELECT * FROM {table} WHERE id <= 0",
                severity="error",
            ),
        ],
    )
    result = runner.run(suite, "test_data")

    assert result.passed is False
    assert result.results[0].passed is False
    assert result.results[1].passed is True


def test_check_execution_is_timed(
    runner_env: tuple[DuckDBSQLRunner, QueryEngine],
) -> None:
    runner, _ = runner_env
    suite = QualitySuite(
        name="timed",
        checks=[
            QualityCheck(
                name="simple",
                description="Quick check",
                query="SELECT * FROM {table} WHERE id < 0",
                severity="error",
            ),
        ],
    )
    result = runner.run(suite, "test_data")
    assert result.results[0].duration_ms >= 0.0
