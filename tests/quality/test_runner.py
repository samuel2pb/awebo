"""Tests for DuckDBSQLRunner — runs SQL quality checks against DuckDB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from awebo.quality.types import QualityCheck, QualitySuite

if TYPE_CHECKING:
    from awebo.quality.runner import DuckDBSQLRunner


def test_check_passes_when_zero_rows(
    quality_runner: DuckDBSQLRunner,
) -> None:
    runner = quality_runner
    suite = QualitySuite(
        name="basic",
        checks=[
            QualityCheck(
                name="all_ids_positive",
                description="All IDs should be positive",
                expression="SELECT * FROM {table} WHERE id <= 0",
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
    quality_runner: DuckDBSQLRunner,
) -> None:
    runner = quality_runner
    suite = QualitySuite(
        name="nulls",
        checks=[
            QualityCheck(
                name="no_null_names",
                description="Name column should have no nulls",
                expression="SELECT * FROM {table} WHERE name IS NULL",
                severity="error",
            ),
        ],
    )
    result = runner.run(suite, "test_data")

    assert result.passed is False
    assert result.results[0].passed is False
    assert result.results[0].failure_rows == 1


def test_error_severity_fails_suite(
    quality_runner: DuckDBSQLRunner,
) -> None:
    runner = quality_runner
    suite = QualitySuite(
        name="strict",
        checks=[
            QualityCheck(
                name="no_nulls",
                description="Error on nulls",
                expression="SELECT * FROM {table} WHERE name IS NULL",
                severity="error",
            ),
            QualityCheck(
                name="ids_positive",
                description="IDs positive",
                expression="SELECT * FROM {table} WHERE id <= 0",
                severity="error",
            ),
        ],
    )
    result = runner.run(suite, "test_data")

    assert result.passed is False
    assert result.results[0].passed is False
    assert result.results[1].passed is True


def test_check_execution_is_timed(
    quality_runner: DuckDBSQLRunner,
) -> None:
    runner = quality_runner
    suite = QualitySuite(
        name="timed",
        checks=[
            QualityCheck(
                name="simple",
                description="Quick check",
                expression="SELECT * FROM {table} WHERE id < 0",
                severity="error",
            ),
        ],
    )
    result = runner.run(suite, "test_data")
    assert result.results[0].duration_ms >= 0.0
