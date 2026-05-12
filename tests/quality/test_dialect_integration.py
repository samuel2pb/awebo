"""Integration tests — Soda and OpenMetadata dialects running through DuckDBSQLRunner."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from awebo.quality.types import CheckThreshold, QualityCheck, QualitySuite

if TYPE_CHECKING:
    from awebo.quality.runner import DuckDBSQLRunner


# ---------------------------------------------------------------- Soda dialect


class TestSodaDialect:
    def test_row_count_passes(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="soda_basic",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="has_rows",
                    description="table not empty",
                    expression="row_count > 0",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")

        assert result.passed is True
        assert result.results[0].metric_value == 5.0
        assert result.results[0].metric_name == "row_count"

    def test_missing_count_detects_nulls(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="null_check",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="no_null_names",
                    description="name not null",
                    expression="missing_count(name) = 0",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")

        assert result.passed is False
        assert result.results[0].passed is False
        assert result.results[0].metric_value == 1.0
        assert result.results[0].failure_rows == 1

    def test_avg_between_passes(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="score_range",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="avg_score",
                    description="average in range",
                    expression="avg(score) between 50 and 100",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")

        assert result.passed is True
        assert result.results[0].metric_value is not None
        assert 50 <= result.results[0].metric_value <= 100

    def test_avg_between_fails(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="tight_range",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="avg_score",
                    description="impossibly tight range",
                    expression="avg(score) between 99 and 100",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is False

    def test_duplicate_count_clean(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="dup_check",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="unique_ids",
                    description="no duplicate ids",
                    expression="duplicate_count(id) = 0",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_value == 0.0

    def test_min_max(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="bounds",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="min_score",
                    description="min >= 0",
                    expression="min(score) >= 0",
                    severity="error",
                ),
                QualityCheck(
                    name="max_score",
                    description="max <= 100",
                    expression="max(score) <= 100",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert all(r.passed for r in result.results)

    def test_sum_metric(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="soda_sum",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="total_score",
                    description="sum > 0",
                    expression="sum(score) > 0",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_name == "sum"
        assert result.results[0].metric_value is not None
        assert result.results[0].metric_value > 0

    def test_stddev_metric(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="soda_stddev",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="score_spread",
                    description="stddev < 100",
                    expression="stddev(score) < 100",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_name == "stddev"

    def test_duplicate_percent_metric(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="soda_dup_pct",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="low_dupes",
                    description="< 50% dupes",
                    expression="duplicate_percent(category) < 50",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_name == "duplicate_percent"

    def test_neq_operator(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="soda_neq",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="has_some",
                    description="not zero rows",
                    expression="row_count != 0",
                    severity="error",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True

    def test_invalid_expression_raises(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="bad_expr",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="bad",
                    description="garbage",
                    expression="not a valid expression",
                    severity="error",
                ),
            ],
        )
        with pytest.raises(ValueError, match="cannot parse"):
            runner.run(suite, "test_data")

    def test_unsupported_metric_raises(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="bad_metric",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="bad",
                    description="unknown metric",
                    expression="foobar(x) = 0",
                    severity="error",
                ),
            ],
        )
        with pytest.raises(ValueError, match="unsupported Soda metric"):
            runner.run(suite, "test_data")

    def test_missing_column_raises(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="bad_col",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="bad",
                    description="no column",
                    expression="missing_count > 0",
                    severity="error",
                ),
            ],
        )
        with pytest.raises(ValueError, match="requires a column"):
            runner.run(suite, "test_data")

    def test_warning_does_not_fail_suite(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="soft",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="nulls_warn",
                    description="warn on nulls",
                    expression="missing_count(name) = 0",
                    severity="warning",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].passed is False

    def test_mixed_soda_checks(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="mixed",
            dialect="soda",
            checks=[
                QualityCheck(
                    name="has_rows",
                    description="not empty",
                    expression="row_count > 0",
                    severity="error",
                ),
                QualityCheck(
                    name="no_null_ids",
                    description="id not null",
                    expression="missing_count(id) = 0",
                    severity="error",
                ),
                QualityCheck(
                    name="score_avg",
                    description="avg in range",
                    expression="avg(score) between 0 and 100",
                    severity="warning",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert len(result.results) == 3


# -------------------------------------------------------- OpenMetadata dialect


class TestOpenMetadataDialect:
    def test_not_null_passes(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_null",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="id_not_null",
                    description="id never null",
                    expression="columnValuesToBeNotNull",
                    severity="error",
                    column="id",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_value == 0.0

    def test_not_null_fails(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_null_fail",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="name_not_null",
                    description="name never null",
                    expression="columnValuesToBeNotNull",
                    severity="error",
                    column="name",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is False
        assert result.results[0].failure_rows == 1

    def test_unique_passes(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_unique",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="id_unique",
                    description="ids unique",
                    expression="columnValuesToBeUnique",
                    severity="error",
                    column="id",
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True

    def test_values_between_passes(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_range",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="score_range",
                    description="score 0-100",
                    expression="columnValuesToBeBetween",
                    severity="error",
                    column="score",
                    threshold=CheckThreshold(min_value=0, max_value=100),
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_value == 0.0

    def test_values_between_fails(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_range_fail",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="score_tight",
                    description="score 90-100",
                    expression="columnValuesToBeBetween",
                    severity="error",
                    column="score",
                    threshold=CheckThreshold(min_value=90, max_value=100),
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is False
        assert result.results[0].failure_rows > 0

    def test_row_count_equal(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_count",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="exact_5",
                    description="must have 5 rows",
                    expression="tableRowCountToEqual",
                    severity="error",
                    threshold=CheckThreshold(value=5),
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_value == 5.0

    def test_row_count_between(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_count_range",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="reasonable_count",
                    description="1-100 rows",
                    expression="tableRowCountToBeBetween",
                    severity="error",
                    threshold=CheckThreshold(min_value=1, max_value=100),
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True

    def test_column_value_lengths_between(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_lengths",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="name_length",
                    description="name 1-50 chars",
                    expression="columnValueLengthsToBeBetween",
                    severity="error",
                    column="name",
                    threshold=CheckThreshold(min_value=1, max_value=50),
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.results[0].metric_name == "length_out_of_range_count"

    def test_column_values_sum_between(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_sum",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="score_sum",
                    description="total in range",
                    expression="columnValuesSumToBeBetween",
                    severity="error",
                    column="score",
                    threshold=CheckThreshold(min_value=100, max_value=1000),
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert result.results[0].metric_name == "sum"

    def test_unsupported_type_raises(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="bad_type",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="bad",
                    description="unknown",
                    expression="columnValuesToBeAwesome",
                    severity="error",
                    column="x",
                ),
            ],
        )
        with pytest.raises(ValueError, match="unsupported OpenMetadata"):
            runner.run(suite, "test_data")

    def test_missing_threshold_raises(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="bad_thresh",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="bad",
                    description="no threshold",
                    expression="tableRowCountToEqual",
                    severity="error",
                ),
            ],
        )
        with pytest.raises(ValueError, match="threshold required"):
            runner.run(suite, "test_data")

    def test_missing_threshold_field_raises(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="bad_field",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="bad",
                    description="incomplete threshold",
                    expression="columnValuesToBeBetween",
                    severity="error",
                    column="x",
                    threshold=CheckThreshold(min_value=0),
                ),
            ],
        )
        with pytest.raises(ValueError, match=r"threshold\.max_value required"):
            runner.run(suite, "test_data")

    def test_mixed_om_checks(
        self,
        quality_runner: DuckDBSQLRunner,
    ) -> None:
        runner = quality_runner
        suite = QualitySuite(
            name="om_mixed",
            dialect="openmetadata",
            checks=[
                QualityCheck(
                    name="id_not_null",
                    description="id never null",
                    expression="columnValuesToBeNotNull",
                    severity="error",
                    column="id",
                ),
                QualityCheck(
                    name="id_unique",
                    description="ids unique",
                    expression="columnValuesToBeUnique",
                    severity="error",
                    column="id",
                ),
                QualityCheck(
                    name="count_check",
                    description="5 rows",
                    expression="tableRowCountToEqual",
                    severity="error",
                    threshold=CheckThreshold(value=5),
                ),
            ],
        )
        result = runner.run(suite, "test_data")
        assert result.passed is True
        assert len(result.results) == 3
        assert all(r.passed for r in result.results)
