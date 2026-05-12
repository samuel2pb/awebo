"""Check compilers — translate dialect-specific expressions to DuckDB SQL."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, ClassVar, Protocol

if TYPE_CHECKING:
    from awebo.quality.types import CheckThreshold, ComparisonOp, CompiledCheck, QualityCheck


class CheckCompiler(Protocol):
    """Translates a QualityCheck into executable SQL + evaluation criteria."""

    def compile(self, check: QualityCheck, table: str) -> CompiledCheck: ...


class SQLCompiler:
    """Raw SQL dialect — expression returns violating rows, 0 = pass."""

    def compile(self, check: QualityCheck, table: str) -> CompiledCheck:
        from awebo.quality.types import CompiledCheck

        sql = check.expression.format(table=table)
        return CompiledCheck(
            metric_sql=f"SELECT COUNT(*) AS v FROM ({sql}) AS _q",
            metric_name=f"{check.name}_violation_count",
            operator="eq",
            threshold=0.0,
        )


class SodaCompiler:
    """Compile SodaCL-style expressions to DuckDB SQL.

    Supported metrics: row_count, missing_count, missing_percent,
    duplicate_count, duplicate_percent, min, max, avg, sum, stddev, freshness.

    Operators: =, !=, <, <=, >, >=, between ... and ...
    Time units for freshness: d (days), h (hours), m (minutes).
    """

    _RE: ClassVar[re.Pattern[str]] = re.compile(
        r"^(\w+)"
        r"(?:\((\w+)\))?"
        r"\s+"
        r"(between|!=|<=|>=|=|<|>)"
        r"\s+"
        r"(-?[\d.]+)"
        r"([dhm])?"
        r"(?:\s+and\s+(-?[\d.]+)([dhm])?)?"
        r"$",
        re.IGNORECASE,
    )

    _METRIC_SQL: ClassVar[dict[str, str]] = {
        "row_count": "SELECT COUNT(*) FROM {table}",
        "missing_count": "SELECT COUNT(*) FROM {table} WHERE {column} IS NULL",
        "missing_percent": (
            "SELECT 100.0 * COUNT(CASE WHEN {column} IS NULL THEN 1 END)"
            " / NULLIF(COUNT(*), 0) FROM {table}"
        ),
        "duplicate_count": (
            "SELECT COALESCE(SUM(cnt - 1), 0) FROM"
            " (SELECT {column}, COUNT(*) AS cnt FROM {table}"
            " GROUP BY {column} HAVING COUNT(*) > 1) AS _d"
        ),
        "duplicate_percent": (
            "SELECT 100.0 * COALESCE(SUM(cnt - 1), 0)"
            " / NULLIF((SELECT COUNT(*) FROM {table}), 0) FROM"
            " (SELECT {column}, COUNT(*) AS cnt FROM {table}"
            " GROUP BY {column} HAVING COUNT(*) > 1) AS _d"
        ),
        "min": "SELECT MIN({column}) FROM {table}",
        "max": "SELECT MAX({column}) FROM {table}",
        "avg": "SELECT AVG({column}) FROM {table}",
        "sum": "SELECT SUM({column}) FROM {table}",
        "stddev": "SELECT STDDEV({column}) FROM {table}",
        "freshness": (
            "SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({column}))) FROM {table}"
        ),
    }

    _OP_MAP: ClassVar[dict[str, ComparisonOp]] = {
        "=": "eq",
        "!=": "neq",
        "<": "lt",
        "<=": "lte",
        ">": "gt",
        ">=": "gte",
        "between": "between",
    }

    _TIME_SECONDS: ClassVar[dict[str, float]] = {
        "d": 86400.0,
        "h": 3600.0,
        "m": 60.0,
    }

    def compile(self, check: QualityCheck, table: str) -> CompiledCheck:
        from awebo.quality.types import CompiledCheck

        m = self._RE.match(check.expression.strip())
        if m is None:
            raise ValueError(f"cannot parse Soda expression: {check.expression!r}")

        metric = m.group(1).lower()
        column = m.group(2) or check.column
        op_str = m.group(3).lower()
        val = float(m.group(4))
        unit = m.group(5)
        val_max_str = m.group(6)
        unit_max = m.group(7)

        if metric not in self._METRIC_SQL:
            raise ValueError(f"unsupported Soda metric: {metric!r}")

        template = self._METRIC_SQL[metric]
        if "{column}" in template and column is None:
            raise ValueError(f"metric {metric!r} requires a column")

        sql = template.format(table=table, column=column or "")

        if unit:
            val *= self._TIME_SECONDS.get(unit, 1.0)
        threshold_max: float | None = None
        if val_max_str is not None:
            threshold_max = float(val_max_str)
            if unit_max:
                threshold_max *= self._TIME_SECONDS.get(unit_max, 1.0)

        return CompiledCheck(
            metric_sql=sql,
            metric_name=metric,
            operator=self._OP_MAP[op_str],
            threshold=val,
            threshold_max=threshold_max,
        )


class OpenMetadataCompiler:
    """Compile OpenMetadata test type definitions to DuckDB SQL.

    Supported: columnValuesToBeNotNull, columnValuesToBeUnique,
    columnValuesToBeBetween, tableRowCountToEqual, tableRowCountToBeBetween,
    columnValueLengthsToBeBetween, columnValuesSumToBeBetween.
    """

    def compile(self, check: QualityCheck, table: str) -> CompiledCheck:
        from awebo.quality.types import CompiledCheck

        test_type = check.expression
        col = check.column or ""
        t = check.threshold

        match test_type:
            case "columnValuesToBeNotNull" | "columnValuesToBeUnique":
                return self._compile_uniqueness(test_type, col, table)
            case "columnValuesToBeBetween":
                vt = _require_threshold(t, "min_value", "max_value")
                return CompiledCheck(
                    metric_sql=(
                        f"SELECT COUNT(*) FROM {table}"
                        f" WHERE {col} NOT BETWEEN {vt.min_value} AND {vt.max_value}"
                    ),
                    metric_name="out_of_range_count",
                    operator="eq",
                    threshold=0.0,
                )
            case "tableRowCountToEqual":
                vt = _require_threshold(t, "value")
                return CompiledCheck(
                    metric_sql=f"SELECT COUNT(*) FROM {table}",
                    metric_name="row_count",
                    operator="eq",
                    threshold=vt.value,  # type: ignore[arg-type]
                )
            case "tableRowCountToBeBetween":
                vt = _require_threshold(t, "min_value", "max_value")
                return CompiledCheck(
                    metric_sql=f"SELECT COUNT(*) FROM {table}",
                    metric_name="row_count",
                    operator="between",
                    threshold=vt.min_value,  # type: ignore[arg-type]
                    threshold_max=vt.max_value,
                )
            case "columnValueLengthsToBeBetween":
                vt = _require_threshold(t, "min_value", "max_value")
                return CompiledCheck(
                    metric_sql=(
                        f"SELECT COUNT(*) FROM {table}"
                        f" WHERE LENGTH(CAST({col} AS VARCHAR))"
                        f" NOT BETWEEN {vt.min_value} AND {vt.max_value}"
                    ),
                    metric_name="length_out_of_range_count",
                    operator="eq",
                    threshold=0.0,
                )
            case "columnValuesSumToBeBetween":
                vt = _require_threshold(t, "min_value", "max_value")
                return CompiledCheck(
                    metric_sql=f"SELECT SUM({col}) FROM {table}",
                    metric_name="sum",
                    operator="between",
                    threshold=vt.min_value,  # type: ignore[arg-type]
                    threshold_max=vt.max_value,
                )
            case _:
                raise ValueError(f"unsupported OpenMetadata test type: {test_type!r}")

    @staticmethod
    def _compile_uniqueness(test_type: str, col: str, table: str) -> CompiledCheck:
        from awebo.quality.types import CompiledCheck

        if test_type == "columnValuesToBeNotNull":
            return CompiledCheck(
                metric_sql=f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL",
                metric_name="missing_count",
                operator="eq",
                threshold=0.0,
            )
        return CompiledCheck(
            metric_sql=(
                f"SELECT COALESCE(SUM(cnt - 1), 0) FROM"
                f" (SELECT {col}, COUNT(*) AS cnt FROM {table}"
                f" GROUP BY {col} HAVING COUNT(*) > 1) AS _d"
            ),
            metric_name="duplicate_count",
            operator="eq",
            threshold=0.0,
        )


def _require_threshold(
    t: CheckThreshold | None,
    *fields: str,
) -> CheckThreshold:
    if t is None:
        raise ValueError(f"threshold required with fields: {', '.join(fields)}")
    for field in fields:
        if getattr(t, field) is None:
            raise ValueError(f"threshold.{field} required")
    return t
