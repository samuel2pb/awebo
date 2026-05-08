"""Pluggable data quality testing."""

from awebo.quality.runner import DuckDBSQLRunner, QualityRunner
from awebo.quality.types import CheckResult, QualityCheck, QualityResult, QualitySuite

__all__ = [
    "CheckResult",
    "DuckDBSQLRunner",
    "QualityCheck",
    "QualityResult",
    "QualityRunner",
    "QualitySuite",
]
