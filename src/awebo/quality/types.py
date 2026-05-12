"""Core data quality types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Sequence

type CheckDialect = Literal["sql", "soda", "openmetadata"]

type ComparisonOp = Literal["eq", "neq", "lt", "lte", "gt", "gte", "between"]


@dataclass(frozen=True, slots=True)
class CheckThreshold:
    """Parameters for threshold-based quality checks (OpenMetadata dialect)."""

    min_value: float | None = None
    max_value: float | None = None
    value: float | None = None


@dataclass(frozen=True, slots=True)
class QualityCheck:
    name: str
    description: str
    expression: str
    severity: Literal["warning", "error"]
    column: str | None = None
    threshold: CheckThreshold | None = None


@dataclass(frozen=True, slots=True)
class QualitySuite:
    name: str
    checks: tuple[QualityCheck, ...]
    dialect: CheckDialect = "sql"

    def __init__(
        self,
        name: str,
        checks: Sequence[QualityCheck],
        dialect: CheckDialect = "sql",
    ) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "checks", tuple(checks))
        object.__setattr__(self, "dialect", dialect)


@dataclass(frozen=True, slots=True)
class CompiledCheck:
    """Compiler output consumed by the runner."""

    metric_sql: str
    metric_name: str
    operator: ComparisonOp
    threshold: float
    threshold_max: float | None = None


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: QualityCheck
    passed: bool
    failure_rows: int
    duration_ms: float
    metric_value: float | None = None
    metric_name: str | None = None


@dataclass(frozen=True, slots=True)
class QualityResult:
    suite: str
    results: tuple[CheckResult, ...]
    passed: bool
