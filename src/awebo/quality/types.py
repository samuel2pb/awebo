"""Core data quality types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class QualityCheck:
    name: str
    description: str
    query: str
    severity: Literal["warning", "error"]


@dataclass(frozen=True, slots=True)
class QualitySuite:
    name: str
    checks: tuple[QualityCheck, ...] | list[QualityCheck]


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: QualityCheck
    passed: bool
    failure_rows: int
    duration_ms: float


@dataclass(frozen=True, slots=True)
class QualityResult:
    suite: str
    results: tuple[CheckResult, ...]
    passed: bool
