"""Quality check runner implementations."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Protocol

from awebo.quality.types import CheckResult, QualityResult, QualitySuite

if TYPE_CHECKING:
    from awebo.lineage.emitter import LineageEmitter
    from awebo.quality.compiler import CheckCompiler
    from awebo.quality.types import CheckDialect, CompiledCheck
    from awebo.query.engine import QueryEngine

log = logging.getLogger(__name__)


def _default_compiler(dialect: CheckDialect) -> CheckCompiler:
    from awebo.quality.compiler import OpenMetadataCompiler, SodaCompiler, SQLCompiler

    compilers: dict[CheckDialect, CheckCompiler] = {
        "sql": SQLCompiler(),
        "soda": SodaCompiler(),
        "openmetadata": OpenMetadataCompiler(),
    }
    return compilers[dialect]


class QualityRunner(Protocol):
    def run(self, suite: QualitySuite, table: str) -> QualityResult: ...


class DuckDBSQLRunner:
    def __init__(
        self,
        engine: QueryEngine,
        emitter: LineageEmitter,
        lineage_namespace: str = "awebo",
    ) -> None:
        self._engine = engine
        self._emitter = emitter
        self._lineage_namespace = lineage_namespace
        self._compiler_cache: dict[CheckDialect, CheckCompiler] = {}

    def _get_compiler(self, dialect: CheckDialect) -> CheckCompiler:
        if dialect not in self._compiler_cache:
            self._compiler_cache[dialect] = _default_compiler(dialect)
        return self._compiler_cache[dialect]

    def run(self, suite: QualitySuite, table: str) -> QualityResult:
        compiler = self._get_compiler(suite.dialect)
        results: list[CheckResult] = []

        for check in suite.checks:
            compiled = compiler.compile(check, table)
            t0 = time.perf_counter()
            try:
                rows = self._engine.fetch_rows(compiled.metric_sql)
                metric_value = float(str(rows[0][0]))
            except Exception:
                log.warning("quality check %r SQL failed", check.name, exc_info=True)
                metric_value = -1.0

            duration_ms = (time.perf_counter() - t0) * 1000
            passed = self._evaluate(compiled, metric_value)

            is_violation_count = compiled.operator == "eq" and compiled.threshold == 0.0
            failure_rows = int(metric_value) if is_violation_count and metric_value >= 0 else 0

            results.append(
                CheckResult(
                    check=check,
                    passed=passed,
                    failure_rows=max(failure_rows, 0),
                    duration_ms=duration_ms,
                    metric_value=metric_value if metric_value >= 0 else None,
                    metric_name=compiled.metric_name,
                ),
            )

        suite_passed = all(r.passed or r.check.severity == "warning" for r in results)

        quality_result = QualityResult(
            suite=suite.name,
            results=tuple(results),
            passed=suite_passed,
        )

        self._emit_quality_facet(suite, table, quality_result)
        return quality_result

    @staticmethod
    def _evaluate(compiled: CompiledCheck, value: float) -> bool:
        t = compiled.threshold
        if compiled.operator == "between":
            hi = compiled.threshold_max if compiled.threshold_max is not None else t
            return t <= value <= hi
        simple: dict[str, bool] = {
            "eq": value == t,
            "neq": value != t,
            "lt": value < t,
            "lte": value <= t,
            "gt": value > t,
            "gte": value >= t,
        }
        return simple[compiled.operator]

    def _emit_quality_facet(
        self,
        suite: QualitySuite,
        table: str,
        result: QualityResult,
    ) -> None:
        try:
            from openlineage.client.generated.data_quality_assertions_dataset import (
                Assertion,
                DataQualityAssertionsDatasetFacet,
            )
            from openlineage.client.run import (
                InputDataset,
                Job,
                Run,
                RunEvent,
                RunState,
            )
            from openlineage.client.uuid import generate_new_uuid

            from awebo.lineage.events import PRODUCER, _now_iso

            assertions = [
                Assertion(
                    assertion=r.check.name,
                    success=r.passed,
                    column=None,
                    severity=r.check.severity,
                )
                for r in result.results
            ]

            facet = DataQualityAssertionsDatasetFacet(assertions=assertions)

            event = RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=_now_iso(),
                run=Run(runId=str(generate_new_uuid())),
                job=Job(
                    namespace=self._lineage_namespace,
                    name=f"quality_{suite.name}",
                ),
                producer=PRODUCER,
                inputs=[
                    InputDataset(
                        namespace=self._lineage_namespace,
                        name=table,
                        inputFacets={"dataQualityAssertions": facet},
                    )
                ],
            )
            self._emitter.emit(event)
        except Exception:
            log.warning("quality lineage emission failed", exc_info=True)
