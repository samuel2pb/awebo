"""Quality check runner implementations."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Protocol

from awebo.quality.types import CheckResult, QualityResult, QualitySuite

if TYPE_CHECKING:
    from awebo.lineage.emitter import LineageEmitter
    from awebo.query.engine import QueryEngine

log = logging.getLogger(__name__)


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

    def run(self, suite: QualitySuite, table: str) -> QualityResult:
        results: list[CheckResult] = []

        for check in suite.checks:
            t0 = time.perf_counter()
            sql = check.query.format(table=table)
            try:
                relation = self._engine.execute(sql)
                row_count = relation.arrow().read_all().num_rows
            except Exception:
                log.warning("quality check %r SQL failed", check.name, exc_info=True)
                row_count = -1

            duration_ms = (time.perf_counter() - t0) * 1000
            passed = row_count == 0
            results.append(
                CheckResult(
                    check=check,
                    passed=passed,
                    failure_rows=max(row_count, 0),
                    duration_ms=duration_ms,
                )
            )

        suite_passed = all(r.passed or r.check.severity == "warning" for r in results)

        quality_result = QualityResult(
            suite=suite.name,
            results=tuple(results),
            passed=suite_passed,
        )

        self._emit_quality_facet(suite, table, quality_result)
        return quality_result

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
