"""Test that DuckDBSQLRunner emits OL quality facet, and emission failures don't break runs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest

from awebo.quality.runner import DuckDBSQLRunner
from awebo.quality.types import QualityCheck, QualitySuite
from awebo.query.engine import QueryEngine

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from openlineage.client.run import RunEvent


class SpyEmitter:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)


class BrokenEmitter:
    def emit(self, event: RunEvent) -> None:
        raise RuntimeError("broken emitter")


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[QueryEngine]:
    eng = QueryEngine(temp_dir=str(tmp_path / "duckdb_tmp"))
    arrow_tbl = pl.DataFrame({"id": [1, 2], "val": [10, 20]}).to_arrow()
    eng.register_arrow("data", arrow_tbl)
    try:
        yield eng
    finally:
        eng.close()


def _suite() -> QualitySuite:
    return QualitySuite(
        name="test_suite",
        checks=[
            QualityCheck(
                name="ids_positive",
                description="IDs positive",
                query="SELECT * FROM {table} WHERE id <= 0",
                severity="error",
            ),
        ],
    )


def test_emits_quality_assertions_facet(engine: QueryEngine) -> None:
    spy = SpyEmitter()
    runner = DuckDBSQLRunner(engine=engine, emitter=spy, lineage_namespace="test-ns")
    runner.run(_suite(), "data")

    assert len(spy.events) == 1
    event = spy.events[0]
    assert event.eventType.name == "COMPLETE"
    assert event.inputs is not None
    assert len(event.inputs) == 1
    assert event.inputs[0].name == "data"
    assert event.inputs[0].namespace == "test-ns"

    facets = event.inputs[0].inputFacets  # type: ignore[attr-defined]
    assert "dataQualityAssertions" in facets
    facet = facets["dataQualityAssertions"]
    assert len(facet.assertions) == 1
    assert facet.assertions[0].assertion == "ids_positive"
    assert facet.assertions[0].success is True


def test_emission_failure_does_not_break_quality_run(engine: QueryEngine) -> None:
    runner = DuckDBSQLRunner(engine=engine, emitter=BrokenEmitter(), lineage_namespace="ns")
    result = runner.run(_suite(), "data")

    assert result.passed is True
    assert len(result.results) == 1
