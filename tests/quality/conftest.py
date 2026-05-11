"""Shared fixtures for quality tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest

from awebo.lineage.emitter import NoopEmitter
from awebo.quality.runner import DuckDBSQLRunner
from awebo.query.engine import QueryEngine

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture
def quality_engine(tmp_path: Path) -> Iterator[QueryEngine]:
    """DuckDB engine with a 5-row test table registered as ``test_data``."""
    engine = QueryEngine(temp_dir=str(tmp_path / "duckdb_tmp"))
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", None, "dave", "eve"],
            "score": [85.0, 92.0, 78.0, 95.0, 88.0],
            "category": ["a", "b", "a", "c", "b"],
        },
    )
    engine.register_arrow("test_data", df.to_arrow())
    try:
        yield engine
    finally:
        engine.close()


@pytest.fixture
def quality_runner(quality_engine: QueryEngine) -> DuckDBSQLRunner:
    return DuckDBSQLRunner(engine=quality_engine, emitter=NoopEmitter())
