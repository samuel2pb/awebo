"""Test that IcebergWriter emits lineage events at write boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest

from awebo.exceptions import WriteError
from awebo.io.writer import IcebergWriter
from awebo.query.engine import QueryEngine
from tests._fixtures.auth import NoopAuthProvider
from tests._fixtures.catalog import LocalIcebergCatalogBackend

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from openlineage.client.run import RunEvent


class SpyEmitter:
    """Records emitted events for assertion."""

    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)


@pytest.fixture
def writer_env(tmp_path: Path) -> Iterator[tuple[IcebergWriter, SpyEmitter]]:
    warehouse = tmp_path / "warehouse"
    catalog = LocalIcebergCatalogBackend("test", warehouse)
    auth = NoopAuthProvider()
    credentials = auth.resolve()

    engine = QueryEngine(temp_dir=str(tmp_path / "duckdb_tmp"))
    engine.attach_catalog(catalog, credentials)

    spy = SpyEmitter()
    writer = IcebergWriter(
        engine=engine,
        catalog=catalog,
        credentials=credentials,
        streaming_threshold_mb=1,
        emitter=spy,
        lineage_namespace="test-ns",
    )
    try:
        yield writer, spy
    finally:
        engine.close()


def test_append_emits_start_and_complete(
    writer_env: tuple[IcebergWriter, SpyEmitter],
) -> None:
    writer, spy = writer_env
    df = pl.DataFrame({"x": [1, 2, 3]})
    writer.write(df, "db", "t")

    assert len(spy.events) == 2
    assert spy.events[0].eventType.name == "START"
    assert spy.events[1].eventType.name == "COMPLETE"


def test_failed_write_emits_start_and_fail(
    writer_env: tuple[IcebergWriter, SpyEmitter],
) -> None:
    writer, spy = writer_env
    with pytest.raises(WriteError, match="failed"):
        writer.write("SELECT * FROM nonexistent_table_xyz", "db", "bad")

    assert len(spy.events) == 2
    assert spy.events[0].eventType.name == "START"
    assert spy.events[1].eventType.name == "FAIL"


def test_events_contain_output_dataset(
    writer_env: tuple[IcebergWriter, SpyEmitter],
) -> None:
    writer, spy = writer_env
    df = pl.DataFrame({"x": [1]})
    writer.write(df, "mydb", "mytable")

    complete = spy.events[1]
    assert complete.outputs is not None
    assert len(complete.outputs) == 1
    assert complete.outputs[0].name == "mydb.mytable"
    assert complete.outputs[0].namespace == "test-ns"


def test_events_share_same_run_id(
    writer_env: tuple[IcebergWriter, SpyEmitter],
) -> None:
    writer, spy = writer_env
    df = pl.DataFrame({"x": [1]})
    writer.write(df, "db", "t")

    assert spy.events[0].run.runId == spy.events[1].run.runId


def test_emission_failure_does_not_break_write(tmp_path: Path) -> None:
    """Even if emitter raises, the write must succeed."""
    warehouse = tmp_path / "warehouse"
    catalog = LocalIcebergCatalogBackend("test", warehouse)
    auth = NoopAuthProvider()
    credentials = auth.resolve()

    engine = QueryEngine(temp_dir=str(tmp_path / "duckdb_tmp"))
    engine.attach_catalog(catalog, credentials)

    class BrokenEmitter:
        def emit(self, event: RunEvent) -> None:
            raise RuntimeError("emitter is broken")

    writer = IcebergWriter(
        engine=engine,
        catalog=catalog,
        credentials=credentials,
        streaming_threshold_mb=1,
        emitter=BrokenEmitter(),
        lineage_namespace="test-ns",
    )
    try:
        df = pl.DataFrame({"x": [1, 2]})
        writer.write(df, "db", "t")

        tbl = catalog.pyiceberg(credentials).load_table("db.t")
        assert tbl.scan().to_arrow().num_rows == 2
    finally:
        engine.close()
