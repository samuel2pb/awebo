"""Tests for LakehouseBuilder."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from awebo.config import CatalogSettings, LakehouseConfig
from awebo.lakehouse import LakehouseBuilder
from tests._fixtures.auth import NoopAuthProvider
from tests._fixtures.catalog import LocalIcebergCatalogBackend

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from awebo import Lakehouse


@pytest.fixture
def builder_lake(tmp_path: Path) -> Iterator[Lakehouse]:
    warehouse = tmp_path / "warehouse"
    config = LakehouseConfig(
        catalog=CatalogSettings(type="local", name="test", warehouse=str(warehouse)),
        streaming_threshold_mb=1,
        temp_dir=str(tmp_path / "duckdb_tmp"),
    )
    lake = (
        LakehouseBuilder(config)
        .with_auth(NoopAuthProvider())
        .with_catalog(LocalIcebergCatalogBackend("test", warehouse))
        .build()
    )
    try:
        yield lake
    finally:
        lake.close()


def test_builder_creates_working_lakehouse(builder_lake: Lakehouse) -> None:
    import polars as pl

    builder_lake.to_lake(pl.DataFrame({"x": [1, 2]}), database="db", table="t")
    result = builder_lake.pyiceberg_catalog().load_table("db.t").scan().to_arrow()
    assert result.num_rows == 2


def test_builder_is_chainable(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    config = LakehouseConfig(
        catalog=CatalogSettings(type="local", name="test", warehouse=str(warehouse)),
    )
    auth = NoopAuthProvider()
    catalog = LocalIcebergCatalogBackend("test", warehouse)
    builder = LakehouseBuilder(config)
    assert builder.with_auth(auth) is builder
    assert builder.with_catalog(catalog) is builder


def test_builder_requires_auth_before_build(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    config = LakehouseConfig(
        catalog=CatalogSettings(type="local", name="test", warehouse=str(warehouse)),
    )
    builder = LakehouseBuilder(config).with_catalog(LocalIcebergCatalogBackend("test", warehouse))
    with pytest.raises(ValueError, match="auth"):
        builder.build()


def test_builder_requires_catalog_before_build(tmp_path: Path) -> None:
    config = LakehouseConfig(
        catalog=CatalogSettings(type="local", name="test", warehouse="x"),
    )
    builder = LakehouseBuilder(config).with_auth(NoopAuthProvider())
    with pytest.raises(ValueError, match="catalog"):
        builder.build()
