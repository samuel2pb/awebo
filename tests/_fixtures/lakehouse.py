"""Factory for an offline Lakehouse used by pipeline tests.

Wires a :class:`LocalIcebergCatalogBackend` + :class:`NoopAuthProvider` into
:class:`awebo.Lakehouse` via the public injection seam so tests exercise the
exact same code paths as production.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from awebo import Lakehouse
from awebo.config import CatalogSettings, LakehouseConfig
from tests._fixtures.auth import NoopAuthProvider
from tests._fixtures.catalog import LocalIcebergCatalogBackend

if TYPE_CHECKING:
    from pathlib import Path

    from awebo.lineage.emitter import LineageEmitter


def make_test_lakehouse(
    tmp_path: Path,
    *,
    emitter: LineageEmitter | None = None,
    memory_limit: str | None = None,
) -> Lakehouse:
    """Build an offline Lakehouse rooted at ``tmp_path``.

    ``memory_limit`` caps DuckDB so tests can force spill-to-disk writes.
    """
    warehouse = tmp_path / "warehouse"
    config = LakehouseConfig(
        catalog=CatalogSettings(
            type="local",
            name="test_lake",
            warehouse=str(warehouse),
        ),
        aws=None,
        temp_dir=str(tmp_path / "duckdb_tmp"),
        memory_limit=memory_limit,
    )
    catalog = LocalIcebergCatalogBackend("test_lake", warehouse)
    auth = NoopAuthProvider()
    return Lakehouse(config, catalog=catalog, auth=auth, emitter=emitter)
