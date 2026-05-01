"""Abstract catalog contract.

A catalog backend is the bridge between the user-facing Lakehouse and the two
engines awebo orchestrates:

* **PyIceberg** for writes, table/namespace creation, schema evolution.
* **DuckDB iceberg extension** for reads via SQL.

Both engines need slightly different views of the same connection details, so
the backend exposes them as two methods rather than leaking implementation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials


class CatalogBackend(ABC):
    """Abstracts an Iceberg catalog (Glue, REST, Hive, Nessie, ...)."""

    type: str
    name: str

    @abstractmethod
    def pyiceberg(self, credentials: Credentials) -> PyIcebergCatalog:
        """Return a connected PyIceberg catalog for write/DDL ops.

        Implementations MUST cache the resulting handle so repeated calls in
        a single Lakehouse session reuse the same connection.
        """

    @abstractmethod
    def duckdb_attach_sql(self, credentials: Credentials) -> list[str]:
        """Return the DuckDB SQL statements needed before ATTACH.

        Typically: secret creation + ``ATTACH '<warehouse>' AS <name>
        (TYPE iceberg, ...);``. Returned as a list so the caller can run them
        in one ``executemany``-style batch (no D+1).
        """

    @abstractmethod
    def ensure_namespace(self, credentials: Credentials, namespace: str) -> None:
        """Create the namespace (database) if it does not exist."""
