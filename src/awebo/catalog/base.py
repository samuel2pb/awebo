"""Abstract catalog contract.

A catalog backend tells DuckDB how to reach one Iceberg catalog. Since
DuckDB 2.0 the iceberg extension reads *and* writes through an attached
catalog, so :meth:`CatalogBackend.duckdb_attach_sql` is the whole data path.

:meth:`CatalogBackend.pyiceberg` remains as the escape hatch behind
``Lakehouse.pyiceberg_catalog()`` (snapshot expiry, manifest inspection and
other maintenance that has no SQL verb). awebo itself never calls it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials


@dataclass(frozen=True, slots=True)
class AttachSpec:
    """Typed return from :meth:`CatalogBackend.duckdb_attach_sql`.

    Separates DuckDB secret creation from catalog ATTACH so callers
    handle each phase explicitly rather than iterating opaque SQL strings.
    """

    secrets: list[str] = field(default_factory=list)
    attach: str | None = None


class CatalogBackend(ABC):
    """Abstracts an Iceberg catalog (Glue, REST, Hive, Nessie, ...)."""

    type: str
    name: str

    @abstractmethod
    def pyiceberg(self, credentials: Credentials) -> PyIcebergCatalog:
        """Return a connected PyIceberg catalog (escape hatch only).

        Implementations MUST cache the resulting handle so repeated calls in
        a single Lakehouse session reuse the same connection.
        """

    @abstractmethod
    def duckdb_attach_sql(self, credentials: Credentials) -> AttachSpec:
        """Return DuckDB secret + attach statements for catalog setup.

        ``secrets`` are executed first (``CREATE SECRET``), then ``attach``
        (``ATTACH ... (TYPE iceberg, ...)``). The attached catalog carries
        both reads and writes.
        """

    def table_location(self, namespace: str, table: str) -> str | None:
        """Return the storage location for a new table, or ``None``.

        ``None`` lets the catalog pick the location (REST catalogs do).
        Catalogs that cannot (Glue without S3 Tables federation) override it.
        """
        del namespace, table
        return None
