"""Abstract catalog contract.

A catalog backend is the bridge between the user-facing Lakehouse and the two
engines awebo orchestrates:

* **PyIceberg** for writes, table/namespace creation, schema evolution.
* **DuckDB iceberg extension** for reads via SQL.

Both engines need slightly different views of the same connection details, so
the backend exposes them as two methods rather than leaking implementation.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from awebo.exceptions import CatalogError

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials

log = logging.getLogger(__name__)


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
        """Return a connected PyIceberg catalog for write/DDL ops.

        Implementations MUST cache the resulting handle so repeated calls in
        a single Lakehouse session reuse the same connection.
        """

    @abstractmethod
    def duckdb_attach_sql(self, credentials: Credentials) -> AttachSpec:
        """Return DuckDB secret + attach statements for catalog setup.

        ``secrets`` are executed first (CREATE SECRET), then ``attach``
        (ATTACH ... TYPE iceberg). REST backends return ``attach=None``
        because DuckDB iceberg 1.5.x does not support REST ATTACH.
        """

    def ensure_namespace(self, credentials: Credentials, namespace: str) -> None:
        """Create the namespace (database) if it does not exist."""
        catalog = self.pyiceberg(credentials)
        existing = {".".join(ns) for ns in catalog.list_namespaces()}
        if namespace in existing:
            return
        log.debug("creating namespace %r", namespace)
        try:
            catalog.create_namespace(namespace)
        except Exception as exc:
            raise CatalogError(f"failed to create namespace {namespace!r}: {exc}") from exc
