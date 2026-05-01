"""Iceberg catalog backends.

Each backend implements :class:`CatalogBackend`, which exposes the minimum
surface awebo needs: a PyIceberg catalog handle (for writes / DDL) plus the
DuckDB ATTACH SQL needed so reads route through the iceberg extension.
"""

from awebo.catalog.base import CatalogBackend
from awebo.catalog.glue import GlueCatalogBackend

__all__ = ["CatalogBackend", "GlueCatalogBackend"]
