"""Cloudflare R2 Iceberg catalog backend.

Derives S3 endpoint and REST catalog URI from a Cloudflare account ID,
then delegates to :class:`~awebo.catalog.rest.RestCatalogBackend`: DuckDB
attaches the R2 Data Catalog with a ``TYPE iceberg`` token secret, so
catalog-qualified reads and writes both work. All operations go through
:class:`~awebo.catalog.rest.RestCatalogBackend`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo.catalog.base import CatalogBackend
from awebo.catalog.rest import RestCatalogBackend

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials
    from awebo.catalog.base import AttachSpec
    from awebo.config import CatalogSettings, CloudflareSettings, RestSettings

log = logging.getLogger(__name__)


def _build_r2_endpoint(account_id: str, jurisdiction: str | None) -> str:
    jur = f".{jurisdiction}" if jurisdiction else ""
    return f"https://{account_id}{jur}.r2.cloudflarestorage.com"


class CloudflareCatalogBackend(CatalogBackend):
    """Cloudflare R2 — config translation + delegation to REST backend."""

    type = "cloudflare"

    def __init__(
        self,
        catalog_settings: CatalogSettings,
        cf: CloudflareSettings,
    ) -> None:
        self.name = catalog_settings.name
        self._r2_endpoint = _build_r2_endpoint(cf.account_id, cf.jurisdiction)
        catalog_default = f"https://catalog.cloudflarestorage.com/{cf.account_id}/{cf.bucket}"
        self._rest_uri = cf.catalog_endpoint or catalog_default

        from awebo.config import RestSettings

        wh = f"{cf.account_id}_{cf.bucket}"
        rest: RestSettings = RestSettings(
            uri=self._rest_uri,
            warehouse=wh,
            token=cf.api_token,
        )
        self._inner = RestCatalogBackend(catalog_settings, rest)

        log.info(
            "cloudflare R2 backend: endpoint=%s catalog=%s",
            self._r2_endpoint,
            self._rest_uri,
        )

    def pyiceberg(self, credentials: Credentials) -> PyIcebergCatalog:
        return self._inner.pyiceberg(credentials)

    def duckdb_attach_sql(self, credentials: Credentials) -> AttachSpec:
        return self._inner.duckdb_attach_sql(credentials)

    def table_location(self, namespace: str, table: str) -> str | None:
        return self._inner.table_location(namespace, table)
