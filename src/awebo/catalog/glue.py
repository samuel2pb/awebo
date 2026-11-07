"""AWS Glue + S3 catalog backend.

awebo attaches Glue's Iceberg REST endpoint to DuckDB
(``ENDPOINT_TYPE 'glue'``, SigV4-signed with the resolved AWS credentials),
so both reads and writes run through DuckDB's iceberg extension. PyIceberg's
``GlueCatalog`` remains available as the ``pyiceberg_catalog()`` escape hatch.

DuckDB's ``ENDPOINT_TYPE 'glue'`` always builds ``glue.<region>.amazonaws.com``;
China regions (``cn-*``) live under ``amazonaws.com.cn``, so for those awebo
passes the explicit ``URI`` + ``AUTHORIZATION_TYPE 'sigv4'`` form instead.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo._sql import quote_ident, quote_literal
from awebo.catalog.base import AttachSpec, CatalogBackend
from awebo.exceptions import CatalogError

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials
    from awebo.config import CatalogSettings

log = logging.getLogger(__name__)


class GlueCatalogBackend(CatalogBackend):
    type = "glue"

    def __init__(self, settings: CatalogSettings) -> None:
        self._settings = settings
        self.name = settings.name
        self._pyiceberg: PyIcebergCatalog | None = None

    def pyiceberg(self, credentials: Credentials) -> PyIcebergCatalog:
        if self._pyiceberg is not None:
            return self._pyiceberg

        log.debug("connecting to Glue catalog %r", self.name)

        from pyiceberg.catalog import load_catalog

        props: dict[str, str] = {
            "type": "glue",
            "warehouse": self._settings.warehouse or "",
        }
        if credentials.region:
            props["glue.region"] = credentials.region
            props["s3.region"] = credentials.region
        if credentials.access_key_id:
            props["glue.access-key-id"] = credentials.access_key_id
            props["s3.access-key-id"] = credentials.access_key_id
        if credentials.secret_access_key:
            props["glue.secret-access-key"] = credentials.secret_access_key
            props["s3.secret-access-key"] = credentials.secret_access_key
        if credentials.session_token:
            props["glue.session-token"] = credentials.session_token
            props["s3.session-token"] = credentials.session_token

        try:
            self._pyiceberg = load_catalog(self.name, **props)
        except Exception as exc:  # pyiceberg raises bare exceptions on connect
            raise CatalogError(f"failed to load Glue catalog {self.name!r}: {exc}") from exc
        return self._pyiceberg

    def duckdb_attach_sql(self, credentials: Credentials) -> AttachSpec:
        secret_name = quote_ident(f"awebo_{self.name}_s3")
        secret_parts = ["TYPE s3"]
        if credentials.region:
            secret_parts.append(f"REGION {quote_literal(credentials.region)}")
        if credentials.access_key_id:
            secret_parts.append(f"KEY_ID {quote_literal(credentials.access_key_id)}")
        if credentials.secret_access_key:
            secret_parts.append(f"SECRET {quote_literal(credentials.secret_access_key)}")
        if credentials.session_token:
            secret_parts.append(f"SESSION_TOKEN {quote_literal(credentials.session_token)}")

        secret_sql = f"CREATE OR REPLACE SECRET {secret_name} ({', '.join(secret_parts)});"

        glue_path = self._settings.properties.get("glue_catalog_id", ":")
        region = credentials.region or ""
        if region.startswith("cn-"):
            uri = f"glue.{region}.amazonaws.com.cn/iceberg"
            attach_props = [
                "TYPE iceberg",
                f"URI {quote_literal(uri)}",
                "AUTHORIZATION_TYPE 'sigv4'",
            ]
        else:
            attach_props = ["TYPE iceberg", "ENDPOINT_TYPE 'glue'"]
        attach_sql = (
            f"ATTACH {quote_literal(glue_path)} AS {quote_ident(self.name)} "
            f"({', '.join(attach_props)});"
        )

        return AttachSpec(secrets=[secret_sql], attach=attach_sql)

    def table_location(self, namespace: str, table: str) -> str | None:
        """Glue does not assign locations: mirror PyIceberg's ``<wh>/<ns>.db/<tbl>``."""
        warehouse = self._settings.warehouse
        if not warehouse:
            return None
        return f"{warehouse.rstrip('/')}/{namespace}.db/{table}"
