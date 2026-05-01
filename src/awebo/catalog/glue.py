"""AWS Glue + S3 catalog backend.

PyIceberg's ``GlueCatalog`` is used directly for write and DDL paths. For
reads, awebo issues DuckDB iceberg ATTACH against the same Glue endpoint so
``SELECT`` queries go through DuckDB's vectorised iceberg scan.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from awebo.catalog.base import CatalogBackend
from awebo.exceptions import CatalogError

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials
    from awebo.config import CatalogSettings


class GlueCatalogBackend(CatalogBackend):
    type = "glue"

    def __init__(self, settings: CatalogSettings) -> None:
        if settings.warehouse is None:
            raise CatalogError("glue catalog requires a warehouse S3 URI")
        self._settings = settings
        self.name = settings.name
        self._pyiceberg: PyIcebergCatalog | None = None

    def pyiceberg(self, credentials: Credentials) -> PyIcebergCatalog:
        if self._pyiceberg is not None:
            return self._pyiceberg

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

    def duckdb_attach_sql(self, credentials: Credentials) -> list[str]:
        secret_name = f"awebo_{self.name}_s3"
        # Build a single CREATE SECRET + single ATTACH; intentionally batched.
        secret_parts = ["TYPE s3"]
        if credentials.region:
            secret_parts.append(f"REGION '{credentials.region}'")
        if credentials.access_key_id:
            secret_parts.append(f"KEY_ID '{credentials.access_key_id}'")
        if credentials.secret_access_key:
            secret_parts.append(f"SECRET '{credentials.secret_access_key}'")
        if credentials.session_token:
            secret_parts.append(f"SESSION_TOKEN '{credentials.session_token}'")

        secret_sql = f"CREATE OR REPLACE SECRET {secret_name} ({', '.join(secret_parts)});"

        attach_props = ["TYPE iceberg", "ENDPOINT_TYPE 'glue'"]
        attach_sql = (
            f"ATTACH '{self._settings.warehouse}' AS {self.name} ({', '.join(attach_props)});"
        )

        return [secret_sql, attach_sql]

    def ensure_namespace(self, credentials: Credentials, namespace: str) -> None:
        catalog = self.pyiceberg(credentials)
        existing = {".".join(ns) for ns in catalog.list_namespaces()}
        if namespace in existing:
            return
        try:
            catalog.create_namespace(namespace)
        except Exception as exc:
            raise CatalogError(f"failed to create namespace {namespace!r}: {exc}") from exc
