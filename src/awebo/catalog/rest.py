"""Iceberg REST catalog backend.

Connects to any REST-compatible Iceberg catalog (Tabular's
``tabulario/iceberg-rest``, Lakekeeper, Nessie, Polaris, ...). Object-store
credentials ride on :class:`Credentials` so the same keys feed both PyIceberg
(for writes / DDL) and the DuckDB iceberg extension (for reads via
``ATTACH ... ENDPOINT_TYPE 'iceberg_rest'``).

The DuckDB ATTACH path emits at most two statements — one secret and one
ATTACH — so credential propagation is batched (no D+1).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo.catalog.base import CatalogBackend
from awebo.exceptions import CatalogError

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    from awebo.auth.base import Credentials
    from awebo.config import CatalogSettings, RestSettings

log = logging.getLogger(__name__)


def _strip_scheme(endpoint: str) -> str:
    """DuckDB's ENDPOINT secret param wants ``host:port``, no scheme."""
    for scheme in ("https://", "http://"):
        if endpoint.startswith(scheme):
            return endpoint[len(scheme) :]
    return endpoint


def _is_https(endpoint: str) -> bool:
    return endpoint.lower().startswith("https://")


class RestCatalogBackend(CatalogBackend):
    type = "rest"

    def __init__(self, catalog_settings: CatalogSettings, rest: RestSettings) -> None:
        self._catalog_settings = catalog_settings
        self._rest = rest
        self.name = catalog_settings.name
        self._pyiceberg: PyIcebergCatalog | None = None

    def pyiceberg(self, credentials: Credentials) -> PyIcebergCatalog:
        if self._pyiceberg is not None:
            return self._pyiceberg

        log.debug("connecting to REST catalog %r at %s", self.name, self._rest.uri)

        from pyiceberg.catalog import load_catalog

        props: dict[str, str] = {
            "type": "rest",
            "uri": self._rest.uri,
            "warehouse": self._rest.warehouse,
        }
        if self._rest.token:
            props["token"] = self._rest.token
        if self._rest.oauth_credential:
            props["credential"] = self._rest.oauth_credential

        if credentials.region:
            props["s3.region"] = credentials.region
        if credentials.access_key_id:
            props["s3.access-key-id"] = credentials.access_key_id
        if credentials.secret_access_key:
            props["s3.secret-access-key"] = credentials.secret_access_key
        if credentials.session_token:
            props["s3.session-token"] = credentials.session_token

        if credentials.extra:
            for key, value in credentials.extra.items():
                props[key] = value

        try:
            self._pyiceberg = load_catalog(self.name, **props)
        except Exception as exc:
            raise CatalogError(f"failed to load REST catalog {self.name!r}: {exc}") from exc
        return self._pyiceberg

    def duckdb_attach_sql(self, credentials: Credentials) -> list[str]:
        """Return DuckDB statements needed before query execution.

        The DuckDB iceberg extension (as of 1.5.x) only accepts
        ``ENDPOINT_TYPE`` values of ``'glue'`` and ``'s3_tables'`` — REST
        attach is not implemented yet. So this backend deliberately skips
        ATTACH and only registers an S3 secret for object-store reads.

        Consequences:

        * Catalog-qualified SQL like ``select * from <name>.<ns>.<tbl>`` is
          unavailable on REST.
        * Direct iceberg scans (``select * from iceberg_scan('s3://...')``)
          and pyiceberg-driven reads (``lake.pyiceberg_catalog().load_table(
          ...).scan().to_arrow()``) still work and benefit from the secret.
        * Writes through ``to_lake`` are unaffected — they go through
          PyIceberg directly.

        When DuckDB grows native REST attach, lift this restriction by
        emitting the ``ATTACH '<uri>' AS <name> (TYPE iceberg, ...)`` here.
        """
        secret_parts: list[str] = ["TYPE s3"]
        endpoint = (credentials.extra or {}).get("s3.endpoint")
        if endpoint:
            secret_parts.append(f"ENDPOINT '{_strip_scheme(endpoint)}'")
            secret_parts.append(f"USE_SSL {'TRUE' if _is_https(endpoint) else 'FALSE'}")
        if (credentials.extra or {}).get("s3.path-style-access") == "true":
            secret_parts.append("URL_STYLE 'path'")
        if credentials.region:
            secret_parts.append(f"REGION '{credentials.region}'")
        if credentials.access_key_id:
            secret_parts.append(f"KEY_ID '{credentials.access_key_id}'")
        if credentials.secret_access_key:
            secret_parts.append(f"SECRET '{credentials.secret_access_key}'")
        if credentials.session_token:
            secret_parts.append(f"SESSION_TOKEN '{credentials.session_token}'")

        # Empty secret is rejected by DuckDB; only emit when we have S3 props.
        if len(secret_parts) <= 1:
            return []
        secret_name = f"awebo_{self.name}_s3"
        return [f"CREATE OR REPLACE SECRET {secret_name} ({', '.join(secret_parts)});"]

    def ensure_namespace(self, credentials: Credentials, namespace: str) -> None:
        catalog = self.pyiceberg(credentials)
        existing = {".".join(ns) for ns in catalog.list_namespaces()}
        if namespace in existing:
            return
        log.debug("creating namespace %r", namespace)
        try:
            catalog.create_namespace(namespace)
        except Exception as exc:
            raise CatalogError(f"failed to create namespace {namespace!r}: {exc}") from exc
