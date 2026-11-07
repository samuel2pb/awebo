"""Iceberg REST catalog backend.

Connects to any REST-compatible Iceberg catalog (Tabular's
``tabulario/iceberg-rest``, Lakekeeper, Nessie, Polaris, ...). Object-store
credentials ride on :class:`Credentials` and become a DuckDB ``TYPE s3``
secret; catalog auth (bearer token or OAuth2 client credentials) becomes a
DuckDB ``TYPE iceberg`` secret. ``ATTACH '<warehouse>' (TYPE iceberg, URI ...)``
then gives DuckDB full read + write access to the catalog.

Credential propagation is batched: at most two secrets and one ATTACH.
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

        self._add_credential_props(props, credentials)

        try:
            self._pyiceberg = load_catalog(self.name, **props)
        except Exception as exc:
            raise CatalogError(f"failed to load REST catalog {self.name!r}: {exc}") from exc
        return self._pyiceberg

    @staticmethod
    def _add_credential_props(props: dict[str, str], credentials: Credentials) -> None:
        if credentials.region:
            props["s3.region"] = credentials.region
        if credentials.access_key_id:
            props["s3.access-key-id"] = credentials.access_key_id
        if credentials.secret_access_key:
            props["s3.secret-access-key"] = credentials.secret_access_key
        if credentials.session_token:
            props["s3.session-token"] = credentials.session_token
        if credentials.s3:
            if credentials.s3.endpoint:
                props["s3.endpoint"] = credentials.s3.endpoint
            if credentials.s3.path_style:
                props["s3.path-style-access"] = "true"
        if credentials.extra:
            for key, value in credentials.extra.items():
                props[key] = value

    def duckdb_attach_sql(self, credentials: Credentials) -> AttachSpec:
        """Return the object-store secret, the catalog-auth secret and the ATTACH."""
        secrets: list[str] = []
        s3_secret = self._s3_secret_sql(credentials)
        if s3_secret is not None:
            secrets.append(s3_secret)

        attach_props = ["TYPE iceberg", f"URI {quote_literal(self._rest.uri)}"]
        auth_secret = self._auth_secret_sql()
        if auth_secret is None:
            attach_props.append("AUTHORIZATION_TYPE 'none'")
        else:
            secret_name, secret_sql = auth_secret
            secrets.append(secret_sql)
            attach_props.append(f"SECRET {secret_name}")

        attach = (
            f"ATTACH {quote_literal(self._rest.warehouse)} AS {quote_ident(self.name)} "
            f"({', '.join(attach_props)});"
        )
        return AttachSpec(secrets=secrets, attach=attach)

    def _auth_secret_sql(self) -> tuple[str, str] | None:
        """Return ``(name, CREATE SECRET ...)`` for catalog auth, or ``None``."""
        parts = ["TYPE iceberg"]
        if self._rest.token:
            parts.append(f"TOKEN {quote_literal(self._rest.token)}")
        elif self._rest.oauth_credential:
            client_id, _, client_secret = self._rest.oauth_credential.partition(":")
            token_uri = f"{self._rest.uri.rstrip('/')}/v1/oauth/tokens"
            parts.append(f"CLIENT_ID {quote_literal(client_id)}")
            parts.append(f"CLIENT_SECRET {quote_literal(client_secret)}")
            parts.append(f"OAUTH2_SERVER_URI {quote_literal(token_uri)}")
        else:
            return None
        name = quote_ident(f"awebo_{self.name}_iceberg")
        return name, f"CREATE OR REPLACE SECRET {name} ({', '.join(parts)});"

    def _s3_secret_sql(self, credentials: Credentials) -> str | None:
        parts: list[str] = ["TYPE s3"]
        s3 = credentials.s3
        if s3 and s3.endpoint:
            parts.append(f"ENDPOINT {quote_literal(_strip_scheme(s3.endpoint))}")
            parts.append(f"USE_SSL {'TRUE' if _is_https(s3.endpoint) else 'FALSE'}")
        if s3 and s3.path_style:
            parts.append("URL_STYLE 'path'")
        if credentials.region:
            parts.append(f"REGION {quote_literal(credentials.region)}")
        if credentials.access_key_id:
            parts.append(f"KEY_ID {quote_literal(credentials.access_key_id)}")
        if credentials.secret_access_key:
            parts.append(f"SECRET {quote_literal(credentials.secret_access_key)}")
        if credentials.session_token:
            parts.append(f"SESSION_TOKEN {quote_literal(credentials.session_token)}")
        if len(parts) <= 1:
            return None
        name = quote_ident(f"awebo_{self.name}_s3")
        return f"CREATE OR REPLACE SECRET {name} ({', '.join(parts)});"
