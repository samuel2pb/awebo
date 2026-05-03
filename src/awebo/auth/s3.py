"""S3-compatible object-store auth provider.

Used by non-AWS catalogs (e.g. Iceberg REST against MinIO) where credentials
are local static keys plus an endpoint override. No STS, no profile chain —
everything is read once from :class:`S3Settings` and frozen into a
:class:`Credentials` bundle.

Endpoint and path-style addressing flags ride on :attr:`Credentials.extra`
under the ``s3.endpoint`` and ``s3.path-style-access`` keys; downstream
catalog backends (PyIceberg + DuckDB) read them from there.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo.auth.base import AuthProvider, Credentials

if TYPE_CHECKING:
    from awebo.config import S3Settings

log = logging.getLogger(__name__)


class S3AuthProvider(AuthProvider):
    cloud = "s3"

    def __init__(self, settings: S3Settings) -> None:
        self._settings = settings
        self._cached: Credentials | None = None

    def resolve(self) -> Credentials:
        if self._cached is not None:
            return self._cached

        log.debug("resolving S3 credentials (endpoint=%s)", self._settings.endpoint)

        extra: dict[str, str] = {}
        if self._settings.endpoint:
            extra["s3.endpoint"] = self._settings.endpoint
        if self._settings.path_style:
            extra["s3.path-style-access"] = "true"

        self._cached = Credentials(
            region=self._settings.region,
            access_key_id=self._settings.access_key_id,
            secret_access_key=self._settings.secret_access_key,
            extra=extra or None,
        )
        return self._cached
