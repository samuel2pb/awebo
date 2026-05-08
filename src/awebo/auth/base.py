"""Abstract auth contract.

Every cloud provider implements :class:`AuthProvider` exactly once. The
provider resolves long-lived configuration (profile, role) into short-lived
:class:`Credentials` that downstream code injects into DuckDB secrets and
PyIceberg catalog properties.

The contract is deliberately tiny so we never grow a per-cloud bespoke API;
callers always go through ``resolve()`` -> ``Credentials`` -> consumers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class S3Credentials:
    """Typed S3-compatible object-store credentials.

    Replaces the untyped ``extra`` dict for S3 endpoint/path-style config
    so producers (auth) and consumers (catalog backends) get compile-time safety.
    """

    endpoint: str | None = None
    path_style: bool = False
    access_key_id: str | None = None
    secret_access_key: str | None = None
    region: str | None = None


@dataclass(frozen=True, slots=True)
class Credentials:
    """Cloud-agnostic credential bundle.

    Fields are intentionally a superset of what AWS, GCP and Azure require so
    every provider can populate the same struct. Unused fields stay ``None``.
    """

    region: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    s3: S3Credentials | None = None
    extra: dict[str, str] | None = None


class AuthProvider(ABC):
    """Resolves runtime credentials for a single cloud."""

    cloud: str

    @abstractmethod
    def resolve(self) -> Credentials:
        """Resolve credentials. Called once per :class:`Lakehouse` lifetime.

        Implementations should batch any STS / metadata calls (no D+1).
        """
