"""Stub auth provider for offline tests."""

from __future__ import annotations

from awebo.auth.base import AuthProvider, Credentials


class NoopAuthProvider(AuthProvider):
    """Returns empty credentials. Suitable for offline / local-fs catalogs."""

    cloud = "noop"

    def resolve(self) -> Credentials:
        return Credentials()
