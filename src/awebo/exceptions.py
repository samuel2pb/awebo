"""Exception hierarchy.

Every awebo error inherits from :class:`AweboError` so callers can catch the
whole library with a single except clause when integrating into pipelines.
"""

from __future__ import annotations


class AweboError(Exception):
    """Base class for all awebo errors."""


class ConfigError(AweboError):
    """Raised when configuration (env vars, files) is missing or invalid."""


class AuthError(AweboError):
    """Raised when credential resolution or assume-role fails."""


class CatalogError(AweboError):
    """Raised for catalog connection, namespace, or schema issues."""


class QueryError(AweboError):
    """Raised when DuckDB query execution fails inside awebo's wrapper."""


class WriteError(AweboError):
    """Raised when an Iceberg write/commit fails."""
