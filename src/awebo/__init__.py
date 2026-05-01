"""awebo — DuckDB-powered Apache Iceberg data wrangler.

Public API is intentionally small: the :class:`Lakehouse` facade is the only
import most users need.
"""

from awebo._version import __version__
from awebo.exceptions import (
    AuthError,
    AweboError,
    CatalogError,
    ConfigError,
    QueryError,
    WriteError,
)
from awebo.lakehouse import Lakehouse
from awebo.query.result import QueryResult

__all__ = [
    "AuthError",
    "AweboError",
    "CatalogError",
    "ConfigError",
    "Lakehouse",
    "QueryError",
    "QueryResult",
    "WriteError",
    "__version__",
]
