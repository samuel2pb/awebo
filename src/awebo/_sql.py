"""SQL rendering helpers shared by every module that builds DuckDB SQL.

awebo builds DDL (``CREATE SECRET``, ``ATTACH``, ``CREATE TABLE``) and
identifiers dynamically. DuckDB cannot bind identifiers or secret options as
prepared-statement parameters, so every value that lands in SQL text goes
through one of these two functions.
"""

from __future__ import annotations


def quote_ident(name: str) -> str:
    """Render ``name`` as a double-quoted DuckDB identifier."""
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Render ``value`` as a single-quoted DuckDB string literal."""
    return "'" + value.replace("'", "''") + "'"


def qualified(*parts: str) -> str:
    """Render a dotted, fully quoted name such as ``"lake"."sales"."orders"``."""
    return ".".join(quote_ident(p) for p in parts)
