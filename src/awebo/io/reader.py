"""File reader with format auto-detection.

``Lakehouse.read_file(path, alias=...)`` registers an external file as a
DuckDB view named ``alias`` so subsequent ``sql()`` calls can join it. We
deliberately register a *view* (not a materialised table) so DuckDB streams
the file from disk/S3 on demand and respects the streaming threshold.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo._sql import quote_ident, quote_literal
from awebo.exceptions import QueryError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from awebo.query.engine import QueryEngine

type ReaderOptionScalar = bool | int | float | str
type ReaderOptionValue = ReaderOptionScalar | Sequence[ReaderOptionScalar]
type ReaderOptions = dict[str, ReaderOptionValue]

_EXT_TO_FUNC: dict[str, str] = {
    ".parquet": "read_parquet",
    ".pq": "read_parquet",
    ".csv": "read_csv",
    ".tsv": "read_csv",
    ".json": "read_json",
    ".ndjson": "read_json",
    ".jsonl": "read_json",
}

log = logging.getLogger(__name__)


class FileReader:
    def __init__(self, engine: QueryEngine) -> None:
        self._engine = engine

    @staticmethod
    def detect_format(path: str) -> str:
        lower = path.lower().split("?", 1)[0]
        for ext, func in _EXT_TO_FUNC.items():
            if lower.endswith(ext):
                return func
        raise QueryError(f"could not autodetect file format for {path!r}")

    def register(
        self,
        path: str,
        alias: str,
        options: ReaderOptions | None = None,
    ) -> None:
        func = self.detect_format(path)
        log.debug("registering %s as %r (reader=%s)", path, alias, func)
        opts_sql = ""
        if options:
            parts = [f"{k}={_render_option(v)}" for k, v in options.items()]
            opts_sql = ", " + ", ".join(parts)
        sql = (
            f"CREATE OR REPLACE VIEW {quote_ident(alias)} AS "
            f"SELECT * FROM {func}({quote_literal(path)}{opts_sql});"
        )
        self._engine.execute(sql)


def _render_option(value: ReaderOptionValue) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return quote_literal(value)
    # Sequence of scalars
    return "[" + ", ".join(_render_option(v) for v in value) + "]"
