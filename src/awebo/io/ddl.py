"""Translate PyIceberg schema / partition-spec objects into DuckDB Iceberg DDL.

``Lakehouse.to_lake`` keeps accepting PyIceberg ``Schema`` and
``PartitionSpec`` objects (callers build them from data contracts), but
tables are created and evolved by DuckDB's iceberg extension. This module is
the pure, side-effect-free bridge: types to DuckDB type strings, partition
fields to :class:`PartitionTerm` values that render as ``PARTITIONED BY``
items and as the matching scope expressions for partition overwrite.

Partition transforms use the Iceberg spec spelling on both sides:
PyIceberg's ``str(transform)`` and the ``transform`` column of DuckDB's
``iceberg_partition_specs()`` are both ``identity``, ``year``, ``month``,
``day``, ``hour``, ``bucket[N]``, ``truncate[W]`` or ``void``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from awebo._sql import quote_ident
from awebo.exceptions import WriteError

if TYPE_CHECKING:
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.types import IcebergType

_PRIMITIVES: Final[dict[str, str]] = {
    "boolean": "BOOLEAN",
    "int": "INTEGER",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "date": "DATE",
    "time": "TIME",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    "timestamp_ns": "TIMESTAMP_NS",
    "timestamptz_ns": "TIMESTAMPTZ_NS",
    "string": "VARCHAR",
    "uuid": "UUID",
    "binary": "BLOB",
    "variant": "VARIANT",
}

_TEMPORAL: Final[frozenset[str]] = frozenset({"year", "month", "day", "hour"})
_PARAMETRISED: Final[frozenset[str]] = frozenset({"bucket", "truncate"})
_TRANSFORM_RE: Final[re.Pattern[str]] = re.compile(r"^([a-z_]+)(?:\[(\d+)\])?$")


def duckdb_type(field_type: IcebergType) -> str:
    """Render an Iceberg type as a DuckDB type string."""
    from pyiceberg.types import DecimalType, FixedType, ListType, MapType, StructType

    if isinstance(field_type, StructType):
        inner = ", ".join(
            f"{quote_ident(f.name)} {duckdb_type(f.field_type)}" for f in field_type.fields
        )
        return f"STRUCT({inner})"
    if isinstance(field_type, ListType):
        return f"{duckdb_type(field_type.element_type)}[]"
    if isinstance(field_type, MapType):
        return f"MAP({duckdb_type(field_type.key_type)}, {duckdb_type(field_type.value_type)})"
    if isinstance(field_type, DecimalType):
        return f"DECIMAL({field_type.precision}, {field_type.scale})"
    if isinstance(field_type, FixedType):
        return "BLOB"
    name = str(field_type)
    try:
        return _PRIMITIVES[name]
    except KeyError:
        raise WriteError(f"unsupported Iceberg type for DuckDB DDL: {name!r}") from None


def column_definitions(schema: Schema) -> list[tuple[str, str]]:
    """Return ``(name, duckdb_type)`` pairs for the top-level schema fields."""
    return [(f.name, duckdb_type(f.field_type)) for f in schema.fields]


@dataclass(frozen=True, slots=True)
class PartitionTerm:
    """One partition field, keyed by what it partitions on (not by its name).

    Two terms are equal when they apply the same transform to the same
    source column, which is exactly when Iceberg routes rows identically.
    """

    transform: str
    column: str
    param: int | None = None

    @classmethod
    def parse(cls, transform: str, column: str) -> PartitionTerm:
        """Build a term from an Iceberg transform string (``bucket[16]``, ``day``...)."""
        match = _TRANSFORM_RE.match(transform.strip().lower())
        if match is None:
            raise WriteError(f"unrecognised partition transform {transform!r}")
        kind, raw_param = match.group(1), match.group(2)
        if kind in _PARAMETRISED:
            if raw_param is None:
                raise WriteError(f"partition transform {transform!r} needs a parameter")
            return cls(kind, column, int(raw_param))
        if kind in {"identity", "void"} or kind in _TEMPORAL:
            if raw_param is not None:
                raise WriteError(f"partition transform {transform!r} takes no parameter")
            return cls(kind, column)
        raise WriteError(f"unsupported partition transform {transform!r}")

    @property
    def is_void(self) -> bool:
        return self.transform == "void"

    def ddl(self) -> str:
        """Render as a ``PARTITIONED BY`` item."""
        col = quote_ident(self.column)
        if self.transform == "identity":
            return col
        if self.transform in _PARAMETRISED:
            return f"{self.transform}({self.param}, {col})"
        return f"{self.transform}({col})"

    def scope_expr(self, column_sql: str) -> str:
        """Render the value this term routes ``column_sql`` to.

        Rows from the incoming data and rows already in the table belong to
        the same partition iff these expressions are not distinct.
        """
        if self.transform == "identity":
            return column_sql
        if self.transform in _TEMPORAL:
            return f"date_trunc('{self.transform}', {column_sql})"
        if self.transform == "bucket":
            return f"iceberg_bucket({self.param}, {column_sql})"
        if self.transform == "truncate":
            return f"iceberg_truncate({self.param}, {column_sql})"
        return "NULL"


def partition_terms(spec: PartitionSpec, field_names: dict[int, str]) -> list[PartitionTerm]:
    """Resolve a PyIceberg ``PartitionSpec`` into terms.

    ``field_names`` maps Iceberg field id to column name for the schema the
    spec was built against. Void fields are dropped.
    """
    terms: list[PartitionTerm] = []
    for field in spec.fields:
        try:
            column = field_names[field.source_id]
        except KeyError:
            raise WriteError(
                f"partition field {field.name!r} references unknown source id {field.source_id}"
            ) from None
        term = PartitionTerm.parse(str(field.transform), column)
        if not term.is_void:
            terms.append(term)
    return terms


def schema_field_names(schema: Schema) -> dict[int, str]:
    """Map top-level field id to name for a PyIceberg schema."""
    return {f.field_id: f.name for f in schema.fields}
