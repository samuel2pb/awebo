"""Iceberg writer on DuckDB's native Iceberg write support (DuckDB >= 2.0).

``Lakehouse.to_lake(data, database, table)`` accepts:

* a Polars ``DataFrame`` or ``LazyFrame``
* a PyArrow ``Table`` or ``RecordBatchReader``
* a SQL string (any query DuckDB can run, including over attached catalogs)

In-memory sources are registered with DuckDB and scanned zero-copy;
``RecordBatchReader`` and ``LazyFrame`` sources are pulled on demand. DuckDB
writes the Parquet data files and commits to the attached Iceberg catalog,
so memory stays bounded by DuckDB's own ``memory_limit`` and spills to
``temp_directory``: there is no separate streaming path to choose.

Table lifecycle
---------------

* ``CREATE SCHEMA IF NOT EXISTS`` for the namespace.
* Missing table: ``CREATE TABLE ... PARTITIONED BY (...) WITH (...)`` from the
  explicit PyIceberg ``schema`` / ``partition_spec`` or, without them, from
  the source's column types.
* Existing table: ``ALTER TABLE ADD COLUMN`` / ``ALTER COLUMN TYPE`` for schema
  evolution and ``ALTER TABLE SET PARTITIONED BY`` for partition evolution.

Write modes
-----------

``mode="append"``: one ``INSERT INTO ... BY NAME``.

``mode="overwrite"``: the source is staged once into a DuckDB temp table (so
one-shot readers and SQL are read exactly once), then one transaction runs
``DELETE`` + ``INSERT``, which DuckDB commits as a single Iceberg snapshot.
On a partitioned table the ``DELETE`` only matches rows whose partition
values (every transform: identity, year/month/day/hour, bucket, truncate)
appear in the incoming data: a dynamic partition overwrite. An unpartitioned
table is fully replaced. An empty source never deletes anything.

DuckDB writes merge-on-read positional deletes; compact with
``CALL iceberg_rewrite_data_files(...)`` when delete files pile up.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from awebo._sql import qualified, quote_ident, quote_literal
from awebo.exceptions import QueryError, WriteError
from awebo.io.ddl import (
    PartitionTerm,
    column_definitions,
    partition_terms,
    schema_field_names,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    import polars as pl
    from pyiceberg.partitioning import PartitionSpec as PyIcebergPartitionSpec
    from pyiceberg.schema import Schema as PyIcebergSchema

    from awebo.catalog.base import CatalogBackend
    from awebo.lineage.emitter import LineageEmitter
    from awebo.query.engine import QueryEngine

type WriteSource = "pl.DataFrame | pl.LazyFrame | pa.Table | pa.RecordBatchReader | str"
type WriteMode = Literal["append", "overwrite"]
type Columns = dict[str, str]
"""Column name to DuckDB type string, in table order."""

_VALID_MODES: frozenset[str] = frozenset({"append", "overwrite"})

# Lossless widenings DuckDB's iceberg ALTER COLUMN TYPE accepts (Iceberg spec
# type promotion). Decimal precision growth is handled separately.
_WIDENINGS: frozenset[tuple[str, str]] = frozenset(
    {
        ("INTEGER", "BIGINT"),
        ("FLOAT", "DOUBLE"),
    }
)

log = logging.getLogger(__name__)


class IcebergWriter:
    """Writes sources into Iceberg tables of the attached catalog."""

    def __init__(
        self,
        engine: QueryEngine,
        catalog: CatalogBackend,
        *,
        emitter: LineageEmitter | None = None,
        lineage_namespace: str = "awebo",
    ) -> None:
        self._engine = engine
        self._catalog = catalog
        self._lineage_namespace = lineage_namespace
        if emitter is None:
            from awebo.lineage.emitter import NoopEmitter

            self._emitter: LineageEmitter = NoopEmitter()
        else:
            self._emitter = emitter

    def write(
        self,
        data: WriteSource,
        database: str,
        table: str,
        *,
        mode: WriteMode = "append",
        schema: PyIcebergSchema | None = None,
        partition_spec: PyIcebergPartitionSpec | None = None,
        schema_evolution: bool = True,
        properties: Mapping[str, str] | None = None,
    ) -> None:
        if mode not in _VALID_MODES:
            raise WriteError(f"invalid write mode {mode!r}; expected 'append' or 'overwrite'")

        identifier = f"{database}.{table}"
        run_id = self._new_run_id()
        self._emit_start(identifier, run_id)
        t0 = time.perf_counter()
        target = _Target(self._catalog.name, database, table)
        plan = _TablePlan(schema, partition_spec, schema_evolution, properties)

        try:
            with self._scan(data) as source:
                if mode == "overwrite":
                    self._overwrite(source, target, plan)
                else:
                    self._append(source, target, plan)
        except WriteError:
            self._emit_fail(identifier, run_id)
            raise
        except Exception as exc:
            self._emit_fail(identifier, run_id)
            raise WriteError(f"iceberg {mode} to {identifier!r} failed: {exc}") from exc

        self._log_write_done(mode, identifier, t0)
        self._emit_complete(identifier, run_id)

    # ----------------------------------------------------------- write modes

    def _append(self, source: str, target: _Target, plan: _TablePlan) -> None:
        self._prepare_table(target, self._describe(f"SELECT * FROM {source}"), plan)
        self._engine.execute(f"INSERT INTO {target.sql} BY NAME SELECT * FROM {source}")

    def _overwrite(self, source: str, target: _Target, plan: _TablePlan) -> None:
        stage = quote_ident(f"_awebo_stage_{uuid.uuid4().hex}")
        rows = self._engine.fetch_rows(f"CREATE TEMP TABLE {stage} AS SELECT * FROM {source}")
        try:
            if not rows or rows[0][0] == 0:
                log.warning("empty source — skipping overwrite of %s", target.identifier)
                return
            columns = self._describe(f"SELECT * FROM {stage}")
            self._prepare_table(target, columns, plan)
            terms = self._current_partition_terms(target)
            with self._engine.transaction():
                self._engine.execute(self._scoped_delete_sql(target, stage, columns, terms))
                self._engine.execute(f"INSERT INTO {target.sql} BY NAME SELECT * FROM {stage}")
        finally:
            self._engine.execute(f"DROP TABLE IF EXISTS {stage}")

    @staticmethod
    def _scoped_delete_sql(
        target: _Target, stage: str, columns: Columns, terms: list[PartitionTerm]
    ) -> str:
        """``DELETE`` every target row in a partition the staged data touches."""
        if not terms:
            return f"DELETE FROM {target.sql}"
        keys: list[str] = []
        matches: list[str] = []
        for i, term in enumerate(terms):
            key = f"k{i}"
            col = quote_ident(term.column)
            staged = term.scope_expr(col) if term.column in columns else "NULL"
            keys.append(f"{staged} AS {key}")
            matches.append(f"p.{key} IS NOT DISTINCT FROM {term.scope_expr(f'tgt.{col}')}")
        return (
            f"DELETE FROM {target.sql} AS tgt WHERE EXISTS ("
            f"SELECT 1 FROM (SELECT DISTINCT {', '.join(keys)} FROM {stage}) AS p "
            f"WHERE {' AND '.join(matches)})"
        )

    # -------------------------------------------------------- table lifecycle

    def _prepare_table(self, target: _Target, source_columns: Columns, plan: _TablePlan) -> None:
        """Create the table, or evolve schema / partitioning / properties to the plan."""
        self._engine.execute(f"CREATE SCHEMA IF NOT EXISTS {target.schema_sql}")
        existing = self._table_columns(target)
        if existing is None:
            self._create_table(target, source_columns, plan)
            if plan.schema is not None and plan.schema_evolution:
                created = self._table_columns(target) or {}
                self._evolve_schema(target, created, source_columns)
            return

        if plan.schema_evolution:
            self._evolve_schema(target, existing, source_columns)
        else:
            extra = sorted(set(source_columns) - set(existing))
            if extra:
                raise WriteError(
                    f"schema evolution disabled and {target.identifier!r} lacks columns {extra}"
                )
        if plan.partition_spec is not None:
            self._evolve_partitioning(target, plan)
        if plan.properties:
            self._sync_properties(target, plan.properties)

    def _create_table(self, target: _Target, source_columns: Columns, plan: _TablePlan) -> None:
        log.debug("creating table %s", target.identifier)
        if plan.schema is not None:
            columns = column_definitions(plan.schema)
            field_names = schema_field_names(plan.schema)
        else:
            columns = list(source_columns.items())
            field_names = {i: name for i, name in enumerate(source_columns, start=1)}
        cols_sql = ", ".join(f"{quote_ident(name)} {dtype}" for name, dtype in columns)
        sql = f"CREATE TABLE {target.sql} ({cols_sql})"

        if plan.partition_spec is not None:
            terms = partition_terms(plan.partition_spec, field_names)
            if terms:
                sql += f" PARTITIONED BY ({', '.join(t.ddl() for t in terms)})"

        table_props = dict(plan.properties or {})
        location = self._catalog.table_location(target.database, target.table)
        if location is not None:
            table_props.setdefault("location", location)
        if table_props:
            rendered = ", ".join(
                f"{quote_literal(k)} = {quote_literal(v)}" for k, v in table_props.items()
            )
            sql += f" WITH ({rendered})"
        self._engine.execute(sql)

    def _table_columns(self, target: _Target) -> Columns | None:
        """Return the live table's columns, or ``None`` if it does not exist."""
        import duckdb

        try:
            return self._describe(target.sql)
        except QueryError as exc:
            if isinstance(exc.__cause__, duckdb.CatalogException):
                return None
            raise

    def _describe(self, relation_sql: str) -> Columns:
        rows = self._engine.fetch_rows(f"DESCRIBE {relation_sql}")
        return {str(row[0]): str(row[1]) for row in rows}

    def _evolve_schema(self, target: _Target, existing: Columns, incoming: Columns) -> None:
        """Add new columns and apply lossless type widenings."""
        try:
            for name, dtype in incoming.items():
                current = existing.get(name)
                if current is None:
                    self._engine.execute(
                        f"ALTER TABLE {target.sql} ADD COLUMN {quote_ident(name)} {dtype}"
                    )
                elif current != dtype and _is_widening(current, dtype):
                    self._engine.execute(
                        f"ALTER TABLE {target.sql} ALTER COLUMN {quote_ident(name)} TYPE {dtype}"
                    )
        except Exception as exc:
            raise WriteError(f"schema evolution failed: {exc}") from exc

    def _current_partition_terms(self, target: _Target) -> list[PartitionTerm]:
        rows = self._engine.fetch_rows(
            "SELECT transform, source_column FROM iceberg_partition_specs(?) WHERE is_default",
            [target.sql],
        )
        terms = [PartitionTerm.parse(str(t), str(c)) for t, c in rows]
        return [t for t in terms if not t.is_void]

    def _evolve_partitioning(self, target: _Target, plan: _TablePlan) -> None:
        """``SET PARTITIONED BY`` when the desired terms differ from the live spec.

        Source ids resolve through the caller's ``schema`` when given, else
        through the live table schema: ids then match whatever engine created
        the table (PyIceberg or DuckDB).
        """
        assert plan.partition_spec is not None
        if plan.schema is not None:
            field_names = schema_field_names(plan.schema)
        else:
            field_names = self._live_field_names(target)
        desired = partition_terms(plan.partition_spec, field_names)
        current = self._current_partition_terms(target)
        if set(desired) == set(current):
            return
        try:
            if desired:
                items = ", ".join(t.ddl() for t in desired)
                self._engine.execute(f"ALTER TABLE {target.sql} SET PARTITIONED BY ({items})")
            else:
                self._engine.execute(f"ALTER TABLE {target.sql} RESET PARTITIONED BY")
        except Exception as exc:
            raise WriteError(f"partition evolution failed: {exc}") from exc

    def _live_field_names(self, target: _Target) -> dict[int, str]:
        rows = self._engine.fetch_rows(
            "SELECT metadata FROM iceberg_load_table_response(?)", [target.sql]
        )
        metadata = rows[0][0]
        if not isinstance(metadata, dict):
            raise WriteError(f"unexpected table metadata for {target.identifier!r}")
        schema_id = metadata.get("current-schema-id")
        for schema in metadata.get("schemas") or []:
            if isinstance(schema, dict) and schema.get("schema-id") == schema_id:
                return {
                    int(f["id"]): str(f["name"])
                    for f in schema.get("fields") or []
                    if isinstance(f, dict)
                }
        raise WriteError(f"current schema not found for {target.identifier!r}")

    def _sync_properties(self, target: _Target, desired: Mapping[str, str]) -> None:
        rows = self._engine.fetch_rows(
            "SELECT key, value FROM iceberg_table_properties(?)", [target.sql]
        )
        current = {str(k): str(v) for k, v in rows}
        changed = {k: v for k, v in desired.items() if current.get(k) != v}
        if not changed:
            return
        rendered = ", ".join(f"{quote_literal(k)}: {quote_literal(v)}" for k, v in changed.items())
        self._engine.execute(
            f"CALL set_iceberg_table_properties({quote_literal(target.sql)}, {{{rendered}}})"
        )

    # ---------------------------------------------------------------- source

    @contextmanager
    def _scan(self, data: WriteSource) -> Iterator[str]:
        """Yield SQL that reads ``data``: a subquery, or a registered view name."""
        import polars as pl

        if isinstance(data, str):
            yield f"({data.strip().rstrip(';')})"
            return
        if not isinstance(data, (pa.Table, pa.RecordBatchReader, pl.DataFrame, pl.LazyFrame)):
            raise WriteError(f"unsupported write source type: {type(data).__name__}")
        alias = f"_awebo_src_{uuid.uuid4().hex}"
        self._engine.register(alias, data)
        try:
            yield quote_ident(alias)
        finally:
            self._engine.unregister(alias)

    # --------------------------------------------------------------- lineage

    @staticmethod
    def _log_write_done(mode: str, identifier: str, t0: float) -> None:
        from awebo._observability import log_peak_memory

        log_peak_memory(log)
        log.info("%s %s [%.3fs]", mode, identifier, time.perf_counter() - t0)

    @staticmethod
    def _new_run_id() -> str:
        from openlineage.client.uuid import generate_new_uuid

        return str(generate_new_uuid())

    def _emit_start(self, identifier: str, run_id: str) -> None:
        try:
            from awebo.lineage.events import write_start

            self._emitter.emit(write_start(self._lineage_namespace, identifier, run_id))
        except Exception:
            log.warning("lineage START emission failed for %s", identifier, exc_info=True)

    def _emit_complete(self, identifier: str, run_id: str) -> None:
        try:
            from awebo.lineage.events import write_complete

            self._emitter.emit(write_complete(self._lineage_namespace, identifier, run_id))
        except Exception:
            log.warning("lineage COMPLETE emission failed for %s", identifier, exc_info=True)

    def _emit_fail(self, identifier: str, run_id: str) -> None:
        try:
            from awebo.lineage.events import write_fail

            self._emitter.emit(write_fail(self._lineage_namespace, identifier, run_id))
        except Exception:
            log.warning("lineage FAIL emission failed for %s", identifier, exc_info=True)


@dataclass(frozen=True, slots=True)
class _Target:
    """The ``<catalog>.<database>.<table>`` a write lands in."""

    catalog: str
    database: str
    table: str

    @property
    def identifier(self) -> str:
        return f"{self.database}.{self.table}"

    @property
    def sql(self) -> str:
        return qualified(self.catalog, self.database, self.table)

    @property
    def schema_sql(self) -> str:
        return qualified(self.catalog, self.database)


@dataclass(frozen=True, slots=True)
class _TablePlan:
    """What the caller asked the table to look like."""

    schema: PyIcebergSchema | None
    partition_spec: PyIcebergPartitionSpec | None
    schema_evolution: bool
    properties: Mapping[str, str] | None


_DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")


def _is_widening(current: str, incoming: str) -> bool:
    """True when ``incoming`` is an Iceberg-legal promotion of ``current``."""
    if (current, incoming) in _WIDENINGS:
        return True
    cur = _DECIMAL_RE.match(current)
    new = _DECIMAL_RE.match(incoming)
    if cur and new:
        return cur.group(2) == new.group(2) and int(new.group(1)) > int(cur.group(1))
    return False
