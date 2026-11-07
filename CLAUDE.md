# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Read first

Before any non-trivial change, **read `ARCHITECTURE.md`**. It exists so agents
do not have to grep their way to context. Also skim `CHANGELOG.md` to see
what shipped recently.

Before claiming a task done: update `CHANGELOG.md` (under `## [Unreleased]`)
and update `README.md` if the public surface changed.

## Common commands

All targets are wired into the `Makefile` and run via `uv`.

```bash
make install     # uv sync --all-extras
make lint        # ruff check
make fmt         # ruff format + autofix
make typecheck   # mypy strict on src/awebo
make test        # unit tests (excludes property/memory/integration/slow)
make test-prop   # Hypothesis @pytest.mark.property suite
make test-mem    # pytest-memray @pytest.mark.memory suite
make test-int    # @pytest.mark.integration (moto-backed)
make cov         # pytest with coverage
make build       # uv build distributables
```

Single test: `uv run pytest tests/path/to/test_file.py::test_name -v`.
Single test with property markers: include `-m property` (or remove the
default exclusions in `make test`).

## High-level architecture

`awebo` is a thin facade (`src/awebo/lakehouse.py:Lakehouse`) over DuckDB
(>= 2.0) and its native Iceberg read + write support:

| Concern | Engine | Purpose |
| --- | --- | --- |
| Read SQL | DuckDB + `iceberg` extension | Vectorised SELECT over Iceberg tables and external files |
| Write / DDL | DuckDB + `iceberg` extension | CREATE/ALTER TABLE, INSERT, DELETE against the attached catalog |
| Data plane | Apache Arrow | Zero-copy scans of Arrow / Polars sources registered with DuckDB |
| User-facing dataframe | Polars | Returned by `QueryResult.to_polars()` |
| Escape hatch only | PyIceberg | `Lakehouse.pyiceberg_catalog()`; awebo never writes through it |

The codebase is **package-by-feature** (not by layer):

```
src/awebo/
├── lakehouse.py     # facade, no logic
├── config.py        # env-var → frozen LakehouseConfig
├── exceptions.py    # AweboError hierarchy
├── auth/            # AuthProvider ABC + AwsAuthProvider
├── catalog/         # CatalogBackend ABC + GlueCatalogBackend
├── query/           # DuckDB QueryEngine + lazy QueryResult
├── io/              # FileReader (autodetect format) + IcebergWriter (DuckDB DML) + ddl.py
├── lineage/         # LineageEmitter protocol + Http/S3/Composite/Noop emitters
└── quality/         # QualityRunner protocol + DuckDBSQLRunner
```

`tests/` mirrors this structure exactly.

## Invariants — do not violate

1. **No D+1 (N+1) API patterns.** Batch external calls. When a feature
   needs the same data twice, fetch once and pass it down. Anywhere you see
   a loop calling an external API, treat it as a bug. Examples already in
   place: `AuthProvider.resolve()` caches credentials; catalog ATTACH is
   one batched list of statements; DuckDB secrets and the PyIceberg escape
   hatch share the same `Credentials` struct.

2. **Single DuckDB connection per `Lakehouse`.** Lives in `QueryEngine`,
   closed on `Lakehouse.close()` / context-manager exit.

3. **Reads and writes → DuckDB.** The attached Iceberg catalog carries
   both. Do not write or scan through PyIceberg; it is only the
   `pyiceberg_catalog()` escape hatch. Quote every SQL value with
   `awebo._sql.quote_ident` / `quote_literal`.

4. **DuckDB bounds memory.** Register sources with `QueryEngine.register`
   (zero-copy; readers and LazyFrames pulled on demand) — never
   materialise in Python. Memory is capped by `memory_limit`
   (`AWEBO_DUCKDB_MEMORY_LIMIT`) with spill to `temp_dir`. Overwrite is one
   `QueryEngine.transaction()` (one atomic catalog commit).

5. **Config is read once.** Only `config.py` touches `os.environ`. Rest of
   the library receives a frozen `LakehouseConfig` dataclass.

6. **DuckDB extensions** (`iceberg` + its `avro` dependency, `httpfs`) must
   be pre-installed in the runtime image. The engine `LOAD`s them; it deliberately does not
   `INSTALL` them on hot path.

## Testing requirements

- **Property tests (Hypothesis)** — required for parsers, validators, and
  invariants (format detection, batch chunking, idempotent ingest). Mark
  with `@pytest.mark.property`. Run via `make test-prop`.
- **Memory tests (pytest-memray)** — required for any code in a hot loop
  (writer, reader). Mark with `@pytest.mark.memory`. Track
  allocations, not wall time. Run before merging anything touching a hot
  loop: `make test-mem`.
- **Pipeline tests (`tests/pipelines/`)** — end-to-end dataflow tests that
  wire a real :class:`Lakehouse` against :class:`LocalIcebergCatalogBackend`
  (under `tests/_fixtures/`): an in-process Iceberg REST catalog over
  PyIceberg's SQLite `SqlCatalog`, attached by DuckDB like production. Use the
  `lake` / `spill_lake` fixtures from `tests/pipelines/conftest.py`;
  verify writes by re-loading via PyIceberg with the `_read_back` helper
  pattern. Add a new pipeline test for any new public verb on
  :class:`Lakehouse`.
- **Integration tests** — opt-in, mark `@pytest.mark.integration`. Use
  `moto[s3,glue]` for the production Glue + DuckDB-iceberg-attach path.
  Never real AWS in CI.

## Where to make changes

| If you want to… | Edit |
| --- | --- |
| Add a new cloud's credentials | `src/awebo/auth/<cloud>.py` + register in `Lakehouse._build_auth` |
| Add a new catalog backend (REST, Hive, Nessie) | `src/awebo/catalog/<name>.py` + register in `Lakehouse._build_catalog` |
| Change the public surface | `src/awebo/lakehouse.py` and re-export in `src/awebo/__init__.py` |
| Tune DuckDB memory / spill | `src/awebo/config.py` and `src/awebo/query/engine.py` |
| Change how writes map to SQL | `src/awebo/io/writer.py` (+ `src/awebo/io/ddl.py`) |
| Add a new file format auto-detection | `src/awebo/io/reader.py` (`_EXT_TO_FUNC` table) |
| Change DuckDB configuration | `src/awebo/query/engine.py` |
| Add a lineage emitter backend | `src/awebo/lineage/<name>.py` + register in `factory.create_emitter` |
| Add quality check capabilities | `src/awebo/quality/runner.py` |

## Tooling notes

- Python 3.12+ only (`requires-python = ">=3.12"`). Use PEP-695 `type X = ...`
  syntax for type aliases.
- `uv` is the canonical package manager — do not call `pip` or create
  venvs by hand. `uv.lock` is committed.
- `ruff` handles both lint and format. `mypy --strict` is enforced on
  **both** `src/awebo` and `tests/`.
- Lazy imports (inside functions) are intentional throughout — they keep
  `import awebo` fast for CLI consumers. `PLC0415` is silenced project-wide.

## Type-hint enforcement (do not weaken)

Type hints are mandatory and enforced by three layers — never bypass them.

1. **ruff `ANN` ruleset** — every `def` (including `__init__`, fixtures,
   helpers) must have argument and return annotations. `ANN` is selected in
   `[tool.ruff.lint]`. Tests waive `ANN201` / `ANN001` for pytest fixtures
   only.
2. **mypy strict** — `pyproject.toml` enables `strict = true` plus
   `disallow_any_explicit`, `disallow_any_unimported`, `warn_return_any`,
   `warn_unreachable`, `no_implicit_reexport`, `strict_equality`,
   `extra_checks`. Runs on `src/awebo` *and* `tests/`. `pyarrow-stubs` is
   a dev dependency because PyArrow ships a placeholder stub.
3. **pre-commit** — `.pre-commit-config.yaml` runs ruff + mypy on every
   commit. Install with `make hooks`. CI runs the same gates.

When you add code:

- No bare `Any` returns. If you legitimately need a dynamic type, narrow
  with `isinstance` and return the concrete type, or define a `Protocol`.
- No untyped `*args`, `**kwargs`. Use `ParamSpec` / `TypeVarTuple` if
  truly variadic.
- Forward refs (`if TYPE_CHECKING:`) are fine and encouraged for heavy
  optional deps; keep `from __future__ import annotations` at the top of
  every module so string-form annotations work uniformly.
- `__enter__` returns `Self`, not the class name.
- Type aliases use `type X = ...` (PEP 695), not `TypeAlias` or bare
  assignment.

Run `make check` before pushing — it gates lint, format, and types in
one shot.

## TDD workflow (vertical slices)

All feature work follows test-driven development with **vertical slices**:
one test → one implementation → repeat. Never write all tests first then all
implementation (horizontal slicing — produces brittle tests).

- Tests verify **behavior through public interfaces**, not implementation.
- No mocking internal collaborators — mock at system boundaries only
  (external APIs, filesystems).
- Each test reads like a specification: "user can X" or "system does Y when Z".
- Tests must survive internal refactors — if you rename a private function
  and a test breaks, that test was testing implementation.
- Use the `tests/_fixtures/` catalog backend for pipeline tests; verify
  writes by re-loading via PyIceberg (`_read_back` pattern).

## Architecture principles

- **Deep modules**: small interface, deep implementation, high leverage.
  Prefer one class that does a lot behind a simple API over many thin
  pass-through wrappers.
- **Deletion test**: imagine deleting a module. If complexity scatters
  across N callers, it was earning its keep. If complexity vanishes, it
  was pass-through — consider inlining.
- **Seams**: one adapter = hypothetical seam. Two adapters = real seam.
  Don't create abstractions until you have two concrete implementations.
- **Injection over construction**: pass dependencies in, don't build them
  internally. `Lakehouse.__init__` accepts `catalog=`, `auth=` for this
  reason. New subsystems (lineage, quality) follow the same pattern.
