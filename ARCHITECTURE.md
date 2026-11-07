# Architecture

> **Audience:** humans and coding agents working in this repo. Read this
> before grepping. The point of this file is to save you from spelunking
> through the source to figure out what lives where.

## TL;DR

`awebo` is a thin facade (`Lakehouse`) over **DuckDB >= 2.0** and its
native Iceberg support:

| Concern | Engine | Used for |
| --- | --- | --- |
| Read SQL | **DuckDB** + `iceberg` extension | Vectorised `SELECT` over Iceberg tables and external files, time travel |
| Write / DDL | **DuckDB** + `iceberg` extension | `CREATE SCHEMA/TABLE`, `INSERT`, `DELETE`, `ALTER TABLE`, table properties — committed to the attached catalog |
| In-memory data plane | **Apache Arrow** | Zero-copy scans of Arrow / Polars sources registered with DuckDB |
| User-facing dataframe | **Polars** | Returned from `QueryResult.to_polars()` (DuckDB's native `.pl()`) |
| Escape hatch | **PyIceberg** | `Lakehouse.pyiceberg_catalog()` only — maintenance with no SQL verb. awebo never calls it. |

DuckDB attaches the catalog once (`ATTACH ... (TYPE iceberg, ...)`) and
every read and write goes through that one connection.

## Package layout (package-by-feature)

```
src/awebo/
├── __init__.py          # public API: `Lakehouse`, `QueryResult`, exceptions
├── lakehouse.py         # facade — wires features together, no logic
├── config.py            # env-var resolution → frozen LakehouseConfig
├── exceptions.py        # AweboError hierarchy
│
├── auth/                # FEATURE: cloud credential resolution
│   ├── base.py          #   AuthProvider ABC + Credentials struct
│   ├── aws.py           #   AwsAuthProvider (boto3 chain + optional STS assume-role)
│   └── s3.py            #   S3AuthProvider (static S3-compat creds for MinIO, R2, Ceph)
│
├── catalog/             # FEATURE: Iceberg catalog backends
│   ├── base.py          #   CatalogBackend ABC: duckdb_attach_sql(), table_location(), pyiceberg() (escape hatch)
│   ├── glue.py          #   GlueCatalogBackend (Glue Iceberg REST, SigV4; cn-* regions via explicit URI)
│   ├── rest.py          #   RestCatalogBackend (Iceberg REST — Lakekeeper, Nessie, Polaris; token / OAuth2 secret)
│   └── cloudflare.py    #   CloudflareCatalogBackend (R2 Data Catalog — delegates to REST)
│
├── query/               # FEATURE: read path
│   ├── engine.py        #   QueryEngine — single DuckDB connection, register(), transaction()
│   └── result.py        #   QueryResult — lazy: .to_polars/.to_arrow/.batches/.show
│
├── io/                  # FEATURE: external file ingest + iceberg write
│   ├── reader.py        #   FileReader — autodetects format, registers a DuckDB VIEW
│   ├── writer.py        #   IcebergWriter — DuckDB INSERT / DELETE+INSERT, schema + partition evolution
│   └── ddl.py           #   PyIceberg Schema / PartitionSpec → DuckDB DDL (pure functions)
│
├── lineage/             # FEATURE: OpenLineage event emission
│   ├── emitter.py       #   LineageEmitter protocol + NoopEmitter + HttpEmitter
│   ├── events.py        #   RunEvent construction helpers (write_start/complete/fail)
│   ├── factory.py       #   create_emitter() — auto-selects based on config
│   ├── s3.py            #   S3Emitter — persists events as JSON to S3
│   └── composite.py     #   CompositeEmitter — fans out to multiple children
│
└── quality/             # FEATURE: pluggable data quality testing
    ├── types.py         #   QualityCheck, QualitySuite, CheckResult, QualityResult, CheckThreshold
    ├── compiler.py      #   CheckCompiler protocol + SQL/Soda/OpenMetadata compilers
    └── runner.py        #   QualityRunner protocol + DuckDBSQLRunner
```

`tests/` mirrors this structure exactly. When adding a feature, add the
tests under the same package name.

## Where to make changes

| If you want to… | Edit |
| --- | --- |
| Add a new cloud's credentials | `src/awebo/auth/<cloud>.py` + register in `Lakehouse._build_auth` |
| Add a new catalog backend (REST, Hive, Nessie) | `src/awebo/catalog/<name>.py` + register in `Lakehouse._build_catalog` |
| Change the public surface | `src/awebo/lakehouse.py` and re-export in `src/awebo/__init__.py` |
| Tune DuckDB memory / spill | `src/awebo/config.py` (`memory_limit`, `temp_dir`) and `src/awebo/query/engine.py` |
| Change how writes map to SQL | `src/awebo/io/writer.py`; type / transform translation in `src/awebo/io/ddl.py` |
| Add a new file format auto-detection | `src/awebo/io/reader.py` (`_EXT_TO_FUNC` table) |
| Change how DuckDB is configured | `src/awebo/query/engine.py` |
| Add a lineage emitter backend | `src/awebo/lineage/<name>.py` + register in `factory.create_emitter` |
| Add quality check capabilities | `src/awebo/quality/runner.py` |
| Add a quality check dialect | `src/awebo/quality/compiler.py` + register in `runner._default_compiler` |
| Configure Cloudflare R2 | Set `AWEBO_CATALOG_TYPE=cloudflare` + `AWEBO_CF_*` env vars |

## Key invariants

1. **Single DuckDB connection per `Lakehouse`.** Held in `QueryEngine`.
   Closed by `Lakehouse.close()` / context-manager exit.

2. **Credentials resolved exactly once.** `AuthProvider.resolve()` caches.
   DuckDB secrets (and the PyIceberg escape hatch) receive the same
   `Credentials` struct — never re-fetch (no D+1 STS calls).

3. **Catalog ATTACH is batched.** `CatalogBackend.duckdb_attach_sql()`
   returns the secrets + one ATTACH, executed once per Lakehouse.

4. **Reads and writes both go through DuckDB.** awebo never writes data
   or DDL through PyIceberg; `pyiceberg()` exists only for
   `Lakehouse.pyiceberg_catalog()`. Every SQL value is quoted via
   `awebo._sql` (`quote_ident` / `quote_literal`).

5. **DuckDB bounds memory.** Sources are registered with DuckDB and
   scanned zero-copy; `RecordBatchReader` / `LazyFrame` sources are pulled
   on demand. DuckDB spills to `temp_dir` above `memory_limit`. Overwrite
   stages the source once into a DuckDB temp table, then runs `DELETE` +
   `INSERT` in one transaction (one atomic catalog commit).

6. **No D+1 (N+1) API patterns.** When a feature needs the same data
   twice, fetch once and pass it down. Anywhere you see a loop calling an
   external API, treat it as a bug.

7. **Lineage never breaks pipelines.** Every `emit()` call is wrapped
   in try/except — failures log a warning, never raise. Lineage is
   observability, not critical path.

## Engine boundaries (data flow)

### Read path
```
user SQL ──► QueryEngine.execute ──► DuckDB (iceberg ext) ──► Arrow ──► QueryResult
                                                                     ├──► Polars (.to_polars)
                                                                     ├──► Pandas (.to_pandas)
                                                                     └──► RecordBatchReader (.batches)
```

### Write path
```
DataFrame / LazyFrame / Arrow / RecordBatchReader / SQL string
        │
        ▼
IcebergWriter._scan ──► engine.register(alias)  (or "(sql)" subquery)
        │
        ▼
CREATE SCHEMA IF NOT EXISTS ─► DESCRIBE target ─► CREATE TABLE ... PARTITIONED BY ... WITH (...)
                                              └─► ALTER TABLE ADD COLUMN / ALTER COLUMN TYPE / SET PARTITIONED BY
        │
        ├── append:    INSERT INTO <cat>.<ns>.<tbl> BY NAME SELECT * FROM src
        └── overwrite: CREATE TEMP TABLE stage AS src
                       BEGIN; DELETE ... WHERE partition keys ∈ stage; INSERT ...; COMMIT
```

### File-register path
```
read_file(path, alias) ──► FileReader.detect_format ──► CREATE OR REPLACE VIEW alias AS SELECT * FROM read_*(path)
```
The view is lazy; data is read on demand when a query touches the alias.

## Testing strategy

- **Unit tests** — pure-Python feature tests, no network. Mock catalog +
  S3 with `moto`.
- **Pipeline tests** — a real `Lakehouse` against an in-process Iceberg
  REST catalog (`tests/_fixtures/rest_catalog.py`, PyIceberg `SqlCatalog`
  over SQLite + local files). DuckDB attaches it with the production
  `RestCatalogBackend` SQL; tests read writes back through the
  `SqlCatalog`.
- **Property tests** — Hypothesis. Mark with `@pytest.mark.property`.
  Targets: format detection, option rendering, partition-transform
  parsing (`io/ddl.py`), round-trip invariants.
- **Memory tests** — `pytest-memray`. Mark with `@pytest.mark.memory`.
  Targets: `IcebergWriter` under a tight `memory_limit`,
  `QueryResult.batches`, anything on a hot loop. The FinOps moat depends on bounded memory.
- **Integration tests** — `@pytest.mark.integration`, opt-in. Run against
  moto-backed Glue + S3, never real AWS in CI.

Run subsets via the Makefile: `make test`, `make test-prop`, `make test-mem`,
`make test-int`.

## Type-hint contract

Types are not optional. The repo enforces them at three layers:

1. **ruff `ANN`** — every `def` annotated.
2. **mypy `strict = true`** with extras (`disallow_any_explicit`,
   `disallow_any_unimported`, `warn_return_any`, `warn_unreachable`,
   `no_implicit_reexport`, `strict_equality`, `extra_checks`). Runs on
   `src/awebo` *and* `tests/`.
3. **pre-commit** — both gates run on every commit. Install with
   `make hooks`. Same gates run in CI.

Conventions:

- `from __future__ import annotations` at the top of every module.
- PEP-695 `type X = ...` for aliases — never `TypeAlias`.
- `__enter__` returns `Self`.
- Heavy / optional deps imported under `if TYPE_CHECKING:`; runtime types
  forward-referenced as strings.
- `pyarrow-stubs` is a dev dep because PyArrow ships a placeholder stub.

`make check` runs lint + format-check + mypy in one shot.

## Release / changelog

Every PR updates `CHANGELOG.md` under `## [Unreleased]`. Releases tag the
existing unreleased section with a version + date and start a new
unreleased block. Versioning follows SemVer.
