# Architecture

> **Audience:** humans and coding agents working in this repo. Read this
> before grepping. The point of this file is to save you from spelunking
> through the source to figure out what lives where.

## TL;DR

`awebo` is a thin facade (`Lakehouse`) over four cooperating engines:

| Concern | Engine | Used for |
| --- | --- | --- |
| Read SQL | **DuckDB** + `iceberg` extension | Vectorised `SELECT` over Iceberg tables and external files |
| Write / DDL | **PyIceberg** | Table create, namespace create, schema evolution, append commits |
| In-memory data plane | **Apache Arrow** | Zero-copy bridge between DuckDB and PyIceberg, streaming batches |
| User-facing dataframe | **Polars** | Returned from `QueryResult.to_polars()` |

The split is deliberate: DuckDB owns reads (fast scan), PyIceberg owns
writes (correct commits), Arrow is the lingua franca, Polars is the
user-facing surface.

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
│   ├── base.py          #   CatalogBackend ABC: pyiceberg(), duckdb_attach_sql(), ensure_namespace()
│   ├── glue.py          #   GlueCatalogBackend (AWS Glue + S3)
│   ├── rest.py          #   RestCatalogBackend (Iceberg REST — Lakekeeper, Nessie, Polaris)
│   └── cloudflare.py    #   CloudflareCatalogBackend (R2 — delegates to REST)
│
├── query/               # FEATURE: read path
│   ├── engine.py        #   QueryEngine — single DuckDB connection per Lakehouse
│   └── result.py        #   QueryResult — lazy: .to_polars/.to_arrow/.batches/.show
│
├── io/                  # FEATURE: external file ingest + iceberg write
│   ├── reader.py        #   FileReader — autodetects format, registers a DuckDB VIEW
│   └── writer.py        #   IcebergWriter — auto-stream when payload > threshold
│
├── streaming/           # FEATURE: Arrow batch helpers shared by reader+writer
│   └── batches.py       #   estimate_size_mb, iter_batches_from_*
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
| Tune streaming threshold or batch size | `src/awebo/streaming/batches.py` and `src/awebo/config.py` |
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
   Both PyIceberg and DuckDB receive the same `Credentials` struct — never
   re-fetch (no D+1 STS calls).

3. **Catalog ATTACH is batched.** `CatalogBackend.duckdb_attach_sql()`
   returns a list executed in a single loop; secret + ATTACH go together.

4. **Writes go through PyIceberg, reads go through DuckDB.** Do not mix.
   Both share Arrow batches as the data plane.

5. **Auto-streaming.** `IcebergWriter` measures payload size via
   `estimate_size_mb`. Above `streaming_threshold_mb` (default 512MB), the
   writer switches to a `RecordBatchReader` that DuckDB / Polars chunk on
   demand — bounded memory regardless of source size. SQL writes are
   *always* streamed (never materialised).

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
DataFrame / Arrow / SQL string
        │
        ▼
IcebergWriter._coerce_to_reader  ──► size check ──► RecordBatchReader
                                                          │
                                                          ▼
                                          PyIceberg Catalog.{load,create}_table
                                                          │
                                                          ▼
                                                   table.append(reader)
```

### File-register path
```
read_file(path, alias) ──► FileReader.detect_format ──► CREATE OR REPLACE VIEW alias AS SELECT * FROM read_*(path)
```
The view is lazy; data is read on demand when a query touches the alias.

## Testing strategy

- **Unit tests** — pure-Python feature tests, no network. Mock catalog +
  S3 with `moto`.
- **Property tests** — Hypothesis. Mark with `@pytest.mark.property`.
  Targets: format detection, option rendering, batch chunking invariants,
  streaming threshold decisions.
- **Memory tests** — `pytest-memray`. Mark with `@pytest.mark.memory`.
  Targets: `IcebergWriter` streaming path, `QueryResult.batches`, anything
  on a hot loop. The FinOps moat depends on bounded memory.
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
