# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial scaffolding: `Lakehouse` facade, package-by-feature layout
  (`auth/`, `catalog/`, `query/`, `io/`, `streaming/`).
- AWS auth provider with optional STS assume-role and cached credentials.
- AWS Glue + S3 catalog backend (PyIceberg for write/DDL, DuckDB iceberg
  extension for reads).
- `Lakehouse.sql` / `read_file` / `to_lake` ergonomic verbs.
- Lazy `QueryResult` with `.to_polars / .to_arrow / .to_pandas / .batches /
  .show` materialisations.
- Auto-streaming writer that switches to `RecordBatchReader` when payload
  exceeds `AWEBO_STREAMING_THRESHOLD_MB` (default 512MB).
- File reader with format auto-detection (parquet, csv, tsv, json, ndjson).
- Test scaffolding: pytest markers `property`, `memory`, `integration`,
  `slow`. Hypothesis + pytest-memray dev dependencies.
- Tooling: ruff (lint+format), mypy strict, Makefile targets, GitHub
  Actions CI workflow, `ARCHITECTURE.md`, `README.md`.
- **Type-hint enforcement**: ruff `ANN` ruleset selected; mypy `strict`
  extended with `disallow_any_explicit`, `disallow_any_unimported`,
  `warn_return_any`, `warn_unreachable`, `no_implicit_reexport`,
  `strict_equality`, `extra_checks`; mypy now also checks `tests/`.
  `pyarrow-stubs` and `pre-commit` added as dev deps; pre-commit hook
  blocks commits that fail lint or type-check. `make check` runs the full
  gate locally.
- **Lakehouse injection seam**: `Lakehouse(config, *, catalog=..., auth=...)`
  bypasses the env-var-driven build for tests and for callers that already
  manage their own backends.
- **Pipeline test suite**: `tests/pipelines/` exercises end-to-end dataflow
  (read → SQL → Polars, file → Iceberg round-trip, SQL-string streamed
  write, three-stage bronze→silver→gold, RecordBatchReader input,
  streaming threshold). Backed by an offline `LocalIcebergCatalogBackend`
  fixture (PyIceberg `SqlCatalog` + sqlite metastore + tmpdir warehouse)
  so the production writer is exercised without cloud deps. Includes a
  Hypothesis round-trip invariant (`@property`) and a memray streaming
  test (`@memory`). `pyiceberg[sql-sqlite]` added as dev dep.
