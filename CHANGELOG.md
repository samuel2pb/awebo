# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Container image + ECR publish workflow**: new `Dockerfile` (multi-stage,
  non-root) bakes the DuckDB `iceberg` and `httpfs` extensions at build
  time per the cold-start invariant.
  `.github/workflows/publish-ecr.yml` builds and pushes
  `${AWS_ECR_REPO}:latest` and `${AWS_ECR_REPO}:<sha>` on every push to
  `main` via OIDC (`AWS_ECR_REPO`, `AWS_REGION` repo vars;
  `AWS_OIDC_ROLE_ARN` secret).
- **Job-runner entrypoint** (`python -m awebo <target> [args...]`):
  `src/awebo/__main__.py` accepts (a) a single ``.py`` file, (b) a local
  directory containing ``__main__.py`` plus sibling modules, or (c) a
  ``.zip`` zipapp whose root contains ``__main__.py`` and any helper
  modules. Files (.py, .zip) may be passed as a local path or as an
  fsspec URI (`s3://`, `gs://`, `gcs://`, `https://`, `file://`, ...);
  remote targets are streamed into a private temp dir via fsspec.
  Execution uses :func:`runpy.run_path` inside the same interpreter, so
  ``sys.path[0]`` is the script parent / zip root / package dir (sibling
  imports resolve out of the box) and ``import awebo`` finds the
  installed package without re-spawning Python. Forwards trailing args
  as ``sys.argv[1:]``. New ``runner`` extra pulls ``gcsfs`` for GCS
  support; bundled in the container image.

### Fixed

- **REST catalog ATTACH (`#6`)**: `RestCatalogBackend.duckdb_attach_sql`
  no longer emits `ATTACH ... ENDPOINT_TYPE 'iceberg_rest'`. The DuckDB
  iceberg extension (1.5.x) only accepts `endpoint_type='glue'` or
  `'s3_tables'`, so the previous code made `Lakehouse()` instantiation
  fail eagerly against any REST catalog. The backend now emits only the
  S3 secret; `to_lake` works through PyIceberg, and reads must go through
  `iceberg_scan('s3://...')` or `Lakehouse.pyiceberg_catalog()` until
  DuckDB grows native REST attach.

### Added

- **Iceberg REST catalog backend** (`AWEBO_CATALOG_TYPE=rest`): new
  `RestCatalogBackend` and `S3AuthProvider` enable Tabular / Lakekeeper /
  Nessie / Polaris deployments and MinIO-backed local stacks. Adds env vars
  `AWEBO_REST_URI`, `AWEBO_REST_WAREHOUSE`, `AWEBO_REST_TOKEN`,
  `AWEBO_REST_CREDENTIAL`, `AWEBO_S3_ENDPOINT`, `AWEBO_S3_PATH_STYLE`,
  `AWEBO_S3_ACCESS_KEY_ID`, `AWEBO_S3_SECRET_ACCESS_KEY`, `AWEBO_S3_REGION`.
  DuckDB ATTACH emits at most one secret + one
  `ATTACH '<rest_uri>' AS <name> (TYPE iceberg, ENDPOINT_TYPE 'iceberg_rest')`
  (no D+1).
- **`Lakehouse.to_lake(mode=...)`**: `mode="overwrite"` replaces table
  contents via `pyiceberg.Table.overwrite`. Streaming sources
  (`RecordBatchReader`, SQL strings, payloads above
  `streaming_threshold_mb`) reject overwrite with `WriteError`. `mode`
  defaults to `"append"`.
- **`Lakehouse.to_lake(schema=..., partition_spec=...)`**: optional
  `pyiceberg.schema.Schema` and `pyiceberg.partitioning.PartitionSpec`
  arguments are honoured on table creation; ignored when the table
  already exists.
- **`Lakehouse.pyiceberg_catalog()`**: public escape hatch returning the
  cached underlying `pyiceberg.Catalog` for advanced ops (manifest tables,
  snapshots, schema introspection).

### Changed

- `LakehouseConfig` gains `s3: S3Settings | None` and
  `rest: RestSettings | None` fields. `aws` defaults to `None` so REST-only
  deployments resolve config without AWS env vars.

## [0.0.1]

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
