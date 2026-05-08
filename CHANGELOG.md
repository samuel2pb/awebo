# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **OpenLineage event emission (`#18`)**: new `awebo.lineage` module with
  pluggable `LineageEmitter` protocol, `HttpEmitter` (wraps
  `openlineage-python` transport), and `NoopEmitter` (default when
  disabled). `IcebergWriter` emits `START` / `COMPLETE` / `FAIL`
  `RunEvent`s around every write operation. Emission failures log a
  warning and never break pipelines. Config via `AWEBO_LINEAGE_ENABLED`,
  `AWEBO_LINEAGE_URL`, `AWEBO_LINEAGE_API_KEY`, `AWEBO_LINEAGE_NAMESPACE`.
  New `awebo[lineage]` optional extra adds `openlineage-python`.
- **S3 lineage fallback (`#19`)**: `S3Emitter` persists OpenLineage
  `RunEvent` JSON to S3 (`{prefix}/{namespace}/{job}/{run_id}/{type}_{ts}.json`).
  `CompositeEmitter` fans out to multiple children with per-child failure
  isolation. When both HTTP and S3 are enabled, `create_emitter()` auto-
  constructs a `CompositeEmitter`. Config via `AWEBO_LINEAGE_S3_ENABLED`,
  `AWEBO_LINEAGE_S3_BUCKET`, `AWEBO_LINEAGE_S3_PREFIX`.
- **Data quality testing (`#20`)**: new `awebo.quality` module with
  `QualityCheck`, `QualitySuite`, `CheckResult`, `QualityResult`
  dataclasses and `QualityRunner` protocol. `DuckDBSQLRunner` executes
  SQL checks against DuckDB — `query` returning 0 rows = pass, >0 = fail.
  Warning-severity failures don't fail the suite. Each check is timed.
  On completion, emits an OpenLineage `DataQualityAssertionsDatasetFacet`
  via the configured `LineageEmitter`.

### Changed

- **Typed S3 credentials**: `Credentials.s3: S3Credentials | None` replaces
  the untyped `extra` dict for S3 endpoint/path-style config. Producers
  (auth providers) and consumers (catalog backends) now get compile-time
  safety instead of magic string keys. `extra` retained for future providers.
- **`AttachSpec` typed return**: `CatalogBackend.duckdb_attach_sql()` returns
  a typed `AttachSpec(secrets, attach)` instead of `list[str]`. Callers
  handle secrets and attach separately; REST backend explicitly signals
  `attach=None`.
- **Config validation centralized**: all catalog-specific validation
  (warehouse for Glue, URI/warehouse for REST) now lives solely in
  `load_config()`. Redundant checks removed from backend constructors
  and `Lakehouse._build_catalog()`.
- **`ensure_namespace()` shared implementation**: default implementation
  on `CatalogBackend` base class eliminates identical copies in Glue,
  REST, and local test backends.

### Added

- **`LakehouseBuilder`**: explicit builder pattern for composing Lakehouse
  with `.with_auth()` / `.with_catalog()` / `.build()`. Existing
  `Lakehouse(config, catalog=, auth=)` constructor remains as convenience.

### Fixed

- **Non-identity partition overwrite**: tables with non-identity partition
  transforms (e.g. `DayTransform`) now correctly fall back to full
  replacement via `tbl.overwrite()` instead of calling
  `dynamic_partition_overwrite()` which PyIceberg rejects.

### Added

- **Structured logging (`#15`)**: all modules emit structured log events via
  Python's stdlib `logging` at key lifecycle points — lakehouse init, write
  completion (with wall-clock duration), auth resolution, catalog connection,
  schema/partition evolution, streaming decisions. Library-only: callers
  configure handlers. The `__main__` entrypoint sets up `basicConfig` at
  INFO level.
- **Operation timing**: write operations log wall-clock duration at INFO level
  (e.g. `append bronze.events [0.423s]`).
- **Runtime memory reporting**: opt-in via `AWEBO_TRACE_MEMORY=1`. Uses
  `tracemalloc` (nframes=1) to report current/peak memory at write
  completion. Zero overhead when disabled.
- **Health check**: `Lakehouse.health_check()` returns a `dict[str, str]`
  diagnostic — DuckDB connectivity and catalog reachability.
- **Optional OpenTelemetry**: new `awebo[otel]` extra adds
  `opentelemetry-api`. Write and query operations emit OTel spans when the
  SDK is installed; zero overhead otherwise.

### Added

- **Dynamic partition overwrite (`#13`)**: `to_lake(..., mode="overwrite")`
  is now partition-aware. When the target table uses identity partition
  transforms, only the partitions present in the incoming data are replaced
  — untouched partitions are preserved. Unpartitioned tables still get a
  full replacement. All source types (SQL strings, `RecordBatchReader`,
  large DataFrames) are now accepted for overwrite. In-memory sources
  (Arrow tables and Polars DataFrames of any size) use PyIceberg's
  `dynamic_partition_overwrite` directly. SQL strings use a transactional
  path: a cheap `SELECT DISTINCT` pre-query extracts partition values for
  a scoped `delete`, then chunked `append` inside a single PyIceberg
  `Transaction` — DuckDB handles disk spill so memory stays bounded.
  `RecordBatchReader` (one-shot) falls back to full replacement.
- **Schema evolution**: `to_lake` now merges the incoming schema into an
  existing table via PyIceberg's `union_by_name` — adds new columns and
  widens compatible types (e.g. `int32` → `int64`). Enabled by default
  (`schema_evolution=True`); set to `False` to reject schema mismatches.
- **Partition evolution**: passing `partition_spec=` on a `to_lake` call
  against an existing table now evolves the partition spec (adds/removes
  fields) instead of silently ignoring it.

### Added

- **Container image + ECR publish workflow**: new `Dockerfile` (multi-stage,
  non-root) bakes the DuckDB `iceberg` and `httpfs` extensions at build
  time per the cold-start invariant.
  `.github/workflows/publish-ecr.yml` builds and pushes
  `${AWS_ECR_REPO}:latest` and `${AWS_ECR_REPO}:<sha>` on every push to
  `main` (`AWS_ECR_REPO`, `AWS_REGION` repo vars; `AWS_ACCESS_KEY` +
  `AWS_SECRET_ACCESS_KEY` secrets). `scripts/ecr-bootstrap.sh`
  idempotently provisions the ECR repo before the first workflow run.
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
- **Entrypoint runtime deps (`#9`)**: `python -m awebo` accepts
  ``--with PKG`` (repeatable), ``--with-requirements FILE`` (local path
  or fsspec URI, repeatable), and ``--python VERSION``. PEP 723 inline
  ``# /// script ... # ///`` blocks declaring ``dependencies = [...]``
  are honoured automatically and merged with ``--with`` flags.
  Resolution is delegated to ``uv pip install --target`` into a
  content-addressed cache (``$AWEBO_DEPS_CACHE`` or ``/tmp/awebo-deps``)
  with a ``.awebo-installed`` sentinel so reruns of the same dep set
  skip resolution. The runtime container now ships the ``uv`` binary so
  consumer images do not need to extend awebo just to add a few deps.

### Changed

- **README**: expanded container examples (AWS creds, `--with` flags, REST
  catalog / MinIO), added "Orchestrating jobs" section (cron, Fargate,
  EventBridge, Airflow, Step Functions).

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
