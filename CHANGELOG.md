# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (breaking) — DuckDB 2.0 native Iceberg read + write

- **DuckDB `>=2.0,<3`** (was `>=1.1,<2`). The 2.0 typing pass is done:
  Arrow / Polars conversions are typed, `mypy --strict` is clean.
- **Writes go through DuckDB, not PyIceberg.** `IcebergWriter` now issues
  `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE ... PARTITIONED BY ... WITH
  (...)`, `INSERT INTO ... BY NAME`, `ALTER TABLE ADD COLUMN / ALTER COLUMN
  TYPE / SET PARTITIONED BY` against the attached catalog. PyIceberg
  `Schema` / `PartitionSpec` arguments are still accepted and translated to
  DDL (`io/ddl.py`). Consequences:
  - `overwrite` is one `DELETE` + `INSERT` transaction (one atomic catalog
    commit, two snapshots) and is partition-scoped for **every** transform
    (identity, year/month/day/hour, bucket, truncate) and every source,
    including one-shot `RecordBatchReader`s (previously full replacement).
    NULL partition values are matched too.
  - DuckDB writes merge-on-read positional deletes; compact with
    `CALL iceberg_rewrite_data_files(...)`.
  - Partition field names are chosen by DuckDB (`identity_year_1`), not
    taken from `PartitionField.name`. Partition evolution compares
    (transform, source column), so tables created by PyIceberg are not
    re-partitioned spuriously.
  - Schema evolution: new columns added; lossless widenings only
    (int → long, float → double, decimal precision).
  - Lossy writes are rejected with `WriteError` ("incompatible column
    types") instead of being cast silently: a `double` into a `long`
    column, an integer into a `string` column, a `timestamp_ns` into a
    `timestamp` column. Narrower integers, `float` → `double`, decimal
    precision and all-NULL columns are still accepted. (PyIceberg rejected
    these too.)
  - Required (`NestedField(required=True)`) fields are created `NOT NULL`
    and stay required in the Iceberg schema.
  - Temporal partition overwrite (`year`/`month`/`day`/`hour`) on
    `timestamptz` columns buckets by UTC, as Iceberg does, whatever the
    session `TimeZone`. Before this fix a non-UTC host could delete rows
    from the wrong day.
- **REST and Cloudflare R2 catalogs are attached in DuckDB.**
  `ATTACH '<warehouse>' (TYPE iceberg, URI ...)` with a `TYPE iceberg`
  secret (`TOKEN`, or OAuth2 `CLIENT_ID` / `CLIENT_SECRET` from
  `AWEBO_REST_CREDENTIAL`), or `AUTHORIZATION_TYPE 'none'`.
  Catalog-qualified SQL (`select ... from lake.ns.tbl`) now works on every
  backend; the `iceberg_scan('s3://...')` workaround is no longer needed.
- **Glue in China regions** (`cn-*`) attaches via explicit
  `URI 'glue.<region>.amazonaws.com.cn/iceberg'` + `AUTHORIZATION_TYPE
  'sigv4'`; `ENDPOINT_TYPE 'glue'` builds the wrong host there. New Glue
  tables are placed at `<warehouse>/<ns>.db/<table>` (PyIceberg's layout).
- **Removed `AWEBO_STREAMING_THRESHOLD_MB`, `LakehouseConfig.streaming_threshold_mb`
  and the `awebo.streaming` package.** DuckDB streams every source and
  spills to disk, so there is no second path to choose. New
  `AWEBO_DUCKDB_MEMORY_LIMIT` / `LakehouseConfig.memory_limit` sets
  DuckDB's `memory_limit`.
- **Removed `CatalogBackend.ensure_namespace`** (namespaces are created
  with `CREATE SCHEMA IF NOT EXISTS`). Added `CatalogBackend.table_location`.
  `IcebergWriter` no longer takes `credentials` / `streaming_threshold_mb`.
- `FileReader` uses `read_csv` / `read_json` (the `*_auto` aliases are
  legacy) and quotes paths and aliases. `.arrow` is no longer mapped to
  `read_parquet` (it never could read Arrow IPC).
- All secret / ATTACH values are SQL-quoted (`awebo/_sql.py`); a quote in a
  credential no longer breaks the statement.
- `QueryResult.to_polars()` uses DuckDB's native `.pl()`;
  `QueryResult.batches()` defaults to DuckDB 2.0's 131 072-row batches.
  Relations are lazy SQL in DuckDB 2.0: each conversion re-runs the query.
- `health_check()` checks the catalog through DuckDB (`duckdb_schemas()`).

### Added

- **CI workflow** (`.github/workflows/ci.yml`): `make check`, unit +
  pipeline, property, memory and moto integration tests on every PR and
  push to `main`. Read-only token, no secrets, so fork PRs run it safely.
- `Lakehouse.to_lake(..., properties={...})`: Iceberg table properties, set
  on create (`WITH (...)`) and synced onto existing tables
  (`set_iceberg_table_properties`).
- `pl.LazyFrame` is accepted as a write source (scanned on demand).
- Native Iceberg SQL documented: time travel (`AT (VERSION => ...)`),
  `iceberg_snapshots`, `iceberg_partition_specs`,
  `iceberg_rollback_to_snapshot`, `iceberg_rewrite_data_files`.
- Test fixture `tests/_fixtures/rest_catalog.py`: an in-process Iceberg
  REST catalog (PyIceberg `SqlCatalog` over SQLite) so DuckDB writes are
  tested hermetically, without Docker.
- Dockerfile pre-installs DuckDB's `avro` extension (iceberg dependency).

### Added

- **Public-repo hardening**: `publish-ghcr.yml` publishes the container
  image to `ghcr.io/samuel2pb/awebo` using only `GITHUB_TOKEN`;
  `secret-scan.yml` and a `gitleaks` pre-commit hook scan every commit;
  `SECURITY.md` documents vulnerability reporting. `publish-ecr.yml` now
  masks the registry host (AWS account ID) in workflow logs.
- **`docs/COSTS.md`**: line-by-line cost model for running pipelines on
  awebo + tundra (≈ 2¢/day for a daily job, < $1/day for an hourly job
  over 100 GB), with unit prices and sources. README gains a "What it
  costs" section.
- **GitHub OIDC for ECR publish**: `publish-ecr.yml` assumes
  `vars.AWS_OIDC_ROLE_ARN`, with no static-key fallback;
  `scripts/github-oidc-bootstrap.sh` creates the OIDC provider,
  role and ECR push policy.

### Fixed

- **Transform-only partition overwrite now scoped (`#27`)**: `days()`,
  `months()`, `years()`, and `hours()` partition specs no longer return
  `AlwaysTrue()` on overwrite. Both materialised and streaming paths
  now build a `[min, max)` range filter from the data, replacing only
  the touched temporal partitions instead of wiping the entire table.

- **Empty arrow overwrite no longer wipes table (`#25`)**: `_overwrite`
  now short-circuits when the materialised arrow table has zero rows
  (logs warning, returns). `_streaming_overwrite` abandons the
  transaction when the source yields zero rows, so the delete never
  commits. Prevents silent data loss when a pipeline iteration produces
  an empty result (e.g. header-only CSV).

### Added

- **Cloudflare R2 datalake support (`#21`)**: new
  `AWEBO_CATALOG_TYPE=cloudflare` backend. Set `AWEBO_CF_ACCOUNT_ID`,
  `AWEBO_CF_R2_ACCESS_KEY_ID`, `AWEBO_CF_R2_SECRET_ACCESS_KEY`, and
  `AWEBO_CF_R2_BUCKET` to auto-derive R2 S3 endpoint and REST catalog
  URI. Optional `AWEBO_CF_JURISDICTION` (`eu`, `fedramp`) selects
  jurisdiction-specific endpoints. Optional `AWEBO_CF_API_TOKEN` sets
  REST catalog bearer auth. Optional `AWEBO_CF_CATALOG_ENDPOINT`
  overrides auto-derived REST URI. Internally delegates to
  `S3AuthProvider` + `RestCatalogBackend` — no new protocol
  implementations needed since R2 is S3-compatible.

### Changed

- **Dependency refresh (`uv lock --upgrade`)**: clears all 78 Dependabot
  alerts on transitive packages (aiohttp, anyio, certifi, cryptography,
  filelock, fsspec, idna, multidict, oauthlib, opentelemetry-api, pyasn1,
  urllib3, virtualenv, Werkzeug). PyIceberg moves to 0.12, which no longer
  pulls `pyiceberg-core` by default, so the `pyiceberg-core` extra is now a
  declared dependency (partition transforms need it). `ruff` stays at
  0.15.12 to match the pre-commit hook.

- **Quality module internals refactored for depth and testability:**
  - `_require_threshold()` now returns validated `CheckThreshold` instead
    of `None`, eliminating 7 `assert` statements used for type narrowing.
    Safe under `python -O`.
  - `QualitySuite.checks` narrowed from `tuple | list` to `tuple` (accepts
    any `Sequence` at construction, coerces to tuple). Frozen semantics
    fully enforced.
  - `DuckDBSQLRunner` compilers are now lazily instantiated per-dialect
    instead of eagerly creating all three at init time. Module-level
    compiler imports removed from `runner.py`.
  - Compiler unit tests (`test_compiler.py`) replaced by behavioral
    integration tests through `DuckDBSQLRunner.run()` in
    `test_dialect_integration.py` — all edge cases migrated, tests now
    survive internal refactors.
  - Shared `quality_engine` / `quality_runner` fixtures extracted to
    `tests/quality/conftest.py`; duplicate warning-severity test removed.

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
  checks against DuckDB with three dialects selectable via
  `QualitySuite(dialect=...)`:
  - **`"sql"`** (default): raw SQL returning violating rows (0 = pass).
  - **`"soda"`**: SodaCL-style expressions (`missing_count(col) = 0`,
    `avg(col) between N and M`, `freshness(col) < 1d`, etc.) compiled to
    DuckDB SQL. Supports `row_count`, `missing_count/percent`,
    `duplicate_count/percent`, `min`, `max`, `avg`, `sum`, `stddev`,
    `freshness` with time units.
  - **`"openmetadata"`**: OpenMetadata test types (`columnValuesToBeNotNull`,
    `columnValuesToBeUnique`, `columnValuesToBeBetween`,
    `tableRowCountToEqual/ToBeBetween`, `columnValueLengthsToBeBetween`,
    `columnValuesSumToBeBetween`) with `CheckThreshold` parameters.

  Check results now include `metric_value` and `metric_name` for richer
  observability. Warning-severity failures don't fail the suite. Each
  check is timed. On completion, emits an OpenLineage
  `DataQualityAssertionsDatasetFacet` via the configured `LineageEmitter`.

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
