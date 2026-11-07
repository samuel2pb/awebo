<p align="center">
  <img src="./docs/img/logo.png" alt="awebo" width="480" />
</p>

<p align="center">
  <em>DuckDB-powered Apache Iceberg data wrangler.</em><br/>
  DuckDB 2 reads and writes Iceberg; Arrow + Polars on the front.
</p>

<p align="center">
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.12+-blue.svg" alt="Python 3.12+"></a>
  <a href="https://github.com/samuel2pb/awebo/releases"><img src="https://img.shields.io/github/v/release/samuel2pb/awebo?include_prereleases" alt="Release"></a>
</p>

---

`awebo` treats an Iceberg lakehouse as a single ergonomic Python object.
Read with SQL, register external files, write back to Iceberg — all
through one DuckDB connection, with bounded memory (DuckDB spills to disk)
and zero D+1 API patterns.

## Why awebo

- **One engine, end to end.** DuckDB 2's native Iceberg support runs reads
  *and* writes (`CREATE TABLE`, `INSERT`, `DELETE`, `ALTER TABLE`) against
  the attached catalog. Arrow and Polars are scanned zero-copy on the way
  in; Polars is the dataframe handoff on the way out.
- **Multi-cloud catalogs.** AWS Glue, Cloudflare R2 Data Catalog, or any
  Iceberg REST catalog (Lakekeeper, Nessie, Polaris) including
  MinIO-backed local stacks. Catalog-qualified SQL works on all of them.
- **Bounded memory by default.** DuckDB pulls `RecordBatchReader` /
  `LazyFrame` sources on demand and spills to disk above its
  `memory_limit`; SQL-driven writes are never materialised in Python.
- **Container-ready job runner.** Ship a `.py`, a directory, or a zipapp;
  resolve runtime deps from CLI flags, requirements files, or PEP 723
  inline metadata. Consumers don't need to publish their own image.
- **Cents-a-day lakehouse.** No managed clusters spinning idle, no
  per-compute-unit billing, no heavyweight runtimes that charge you to
  sit around waiting for work. awebo runs as a short-lived Fargate task
  (0.5 vCPU / 1 GB, spot by default) reading and writing plain S3 +
  Iceberg — you pay for the seconds your job actually runs and the
  storage it touches. A daily pipeline lands at **about 2 cents a day**;
  an hourly pipeline over 100 GB stays **under a dollar a day**. Full
  breakdown with list prices in [`docs/COSTS.md`](./docs/COSTS.md).
- **Pairs with [tundra](https://github.com/samuel2pb/tundra)** for the
  Terraform substrate underneath — Fargate cluster, IAM, VPC, and the
  S3 zones (`raw` / `sot` / `smt`) that Iceberg writes into. Together
  they are a complete, open framework for anyone who wants efficient,
  extremely cheap data pipelines without a platform team.

## What it costs

| Workload | Per day | Per month |
| --- | ---: | ---: |
| 1 daily job, 0.5 vCPU / 1 GB, 5 min on Spot, 10 GB lake | ≈ $0.02 | ≈ $0.53 |
| 24 hourly jobs, 2 vCPU / 4 GB, 10 min on Spot, 100 GB lake | ≈ $0.89 | ≈ $27 |
| Daily job on GitHub Actions `schedule` + Cloudflare R2 free tier | $0 | $0 |

us-east-1 list prices, November 2026. Compute is under 10% of the bill;
S3 requests and storage are the rest. See [`docs/COSTS.md`](./docs/COSTS.md)
for the line-by-line model, the unit prices, and how to spend less.

## Quickstart

```python
from awebo import Lakehouse

lake = Lakehouse()  # config resolved from env vars

# Read
df = lake.sql("select * from lake.sales.orders").to_polars()
lake.sql("select * from lake.sales.orders").show()

# Register an external file (format auto-detected by suffix)
lake.read_file("s3://bucket/raw/file.parquet", alias="raw")

# Write a DataFrame
lake.to_lake(data=df, database="sales", table="orders_clean")

# Or write the result of a SQL query directly (streamed, never materialised)
lake.to_lake(
    data="select value, date from lake.sales.orders group by date",
    database="sales",
    table="daily_value",
)
```

## Installation

```bash
uv add awebo            # production
uv add "awebo[dev]"     # contributors
uv add "awebo[runner]"  # adds gcsfs for gs:// targets in the job runner
uv add "awebo[otel]"    # adds opentelemetry-api for OTel span export
```

awebo needs DuckDB >= 2.0. The DuckDB `iceberg` (plus its `avro`
dependency) and `httpfs` extensions are required at runtime. In dev DuckDB
auto-installs them on first use; in production bake them into your image
to avoid the cold-start download (the published container image already
does this).

## Configuration

`Lakehouse()` resolves everything from environment variables:

| Variable | Required | Description |
| --- | --- | --- |
| `AWEBO_CATALOG_TYPE` | yes | Catalog backend. Supported: `glue`, `rest`, `cloudflare`. |
| `AWEBO_CATALOG_NAME` | no (default `lake`) | DuckDB ATTACH alias used in SQL. |
| `AWEBO_GLUE_WAREHOUSE` | yes (glue) | S3 warehouse URI for new tables. |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | yes (glue) | AWS region. |
| `AWS_PROFILE` *or* `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` | yes (glue) | Standard boto3 chain. |
| `AWEBO_AWS_ROLE_ARN` | no | If set, awebo assumes this role on top of base creds. |
| `AWEBO_REST_URI` | yes (rest) | Iceberg REST endpoint, e.g. `http://localhost:8181`. |
| `AWEBO_REST_WAREHOUSE` | yes (rest) | Warehouse identifier the REST catalog uses. |
| `AWEBO_REST_TOKEN` | no | Bearer token for protected REST catalogs. |
| `AWEBO_REST_CREDENTIAL` | no | OAuth2 `client_id:client_secret`. |
| `AWEBO_S3_ENDPOINT` | no | S3-compatible endpoint (MinIO, Ceph). |
| `AWEBO_S3_PATH_STYLE` | no | `true` to force path-style addressing (MinIO). |
| `AWEBO_S3_ACCESS_KEY_ID` / `AWEBO_S3_SECRET_ACCESS_KEY` | no | Object-store creds for non-AWS S3. |
| `AWEBO_S3_REGION` | no | Region the S3 client should advertise. |
| `AWEBO_DUCKDB_MEMORY_LIMIT` | no | DuckDB `memory_limit` (e.g. `768MB`); DuckDB spills above it. |
| `AWEBO_TEMP_DIR` | no | DuckDB spill directory (`temp_directory`). |
| `AWEBO_TRACE_MEMORY` | no | `1` to enable `tracemalloc` peak memory reporting on writes. |

Copy [`.env.example`](./.env.example) to `.env` and fill in the values
you need.

### Write modes and explicit schemas

```python
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import NestedField, LongType, StringType, TimestamptzType

schema = Schema(
    NestedField(1, "ingested_at", TimestamptzType(), required=True),
    NestedField(2, "payload", StringType(), required=False),
)
spec = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="day"),
)

# Honoured on create; partition_spec triggers evolution if table already exists.
# `properties` become Iceberg table properties (set on create, synced after).
lake.to_lake(
    arrow_table, "bronze", "events",
    schema=schema, partition_spec=spec,
    properties={"write.parquet.compression-codec": "zstd"},
)

# Dynamic partition overwrite — only replaces partitions present in the data,
# for every transform (identity, year/month/day/hour, bucket, truncate).
# Unpartitioned tables get a full replacement. One atomic catalog commit.
lake.to_lake(snapshot, "bronze", "cadastro", mode="overwrite")
```

awebo translates `schema` / `partition_spec` into DuckDB DDL
(`CREATE TABLE ... PARTITIONED BY (...)`). DuckDB names partition fields
itself (e.g. `day_ingested_at_1`); the `name` on a `PartitionField` is not
kept. DuckDB writes merge-on-read positional deletes, so frequent
overwrites accumulate delete files; compact with
`lake.sql("CALL iceberg_rewrite_data_files('lake.bronze.cadastro')")`.

### Native Iceberg SQL

```python
lake.sql("SELECT * FROM lake.bronze.events AT (VERSION => 123456789)")   # time travel
lake.sql("SELECT * FROM iceberg_snapshots('lake.bronze.events')")         # history
lake.sql("SELECT * FROM iceberg_partition_specs('lake.bronze.events')")   # partitioning
lake.sql("CALL iceberg_rollback_to_snapshot('lake.bronze.events', 123)") # rollback
```

### Schema evolution and type widening

Schema evolution is enabled by default — `to_lake` automatically adds new
columns (`ALTER TABLE ADD COLUMN`) and applies lossless widenings
(`int32` → `int64`, `float` → `double`, decimal precision) with
`ALTER COLUMN TYPE`. To reject new columns instead:

```python
lake.to_lake(df, "bronze", "events", schema_evolution=False)
```

### Observability

awebo emits structured log events via Python's stdlib `logging` — no
handlers configured by default; wire them up in your application:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

lake = Lakehouse()
lake.to_lake(df, "bronze", "events")
# INFO  [awebo.lakehouse] lakehouse ready: catalog=glue name=lake
# INFO  [awebo.io.writer] append bronze.events [0.423s]
```

Enable `tracemalloc` for memory diagnostics (e.g. debugging Fargate OOMs):

```bash
export AWEBO_TRACE_MEMORY=1
```

```
# INFO  [awebo.io.writer] memory: current=12.3MB peak=45.6MB
# INFO  [awebo.io.writer] append bronze.events [0.423s]
```

Quick connectivity check:

```python
lake.health_check()  # {"duckdb": "ok", "catalog": "ok"}
```

If `opentelemetry-api` is installed (`pip install awebo[otel]`), write
and query operations automatically emit OTel spans — zero overhead when
the SDK is absent.

Need PyIceberg for maintenance with no SQL verb yet (snapshot expiry,
orphan-file cleanup)? Use the escape hatch:

```python
cat = lake.pyiceberg_catalog()
tbl = cat.load_table("bronze.events")
```

## Container / job runner

`awebo` ships a thin entrypoint that runs a user-supplied Python job
against the configured `Lakehouse`. Three target shapes are supported:

```bash
# 1. single .py file
python -m awebo path/to/job.py arg1 arg2

# 2. a directory containing __main__.py + sibling modules (sibling imports
#    work because runpy puts the dir on sys.path)
python -m awebo ./jobs/my_pkg arg1 arg2

# 3. a zipapp .zip whose root contains __main__.py + helpers — same
#    sibling-import rules apply
python -m awebo ./dist/bundle.zip arg1 arg2
```

Files (`.py`, `.zip`) may also be given as an fsspec URI; awebo streams
them into a private temp dir, then executes in this interpreter:

```bash
python -m awebo s3://my-bucket/jobs/ingest.py
python -m awebo s3://my-bucket/jobs/bundle.zip
python -m awebo gs://my-bucket/jobs/ingest.py
python -m awebo https://example.com/job.zip
```

Remote *directories* are not supported (fsspec single-shot fetch). Zip
your bundle and upload that.

Trailing arguments are forwarded to the job as `sys.argv[1:]`. The
public container image is published to GitHub Container Registry by
[`publish-ghcr.yml`](.github/workflows/publish-ghcr.yml) on every push
to `main` (`latest`) and every `v*` tag:

```bash
docker pull ghcr.io/samuel2pb/awebo:latest
```

It uses the same entrypoint. (`publish-ecr.yml` is the maintainer's
private ECR mirror; set `AWS_ECR_REPO` to your own image if you fork.)

```bash
# Minimal: Glue catalog, AWS creds from env
docker run --rm \
  -e AWEBO_CATALOG_TYPE=glue \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  ghcr.io/samuel2pb/awebo:latest \
  s3://my-bucket/jobs/ingest.py

# With runtime deps and script args
docker run --rm \
  -e AWEBO_CATALOG_TYPE=glue \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  ghcr.io/samuel2pb/awebo:latest \
  --with httpx --with 'pydantic>=2.7' \
  s3://my-bucket/jobs/ingest.py --date 2024-01-15

# REST catalog (e.g. Lakekeeper + MinIO local stack)
docker run --rm \
  -e AWEBO_CATALOG_TYPE=rest \
  -e AWEBO_REST_URI=http://host.docker.internal:8181 \
  -e AWEBO_REST_WAREHOUSE=warehouse \
  -e AWEBO_S3_ENDPOINT=http://host.docker.internal:9000 \
  -e AWEBO_S3_ACCESS_KEY_ID=minioadmin \
  -e AWEBO_S3_SECRET_ACCESS_KEY=minioadmin \
  -e AWEBO_S3_PATH_STYLE=true \
  ghcr.io/samuel2pb/awebo:latest \
  ./jobs/transform.py
```

### Runtime dependencies

Jobs frequently need a few extra packages on top of the awebo runtime
(e.g. `httpx`, `pydantic`). Three ways to declare them — all merged
before the script runs:

```bash
# 1. repeatable --with flags
python -m awebo --with httpx --with 'pydantic>=2.7' job.py

# 2. pip-style requirements file (local path or fsspec URI)
python -m awebo --with-requirements s3://my-bucket/jobs/reqs.txt job.py

# 3. PEP 723 inline metadata at the top of the script
cat job.py
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.27", "pydantic>=2.7"]
# ///
python -m awebo job.py
```

Resolution is delegated to `uv pip install --target` into a
content-addressed cache (`$AWEBO_DEPS_CACHE`, default `/tmp/awebo-deps`),
so reruns of the same dep set skip resolution entirely. The container
image bundles `uv` for this — no consumer image extension needed just to
pull in a few packages.

## Orchestrating jobs

### Local: script + cron

The simplest deployment — a `.py` file, a cron schedule, and env vars:

```bash
# /etc/cron.d/awebo-daily-ingest
0 6 * * * APP_USER . /home/app/.env && python -m awebo /opt/jobs/ingest.py 2>&1 | logger -t awebo
```

Or run it from any scheduler that can execute a shell command (Airflow
`BashOperator`, GitHub Actions `schedule`, systemd timer).

### Fargate: zero-infra container tasks

With [tundra](https://github.com/samuel2pb/tundra) provisioned, trigger
awebo tasks on Fargate — no long-lived servers:

```bash
# One-off task via AWS CLI
aws ecs run-task \
  --cluster awebo \
  --task-definition awebo-job \
  --overrides '{
    "containerOverrides": [{
      "name": "awebo",
      "command": ["s3://my-bucket/jobs/ingest.py", "--date", "2024-01-15"]
    }]
  }'
```

Common orchestration patterns:

| Trigger | How |
| --- | --- |
| **Scheduled** | EventBridge rule → ECS RunTask |
| **Event-driven** | S3 notification → EventBridge → ECS RunTask |
| **Airflow** | `EcsRunTaskOperator` with `overrides` for the script path |
| **Step Functions** | `ecs:RunTask` action in a state machine |

The task definition created by tundra already sets `AWEBO_CATALOG_TYPE`,
`AWS_REGION`, and the warehouse vars via SSM — the only override needed
per job is the script path (and any `--with` flags or script args).

## Infrastructure

`awebo` is the **runtime** layer. The cloud substrate beneath it is
provisioned by [**tundra**](https://github.com/samuel2pb/tundra) — *"a
small, opinionated Terraform module library for spinning up the cloud
pieces of a Fargate-based data lake: S3 zones, operational buckets, a
minimal VPC, and an ECS cluster with task roles and task definitions."*

| Tundra module | What it gives awebo |
| --- | --- |
| `modules/aws/datalake-buckets` | `raw` / `sot` / `smt` S3 zones Iceberg writes into. |
| `modules/aws/platform-buckets` | tfstate / logs / artifacts / athena buckets for the platform itself. |
| `modules/aws/vpc` | Minimal `/27` VPC the Fargate tasks run in. |
| `modules/aws/iam-container-role` | Task role assumed by awebo containers (Glue + S3 perms). |
| `modules/aws/ecs-cluster` + `ecs-task-definition` | Fargate cluster + task def that run `python -m awebo <job>` against `ghcr.io/samuel2pb/awebo` (or your own ECR mirror from [`publish-ecr.yml`](.github/workflows/publish-ecr.yml)). |
| `modules/aws/main` | Composition that wires the rest together. Accepts `task_image` (point it at the awebo ECR URI), `task_environment`, and `task_secrets` so consumer Terraform doesn't have to wire each module manually. |

Pin a tundra version in the consumer's Terraform (latest tag is `v0.1.0`)
and call `terraform apply`; awebo handles everything from there.

```hcl
module "platform" {
  source     = "git::https://github.com/samuel2pb/tundra.git//modules/aws/main?ref=v0.1.0"
  task_image = "ghcr.io/samuel2pb/awebo:latest"
  # ... task_environment / task_secrets / account_id / account_name ...
}
```

## Architecture

See [`ARCHITECTURE.md`](./ARCHITECTURE.md). High level:

```
                     Lakehouse (facade)
                           │
       ┌─────────┬─────────┼─────────┬───────────┐
       ▼         ▼         ▼         ▼           ▼
     auth/    catalog/   query/     io/      lineage/ + quality/
   (AWS,…)   (Glue,…)  (DuckDB)  (file,
                                  iceberg)
```

| Concern | Engine |
| --- | --- |
| Read SQL | DuckDB + `iceberg` extension |
| Write / DDL | DuckDB + `iceberg` extension (attached catalog) |
| Data plane | Apache Arrow (zero-copy scans of Arrow / Polars sources) |
| User dataframe | Polars (returned by `QueryResult.to_polars()`) |

## Roadmap

- [x] AWS Glue + S3 catalog
- [x] Iceberg REST catalog (cloud-agnostic, MinIO-friendly) — attached in
      DuckDB for catalog-qualified reads and writes
- [x] Cloudflare R2 Data Catalog
- [x] Time travel (`AT (VERSION => ...)` / `AT (TIMESTAMP => ...)` via DuckDB)
- [ ] GCP BigLake / Azure Tables auth providers

## Contributing

Issues and PRs welcome. Quick loop:

```bash
uv sync --all-extras
make lint        # ruff
make test        # unit tests
make test-prop   # Hypothesis property suite
make test-mem    # pytest-memray suite
make check       # everything pre-commit runs
```

`pre-commit` hooks gate gitleaks, ruff, mypy strict, and the test matrix
on every commit. See [`SECURITY.md`](./SECURITY.md) for how to report
vulnerabilities. Update [`CHANGELOG.md`](./CHANGELOG.md) under `## [Unreleased]`
before opening a PR.

See [`CLAUDE.md`](./CLAUDE.md) for architecture invariants and the
where-to-make-changes map.

## License

Apache 2.0. See [`LICENSE`](./LICENSE).
