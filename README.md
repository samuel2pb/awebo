<p align="center">
  <img src="./docs/img/logo.png" alt="awebo" width="240" />
</p>

# awebo

> DuckDB-powered Apache Iceberg data wrangler. PyIceberg + Arrow on the back, DuckDB + Polars on the front.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

`awebo` lets you treat an Iceberg lakehouse like a single ergonomic Python object.
Read tables with SQL, register files from S3, and write back to Iceberg — with
streaming for larger-than-memory datasets and zero D+1 API patterns.

## Quickstart

```python
from awebo import Lakehouse

lake = Lakehouse()  # config resolved from env vars

df = lake.sql("select * from lake.sales.orders").to_polars()

lake.sql("select * from lake.sales.orders").show()

lake.read_file("s3://bucket/raw/file.parquet", alias="raw")  # autodetects format

lake.to_lake(data=df, database="sales", table="orders_clean")

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
```

`awebo` ships expecting the DuckDB `iceberg` and `httpfs` extensions to be
pre-installed in your runtime image (Docker layer or wheel prebake). In dev
they are auto-loaded by DuckDB; in production bake them in to avoid the
download on cold start.

## Configuration

`Lakehouse()` resolves everything from environment variables:

| Variable | Required | Description |
| --- | --- | --- |
| `AWEBO_CATALOG_TYPE` | yes | Catalog backend. Supported: `glue`, `rest`. |
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
| `AWEBO_STREAMING_THRESHOLD_MB` | no (default `512`) | Auto-stream when sources exceed this size. |
| `AWEBO_TEMP_DIR` | no | DuckDB spill directory. |

### Write modes and explicit schemas

```python
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType

schema = Schema(
    NestedField(1, "ingested_at", TimestamptzType(), required=True),
    NestedField(2, "payload", StringType(), required=False),
)
spec = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="day"),
)

lake.to_lake(arrow_table, "bronze", "events", schema=schema, partition_spec=spec)
lake.to_lake(snapshot, "bronze", "cadastro", mode="overwrite")  # full-snapshot replace
```

Need direct PyIceberg access (snapshot rollback, manifest tables, schema
introspection)? Use the escape hatch:

```python
cat = lake.pyiceberg_catalog()
tbl = cat.load_table("bronze.events")
```

## Architecture

See [`ARCHITECTURE.md`](./ARCHITECTURE.md). High level:

```
                     Lakehouse (facade)
                           │
       ┌─────────┬─────────┼─────────┬───────────┐
       ▼         ▼         ▼         ▼           ▼
     auth/    catalog/   query/     io/      streaming/
   (AWS,…)   (Glue,…)  (DuckDB)  (file,    (Arrow batch
                                  iceberg)   helpers)
```

## Roadmap

- [x] AWS Glue + S3 catalog
- [x] Iceberg REST catalog (cloud-agnostic, MinIO-friendly) — write path via
      PyIceberg; SQL reads via `iceberg_scan('s3://...')` or
      `lake.pyiceberg_catalog()`. Catalog-qualified DuckDB ATTACH is
      pending upstream support in the DuckDB iceberg extension.
- [ ] GCP BigLake / Azure Tables auth providers
- [ ] Time-travel SQL helpers (`as of timestamp`, `as of version`)

## Development

```bash
uv sync --all-extras
make lint
make test
make test-prop    # Hypothesis suite
make test-mem     # pytest-memray suite
```

Always update [`CHANGELOG.md`](./CHANGELOG.md) before opening a PR.

## License

Apache 2.0. See [`LICENSE`](./LICENSE).
