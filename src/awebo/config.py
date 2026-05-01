"""Environment-variable based configuration.

awebo deliberately resolves catalog + auth from environment variables so the
``Lakehouse()`` zero-arg constructor "just works" inside containers, CI jobs
and notebooks alike. There is no YAML/TOML config file by design — keep the
runtime contract small and predictable.

Environment contract
--------------------

Required
~~~~~~~~
``AWEBO_CATALOG_TYPE``
    Catalog backend identifier. Currently only ``glue`` is supported.

``AWEBO_CATALOG_NAME``
    DuckDB ATTACH alias. Becomes the ``catalog`` token in
    ``select * from catalog.database.table``.

Glue + S3 (when ``AWEBO_CATALOG_TYPE=glue``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``AWEBO_GLUE_WAREHOUSE``
    S3 URI used as default warehouse for new tables (e.g. ``s3://bucket/wh``).
``AWS_REGION`` (or ``AWS_DEFAULT_REGION``)
    AWS region for Glue + S3 calls.
``AWS_PROFILE`` *or* ``AWS_ACCESS_KEY_ID`` + ``AWS_SECRET_ACCESS_KEY``
    Standard boto3 credential resolution applies. ``AWEBO_AWS_ROLE_ARN`` may
    be set to assume a role on top of the base credentials.

Tunables
~~~~~~~~
``AWEBO_STREAMING_THRESHOLD_MB`` (default ``512``)
    When a write source exceeds this in-memory size, awebo switches to
    streaming Arrow ``RecordBatchReader`` writes that spill to disk via
    DuckDB's temp directory rather than materialising into memory.
``AWEBO_TEMP_DIR``
    Directory used by DuckDB for spill files. Defaults to system temp.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final

from awebo.exceptions import ConfigError

DEFAULT_STREAMING_THRESHOLD_MB: Final[int] = 512


@dataclass(frozen=True, slots=True)
class AwsSettings:
    region: str
    profile: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    role_arn: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogSettings:
    type: str
    name: str
    warehouse: str | None = None
    properties: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LakehouseConfig:
    catalog: CatalogSettings
    aws: AwsSettings | None
    streaming_threshold_mb: int = DEFAULT_STREAMING_THRESHOLD_MB
    temp_dir: str | None = None


def _require(env: str) -> str:
    value = os.getenv(env)
    if not value:
        raise ConfigError(f"required env var {env!r} is not set")
    return value


def load_config() -> LakehouseConfig:
    """Resolve a :class:`LakehouseConfig` from the current process environment.

    All env vars are read exactly once here; downstream code receives a frozen
    dataclass so the rest of the library never touches ``os.environ``.
    """
    catalog_type = _require("AWEBO_CATALOG_TYPE").lower()
    catalog_name = os.getenv("AWEBO_CATALOG_NAME", "lake")

    aws: AwsSettings | None = None
    catalog_warehouse: str | None = None
    if catalog_type == "glue":
        region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if not region:
            raise ConfigError("AWS_REGION (or AWS_DEFAULT_REGION) required for glue catalog")
        aws = AwsSettings(
            region=region,
            profile=os.getenv("AWS_PROFILE"),
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            session_token=os.getenv("AWS_SESSION_TOKEN"),
            role_arn=os.getenv("AWEBO_AWS_ROLE_ARN"),
        )
        catalog_warehouse = os.getenv("AWEBO_GLUE_WAREHOUSE")
        if not catalog_warehouse:
            raise ConfigError("AWEBO_GLUE_WAREHOUSE required for glue catalog")
    else:
        raise ConfigError(f"unsupported AWEBO_CATALOG_TYPE={catalog_type!r}; supported: 'glue'")

    streaming_mb_raw = os.getenv("AWEBO_STREAMING_THRESHOLD_MB")
    streaming_mb = int(streaming_mb_raw) if streaming_mb_raw else DEFAULT_STREAMING_THRESHOLD_MB
    if streaming_mb <= 0:
        raise ConfigError("AWEBO_STREAMING_THRESHOLD_MB must be positive")

    return LakehouseConfig(
        catalog=CatalogSettings(
            type=catalog_type,
            name=catalog_name,
            warehouse=catalog_warehouse,
        ),
        aws=aws,
        streaming_threshold_mb=streaming_mb,
        temp_dir=os.getenv("AWEBO_TEMP_DIR"),
    )
