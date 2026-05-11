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
    Catalog backend identifier. Supported: ``glue``, ``rest``, ``cloudflare``.

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

Iceberg REST (when ``AWEBO_CATALOG_TYPE=rest``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``AWEBO_REST_URI`` (required)
    Base URL of the Iceberg REST endpoint, e.g. ``http://localhost:8181``.
``AWEBO_REST_WAREHOUSE`` (required)
    Warehouse identifier the REST catalog uses, e.g. ``s3://warehouse/``.
``AWEBO_REST_TOKEN``
    Bearer token if the catalog is auth-protected.
``AWEBO_REST_CREDENTIAL``
    OAuth2 client credential (``client_id:client_secret``).

S3-compatible object store (used by the REST catalog backend; all optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``AWEBO_S3_ENDPOINT``
    S3-compatible endpoint (MinIO, etc.), e.g. ``http://localhost:9000``.
``AWEBO_S3_PATH_STYLE``
    Forces path-style addressing (required by MinIO). Truthy values:
    ``1``, ``true``, ``yes`` (case-insensitive).
``AWEBO_S3_ACCESS_KEY_ID`` / ``AWEBO_S3_SECRET_ACCESS_KEY``
    Object-store credentials, independent of AWS Glue credentials.
``AWEBO_S3_REGION``
    Region the S3 client should advertise (some MinIO deployments require
    any non-empty value).

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

_TRUTHY: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})


@dataclass(frozen=True, slots=True)
class AwsSettings:
    region: str
    profile: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    role_arn: str | None = None


@dataclass(frozen=True, slots=True)
class S3Settings:
    """Object-store credentials for non-AWS S3 backends (MinIO, Ceph, ...)."""

    endpoint: str | None = None
    path_style: bool = False
    access_key_id: str | None = None
    secret_access_key: str | None = None
    region: str | None = None


@dataclass(frozen=True, slots=True)
class RestSettings:
    """Iceberg REST catalog connection details."""

    uri: str
    warehouse: str
    token: str | None = None
    oauth_credential: str | None = None


@dataclass(frozen=True, slots=True)
class LineageSettings:
    enabled: bool = False
    url: str | None = None
    api_key: str | None = None
    namespace: str = "awebo"
    s3_bucket: str | None = None
    s3_prefix: str = "openlineage/events"
    s3_enabled: bool = False


@dataclass(frozen=True, slots=True)
class CloudflareSettings:
    """Cloudflare R2 datalake connection details."""

    account_id: str
    r2_access_key_id: str
    r2_secret_access_key: str
    bucket: str
    api_token: str | None = None
    jurisdiction: str | None = None
    catalog_endpoint: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogSettings:
    type: str
    name: str
    warehouse: str | None = None
    properties: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LakehouseConfig:
    catalog: CatalogSettings
    aws: AwsSettings | None = None
    s3: S3Settings | None = None
    rest: RestSettings | None = None
    cloudflare: CloudflareSettings | None = None
    streaming_threshold_mb: int = DEFAULT_STREAMING_THRESHOLD_MB
    lineage: LineageSettings = field(default_factory=LineageSettings)
    temp_dir: str | None = None
    trace_memory: bool = False


def _require(env: str) -> str:
    value = os.getenv(env)
    if not value:
        raise ConfigError(f"required env var {env!r} is not set")
    return value


def _bool(env: str, default: bool = False) -> bool:
    raw = os.getenv(env)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


def load_config() -> LakehouseConfig:
    """Resolve a :class:`LakehouseConfig` from the current process environment.

    All env vars are read exactly once here; downstream code receives a frozen
    dataclass so the rest of the library never touches ``os.environ``.
    """
    catalog_type = _require("AWEBO_CATALOG_TYPE").lower()
    catalog_name = os.getenv("AWEBO_CATALOG_NAME", "lake")

    aws: AwsSettings | None = None
    s3: S3Settings | None = None
    rest: RestSettings | None = None
    cloudflare: CloudflareSettings | None = None
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
    elif catalog_type == "rest":
        rest = RestSettings(
            uri=_require("AWEBO_REST_URI"),
            warehouse=_require("AWEBO_REST_WAREHOUSE"),
            token=os.getenv("AWEBO_REST_TOKEN"),
            oauth_credential=os.getenv("AWEBO_REST_CREDENTIAL"),
        )
        catalog_warehouse = rest.warehouse
        s3 = S3Settings(
            endpoint=os.getenv("AWEBO_S3_ENDPOINT"),
            path_style=_bool("AWEBO_S3_PATH_STYLE"),
            access_key_id=os.getenv("AWEBO_S3_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWEBO_S3_SECRET_ACCESS_KEY"),
            region=os.getenv("AWEBO_S3_REGION"),
        )
    elif catalog_type == "cloudflare":
        cf = CloudflareSettings(
            account_id=_require("AWEBO_CF_ACCOUNT_ID"),
            r2_access_key_id=_require("AWEBO_CF_R2_ACCESS_KEY_ID"),
            r2_secret_access_key=_require("AWEBO_CF_R2_SECRET_ACCESS_KEY"),
            bucket=_require("AWEBO_CF_R2_BUCKET"),
            api_token=os.getenv("AWEBO_CF_API_TOKEN"),
            jurisdiction=os.getenv("AWEBO_CF_JURISDICTION"),
            catalog_endpoint=os.getenv("AWEBO_CF_CATALOG_ENDPOINT"),
        )
        cloudflare = cf

        jur = f".{cf.jurisdiction}" if cf.jurisdiction else ""
        r2_host = f"https://{cf.account_id}{jur}.r2.cloudflarestorage.com"

        s3 = S3Settings(
            endpoint=r2_host,
            path_style=False,
            access_key_id=cf.r2_access_key_id,
            secret_access_key=cf.r2_secret_access_key,
            region=os.getenv("AWEBO_S3_REGION", "auto"),
        )
        rest = RestSettings(
            uri=cf.catalog_endpoint or r2_host,
            warehouse=f"s3://{cf.bucket}/",
            token=cf.api_token,
        )
        catalog_warehouse = f"s3://{cf.bucket}/"
    else:
        raise ConfigError(
            f"unsupported AWEBO_CATALOG_TYPE={catalog_type!r};"
            " supported: 'glue', 'rest', 'cloudflare'"
        )

    streaming_mb_raw = os.getenv("AWEBO_STREAMING_THRESHOLD_MB")
    streaming_mb = int(streaming_mb_raw) if streaming_mb_raw else DEFAULT_STREAMING_THRESHOLD_MB
    if streaming_mb <= 0:
        raise ConfigError("AWEBO_STREAMING_THRESHOLD_MB must be positive")

    lineage = LineageSettings(
        enabled=_bool("AWEBO_LINEAGE_ENABLED"),
        url=os.getenv("AWEBO_LINEAGE_URL"),
        api_key=os.getenv("AWEBO_LINEAGE_API_KEY"),
        namespace=os.getenv("AWEBO_LINEAGE_NAMESPACE", "awebo"),
        s3_bucket=os.getenv("AWEBO_LINEAGE_S3_BUCKET"),
        s3_prefix=os.getenv("AWEBO_LINEAGE_S3_PREFIX", "openlineage/events"),
        s3_enabled=_bool("AWEBO_LINEAGE_S3_ENABLED"),
    )

    return LakehouseConfig(
        catalog=CatalogSettings(
            type=catalog_type,
            name=catalog_name,
            warehouse=catalog_warehouse,
        ),
        aws=aws,
        s3=s3,
        rest=rest,
        cloudflare=cloudflare,
        lineage=lineage,
        streaming_threshold_mb=streaming_mb,
        temp_dir=os.getenv("AWEBO_TEMP_DIR"),
        trace_memory=_bool("AWEBO_TRACE_MEMORY"),
    )
