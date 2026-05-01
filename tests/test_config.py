from __future__ import annotations

import pytest

from awebo.config import DEFAULT_STREAMING_THRESHOLD_MB, load_config
from awebo.exceptions import ConfigError


@pytest.mark.usefixtures("clean_env")
def test_load_config_requires_catalog_type() -> None:
    with pytest.raises(ConfigError, match="AWEBO_CATALOG_TYPE"):
        load_config()


@pytest.mark.usefixtures("clean_env")
def test_load_config_glue_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "glue")
    monkeypatch.setenv("AWEBO_GLUE_WAREHOUSE", "s3://b/wh")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")

    cfg = load_config()

    assert cfg.catalog.type == "glue"
    assert cfg.catalog.name == "lake"  # default
    assert cfg.catalog.warehouse == "s3://b/wh"
    assert cfg.aws is not None
    assert cfg.aws.region == "eu-west-1"
    assert cfg.streaming_threshold_mb == DEFAULT_STREAMING_THRESHOLD_MB


@pytest.mark.usefixtures("clean_env")
def test_load_config_rejects_unknown_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "hive")
    with pytest.raises(ConfigError, match="unsupported"):
        load_config()


@pytest.mark.usefixtures("clean_env")
def test_load_config_rest_minimal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "rest")
    monkeypatch.setenv("AWEBO_REST_URI", "http://localhost:8181")
    monkeypatch.setenv("AWEBO_REST_WAREHOUSE", "s3://warehouse/")

    cfg = load_config()

    assert cfg.catalog.type == "rest"
    assert cfg.aws is None
    assert cfg.rest is not None
    assert cfg.rest.uri == "http://localhost:8181"
    assert cfg.rest.warehouse == "s3://warehouse/"
    assert cfg.s3 is not None
    assert cfg.s3.endpoint is None
    assert cfg.s3.path_style is False


@pytest.mark.usefixtures("clean_env")
def test_load_config_rest_minio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "rest")
    monkeypatch.setenv("AWEBO_REST_URI", "http://localhost:8181")
    monkeypatch.setenv("AWEBO_REST_WAREHOUSE", "s3://warehouse/")
    monkeypatch.setenv("AWEBO_REST_TOKEN", "tok")
    monkeypatch.setenv("AWEBO_S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("AWEBO_S3_PATH_STYLE", "true")
    monkeypatch.setenv("AWEBO_S3_ACCESS_KEY_ID", "minio")
    monkeypatch.setenv("AWEBO_S3_SECRET_ACCESS_KEY", "minio123")
    monkeypatch.setenv("AWEBO_S3_REGION", "us-east-1")

    cfg = load_config()

    assert cfg.rest is not None
    assert cfg.rest.token == "tok"
    assert cfg.s3 is not None
    assert cfg.s3.endpoint == "http://localhost:9000"
    assert cfg.s3.path_style is True
    assert cfg.s3.access_key_id == "minio"
    assert cfg.s3.region == "us-east-1"


@pytest.mark.usefixtures("clean_env")
def test_load_config_rest_requires_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "rest")
    monkeypatch.setenv("AWEBO_REST_WAREHOUSE", "s3://warehouse/")
    with pytest.raises(ConfigError, match="AWEBO_REST_URI"):
        load_config()


@pytest.mark.usefixtures("clean_env")
def test_load_config_rejects_negative_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "glue")
    monkeypatch.setenv("AWEBO_GLUE_WAREHOUSE", "s3://b/wh")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWEBO_STREAMING_THRESHOLD_MB", "0")
    with pytest.raises(ConfigError, match="positive"):
        load_config()
