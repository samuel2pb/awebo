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
def test_load_config_rejects_negative_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "glue")
    monkeypatch.setenv("AWEBO_GLUE_WAREHOUSE", "s3://b/wh")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWEBO_STREAMING_THRESHOLD_MB", "0")
    with pytest.raises(ConfigError, match="positive"):
        load_config()
