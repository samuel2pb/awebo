from __future__ import annotations

import pytest

from awebo.config import load_config


@pytest.mark.usefixtures("clean_env")
def test_lineage_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "glue")
    monkeypatch.setenv("AWEBO_GLUE_WAREHOUSE", "s3://b/wh")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")

    cfg = load_config()

    assert cfg.lineage is not None
    assert cfg.lineage.enabled is False
    assert cfg.lineage.url is None
    assert cfg.lineage.namespace == "awebo"


@pytest.mark.usefixtures("clean_env")
def test_lineage_enabled_with_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "glue")
    monkeypatch.setenv("AWEBO_GLUE_WAREHOUSE", "s3://b/wh")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWEBO_LINEAGE_ENABLED", "true")
    monkeypatch.setenv("AWEBO_LINEAGE_URL", "http://marquez:5000")
    monkeypatch.setenv("AWEBO_LINEAGE_API_KEY", "secret")
    monkeypatch.setenv("AWEBO_LINEAGE_NAMESPACE", "myns")

    cfg = load_config()

    assert cfg.lineage is not None
    assert cfg.lineage.enabled is True
    assert cfg.lineage.url == "http://marquez:5000"
    assert cfg.lineage.api_key == "secret"
    assert cfg.lineage.namespace == "myns"


@pytest.mark.usefixtures("clean_env")
def test_lineage_s3_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "glue")
    monkeypatch.setenv("AWEBO_GLUE_WAREHOUSE", "s3://b/wh")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWEBO_LINEAGE_ENABLED", "true")
    monkeypatch.setenv("AWEBO_LINEAGE_S3_ENABLED", "true")
    monkeypatch.setenv("AWEBO_LINEAGE_S3_BUCKET", "my-lineage-bucket")
    monkeypatch.setenv("AWEBO_LINEAGE_S3_PREFIX", "custom/prefix")

    cfg = load_config()

    assert cfg.lineage.s3_enabled is True
    assert cfg.lineage.s3_bucket == "my-lineage-bucket"
    assert cfg.lineage.s3_prefix == "custom/prefix"
