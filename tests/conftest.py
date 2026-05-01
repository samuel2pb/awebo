"""Shared pytest fixtures."""

from __future__ import annotations

import os

import pytest


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear AWEBO_* and AWS_* env vars so config tests start from a known state."""
    for key in list(os.environ):
        if key.startswith(("AWEBO_", "AWS_")):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture
def glue_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWEBO_CATALOG_TYPE", "glue")
    monkeypatch.setenv("AWEBO_CATALOG_NAME", "test_lake")
    monkeypatch.setenv("AWEBO_GLUE_WAREHOUSE", "s3://test-bucket/warehouse")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testsecret")
