from __future__ import annotations

from awebo.config import LineageSettings
from awebo.lineage.emitter import NoopEmitter
from awebo.lineage.factory import create_emitter


def test_disabled_returns_noop() -> None:
    settings = LineageSettings(enabled=False)
    emitter = create_emitter(settings)
    assert isinstance(emitter, NoopEmitter)


def test_enabled_without_url_returns_noop() -> None:
    settings = LineageSettings(enabled=True, url=None)
    emitter = create_emitter(settings)
    assert isinstance(emitter, NoopEmitter)


def test_enabled_with_url_returns_http() -> None:
    from awebo.lineage.emitter import HttpEmitter

    settings = LineageSettings(enabled=True, url="http://marquez:5000")
    emitter = create_emitter(settings)
    assert isinstance(emitter, HttpEmitter)


def test_enabled_with_url_and_api_key() -> None:
    from awebo.lineage.emitter import HttpEmitter

    settings = LineageSettings(enabled=True, url="http://marquez:5000", api_key="secret")
    emitter = create_emitter(settings)
    assert isinstance(emitter, HttpEmitter)


def test_s3_only_returns_s3_emitter() -> None:
    from awebo.lineage.s3 import S3Emitter

    settings = LineageSettings(enabled=True, s3_enabled=True, s3_bucket="my-bucket")
    emitter = create_emitter(settings)
    assert isinstance(emitter, S3Emitter)


def test_http_and_s3_returns_composite() -> None:
    from awebo.lineage.composite import CompositeEmitter

    settings = LineageSettings(
        enabled=True,
        url="http://marquez:5000",
        s3_enabled=True,
        s3_bucket="my-bucket",
    )
    emitter = create_emitter(settings)
    assert isinstance(emitter, CompositeEmitter)


def test_s3_enabled_without_bucket_returns_noop() -> None:
    settings = LineageSettings(enabled=True, s3_enabled=True, s3_bucket=None)
    emitter = create_emitter(settings)
    assert isinstance(emitter, NoopEmitter)
