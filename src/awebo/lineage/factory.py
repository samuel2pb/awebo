"""Emitter auto-selection based on config."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from awebo.lineage.emitter import NoopEmitter

if TYPE_CHECKING:
    from awebo.config import LineageSettings
    from awebo.lineage.emitter import LineageEmitter

log = logging.getLogger(__name__)


def create_emitter(settings: LineageSettings) -> LineageEmitter:
    if not settings.enabled:
        log.debug("lineage disabled")
        return NoopEmitter()

    emitters: list[LineageEmitter] = []

    if settings.url is not None:
        from awebo.lineage.emitter import HttpEmitter

        log.info("lineage HTTP emitter → %s", settings.url)
        emitters.append(HttpEmitter(settings.url, api_key=settings.api_key))

    if settings.s3_enabled and settings.s3_bucket is not None:
        from awebo.lineage.s3 import S3Emitter

        log.info("lineage S3 emitter → s3://%s/%s", settings.s3_bucket, settings.s3_prefix)
        emitters.append(S3Emitter(bucket=settings.s3_bucket, prefix=settings.s3_prefix))

    if not emitters:
        log.warning("lineage enabled but no emitter configured — falling back to NoopEmitter")
        return NoopEmitter()

    if len(emitters) == 1:
        return emitters[0]

    from awebo.lineage.composite import CompositeEmitter

    return CompositeEmitter(emitters)
