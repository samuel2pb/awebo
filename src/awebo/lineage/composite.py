"""Composite emitter — fans out to multiple child emitters."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from openlineage.client.run import RunEvent

    from awebo.lineage.emitter import LineageEmitter

log = logging.getLogger(__name__)


class CompositeEmitter:
    def __init__(self, emitters: Sequence[LineageEmitter]) -> None:
        self._emitters = list(emitters)

    def emit(self, event: RunEvent) -> None:
        for emitter in self._emitters:
            try:
                emitter.emit(event)
            except Exception:
                log.warning(
                    "child emitter %s failed",
                    type(emitter).__name__,
                    exc_info=True,
                )
