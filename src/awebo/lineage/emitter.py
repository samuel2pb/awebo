"""Lineage emitter protocol and implementations.

``LineageEmitter`` is the seam: two+ adapters (Noop, Http, S3, Composite)
make it a real interface. Emission failures never break pipelines — all
``emit()`` calls are wrapped by callers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from openlineage.client.run import RunEvent

log = logging.getLogger(__name__)


class LineageEmitter(Protocol):
    def emit(self, event: RunEvent) -> None: ...


class NoopEmitter:
    def emit(self, event: RunEvent) -> None:
        pass


class HttpEmitter:
    def __init__(self, url: str, api_key: str | None = None) -> None:
        from openlineage.client.transport import HttpTransport
        from openlineage.client.transport.http import HttpConfig

        config_kwargs: dict[str, object] = {"url": url}
        if api_key is not None:
            from openlineage.client.transport.http import ApiKeyTokenProvider

            config_kwargs["auth"] = ApiKeyTokenProvider({"api_key": api_key})
        self._transport = HttpTransport(HttpConfig(**config_kwargs))  # type: ignore[arg-type]

    def emit(self, event: RunEvent) -> None:
        self._transport.emit(event)
