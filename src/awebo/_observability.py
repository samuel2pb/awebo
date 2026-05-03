"""Lightweight observability helpers.

Timing context manager, optional ``tracemalloc``, optional OpenTelemetry spans.
All zero-overhead when not enabled — library never configures handlers.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator


@contextmanager
def timed(
    logger: logging.Logger,
    msg: str,
    *,
    level: int = logging.DEBUG,
) -> Generator[None, None, None]:
    """Log wall-clock duration of the wrapped block."""
    t0 = time.perf_counter()
    try:
        yield
    finally:
        logger.log(level, "%s [%.3fs]", msg, time.perf_counter() - t0)


def start_memory_tracing() -> None:
    """Start ``tracemalloc`` with minimal overhead (nframes=1)."""
    import tracemalloc

    if not tracemalloc.is_tracing():
        tracemalloc.start(1)


def log_peak_memory(logger: logging.Logger) -> None:
    """Log current/peak memory if ``tracemalloc`` is active. No-op otherwise."""
    import tracemalloc

    if not tracemalloc.is_tracing():
        return
    current, peak = tracemalloc.get_traced_memory()
    logger.info(
        "memory: current=%.1fMB peak=%.1fMB",
        current / 1_048_576,
        peak / 1_048_576,
    )


# --------------- Optional OpenTelemetry (zero overhead when absent) ---------------

_OTEL_AVAILABLE: bool | None = None


@contextmanager
def otel_span(name: str, **attrs: str) -> Generator[None, None, None]:
    """Create an OpenTelemetry span if the SDK is installed, otherwise no-op."""
    global _OTEL_AVAILABLE  # noqa: PLW0603
    if _OTEL_AVAILABLE is False:
        yield
        return
    try:
        from opentelemetry import trace
    except ImportError:
        _OTEL_AVAILABLE = False
        yield
        return
    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer("awebo")
    with tracer.start_as_current_span(name, attributes=attrs):
        yield
