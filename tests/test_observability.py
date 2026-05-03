"""Tests for the _observability module."""

from __future__ import annotations

import logging
import tracemalloc

import pytest

from awebo._observability import log_peak_memory, otel_span, start_memory_tracing, timed


def test_timed_logs_duration(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.timed")
    with caplog.at_level(logging.DEBUG, logger="test.timed"), timed(logger, "test op"):
        pass
    assert len(caplog.records) == 1
    assert "test op" in caplog.records[0].message


def test_timed_logs_on_exception(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.timed.exc")
    with (
        caplog.at_level(logging.DEBUG, logger="test.timed.exc"),
        pytest.raises(ValueError, match="boom"),
        timed(logger, "failing op"),
    ):
        raise ValueError("boom")
    assert len(caplog.records) == 1
    assert "failing op" in caplog.records[0].message


def test_timed_respects_level(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.timed.level")
    with (
        caplog.at_level(logging.INFO, logger="test.timed.level"),
        timed(logger, "info op", level=logging.INFO),
    ):
        pass
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.INFO


def test_log_peak_memory_noop_when_not_tracing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test.mem.noop")
    was_tracing = tracemalloc.is_tracing()
    if was_tracing:
        tracemalloc.stop()
    try:
        with caplog.at_level(logging.INFO, logger="test.mem.noop"):
            log_peak_memory(logger)
        assert len(caplog.records) == 0
    finally:
        if was_tracing:
            tracemalloc.start()


def test_log_peak_memory_reports_when_tracing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test.mem.report")
    was_tracing = tracemalloc.is_tracing()
    if not was_tracing:
        tracemalloc.start(1)
    try:
        with caplog.at_level(logging.INFO, logger="test.mem.report"):
            log_peak_memory(logger)
        assert len(caplog.records) == 1
        assert "peak=" in caplog.records[0].message
    finally:
        if not was_tracing:
            tracemalloc.stop()


def test_start_memory_tracing_starts() -> None:
    was_tracing = tracemalloc.is_tracing()
    if was_tracing:
        tracemalloc.stop()
    try:
        start_memory_tracing()
        assert tracemalloc.is_tracing()
    finally:
        tracemalloc.stop()
        if was_tracing:
            tracemalloc.start()


def test_otel_span_noop_without_sdk() -> None:
    with otel_span("test.span", key="value"):
        pass
