"""Tests for CompositeEmitter — fans out to multiple child emitters."""

from __future__ import annotations

from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

from awebo.lineage.composite import CompositeEmitter


def _make_event() -> RunEvent:
    return RunEvent(
        eventType=RunState.COMPLETE,
        eventTime="2024-06-15T12:00:00Z",
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="ns", name="j"),
        producer="test",
    )


class SpyEmitter:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)


class BrokenEmitter:
    def emit(self, event: RunEvent) -> None:
        raise RuntimeError("broken")


def test_dispatches_to_all_children() -> None:
    spy1 = SpyEmitter()
    spy2 = SpyEmitter()
    composite = CompositeEmitter([spy1, spy2])
    event = _make_event()
    composite.emit(event)

    assert len(spy1.events) == 1
    assert len(spy2.events) == 1
    assert spy1.events[0] is event


def test_isolates_child_failures() -> None:
    spy = SpyEmitter()
    composite = CompositeEmitter([BrokenEmitter(), spy])
    event = _make_event()
    composite.emit(event)

    assert len(spy.events) == 1


def test_empty_children_is_noop() -> None:
    composite = CompositeEmitter([])
    composite.emit(_make_event())
