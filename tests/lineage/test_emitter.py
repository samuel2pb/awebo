from __future__ import annotations

from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

from awebo.lineage.emitter import NoopEmitter


def _make_event(state: RunState = RunState.COMPLETE) -> RunEvent:
    return RunEvent(
        eventType=state,
        eventTime="2024-01-01T00:00:00Z",
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="test", name="test_job"),
        producer="awebo-test",
    )


def test_noop_emitter_discards_events() -> None:
    emitter = NoopEmitter()
    emitter.emit(_make_event(RunState.START))
    emitter.emit(_make_event(RunState.COMPLETE))
    emitter.emit(_make_event(RunState.FAIL))
