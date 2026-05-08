"""OpenLineage RunEvent construction helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from openlineage.client.run import Job, OutputDataset, Run, RunEvent, RunState

PRODUCER = "awebo"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def write_start(namespace: str, table: str, run_id: str) -> RunEvent:
    return RunEvent(
        eventType=RunState.START,
        eventTime=_now_iso(),
        run=Run(runId=run_id),
        job=Job(namespace=namespace, name=f"write_{table}"),
        producer=PRODUCER,
    )


def write_complete(namespace: str, table: str, run_id: str) -> RunEvent:
    return RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=_now_iso(),
        run=Run(runId=run_id),
        job=Job(namespace=namespace, name=f"write_{table}"),
        producer=PRODUCER,
        outputs=[OutputDataset(namespace=namespace, name=table)],
    )


def write_fail(namespace: str, table: str, run_id: str) -> RunEvent:
    return RunEvent(
        eventType=RunState.FAIL,
        eventTime=_now_iso(),
        run=Run(runId=run_id),
        job=Job(namespace=namespace, name=f"write_{table}"),
        producer=PRODUCER,
        outputs=[OutputDataset(namespace=namespace, name=table)],
    )
