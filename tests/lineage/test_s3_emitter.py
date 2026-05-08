"""Tests for S3Emitter — writes OpenLineage events as JSON to S3."""

from __future__ import annotations

import json

import boto3
from moto import mock_aws
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

from awebo.lineage.s3 import S3Emitter


def _make_event(
    state: RunState = RunState.COMPLETE,
    run_id: str | None = None,
    namespace: str = "myns",
    job_name: str = "write_db.tbl",
) -> RunEvent:
    if run_id is None:
        run_id = str(generate_new_uuid())
    return RunEvent(
        eventType=state,
        eventTime="2024-06-15T12:00:00Z",
        run=Run(runId=run_id),
        job=Job(namespace=namespace, name=job_name),
        producer="awebo-test",
    )


@mock_aws
def test_s3_emitter_writes_json_to_s3() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="lineage-bucket")

    emitter = S3Emitter(bucket="lineage-bucket", prefix="events", region="us-east-1")
    event = _make_event(RunState.COMPLETE, namespace="ns", job_name="write_db.t")
    run_id = event.run.runId
    emitter.emit(event)

    objs = s3.list_objects_v2(Bucket="lineage-bucket", Prefix="events/")
    assert objs["KeyCount"] == 1
    key = objs["Contents"][0]["Key"]
    assert f"ns/write_db.t/{run_id}/" in key
    assert "COMPLETE" in key
    assert key.endswith(".json")

    body = json.loads(s3.get_object(Bucket="lineage-bucket", Key=key)["Body"].read())
    assert body["eventType"] == "COMPLETE"
    assert body["run"]["runId"] == run_id


@mock_aws
def test_s3_emitter_key_structure() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="lineage-keys")

    event = _make_event(RunState.START, namespace="prod", job_name="write_a.b")
    run_id = event.run.runId
    emitter = S3Emitter(bucket="lineage-keys", prefix="ol/events", region="us-east-1")
    emitter.emit(event)

    objs = s3.list_objects_v2(Bucket="lineage-keys", Prefix="ol/events/")
    key = objs["Contents"][0]["Key"]
    assert key.startswith(f"ol/events/prod/write_a.b/{run_id}/START_")


@mock_aws
def test_s3_emitter_failure_logs_warning_never_raises() -> None:
    emitter = S3Emitter(bucket="nonexistent-bucket", prefix="e", region="us-east-1")
    emitter.emit(_make_event())
