"""S3 lineage emitter — persists OpenLineage events as JSON to S3."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openlineage.client.run import RunEvent

log = logging.getLogger(__name__)


class S3Emitter:
    def __init__(self, bucket: str, prefix: str, region: str | None = None) -> None:
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._region = region

    def emit(self, event: RunEvent) -> None:
        try:
            import boto3
            from openlineage.client.serde import Serde

            key = self._build_key(event)
            payload = Serde.to_json(event).encode()
            client = boto3.client("s3", region_name=self._region)
            client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=payload,
                ContentType="application/json",
            )
            log.debug("lineage event written to s3://%s/%s", self._bucket, key)
        except Exception:
            log.warning("S3 lineage emission failed", exc_info=True)

    def _build_key(self, event: RunEvent) -> str:
        namespace = event.job.namespace
        job_name = event.job.name
        run_id = event.run.runId
        event_type = event.eventType.name
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        return f"{self._prefix}/{namespace}/{job_name}/{run_id}/{event_type}_{ts}.json"
