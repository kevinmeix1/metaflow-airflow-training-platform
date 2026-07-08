from __future__ import annotations

import hashlib
from pathlib import Path

from .io import write_json


def _hex(value: str, length: int) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def span(trace_id: str, name: str, *, parent: str | None, service: str, duration_ms: float, attributes: dict) -> dict:
    span_id = _hex(f"{trace_id}:{name}:{service}", 16)
    return {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent,
        "name": name,
        "service": service,
        "kind": "internal",
        "status": "ok",
        "duration_ms": duration_ms,
        "attributes": attributes,
    }


def build_trace_report(root: str | Path) -> dict:
    root = Path(root)
    trace_id = _hex("demand-training-backfill-trace", 32)
    airflow = span(
        trace_id,
        "airflow.backfill_wave",
        parent=None,
        service="airflow",
        duration_ms=120.0,
        attributes={
            "service.name": "airflow-scheduler",
            "airflow.dag_id": "enterprise_backfill_training_mesh",
            "airflow.run_id": "scheduled__2026-06-07",
            "airflow.pool.name": "metaflow_training_pool",
            "backfill.wave_index": 2,
            "deployment.environment.name": "local-demo",
        },
    )
    kueue = span(
        trace_id,
        "kueue.admit_workload",
        parent=airflow["span_id"],
        service="kueue",
        duration_ms=35.0,
        attributes={
            "service.name": "kueue",
            "kueue.queue.name": "demand-training-queue",
            "kueue.workload.name": "demand-backfill-2026-06-06",
            "queue.wait_ms": 35.0,
            "k8s.namespace.name": "ml-training",
            "k8s.job.name": "demand-partition-2026-06-06",
        },
    )
    metaflow = span(
        trace_id,
        "metaflow.partition_train",
        parent=kueue["span_id"],
        service="metaflow",
        duration_ms=640.0,
        attributes={
            "service.name": "metaflow",
            "metaflow.flow_name": "DemandTrainingFlow",
            "metaflow.run_id": "demand-2026-06-06-recovery",
            "metaflow.step_name": "train",
            "partition.date": "2026-06-06",
            "partition.status": "recovered",
            "partition.retry_count": 1,
            "training.duration_ms": 640.0,
            "k8s.pod.name": "demand-partition-2026-06-06-0",
        },
    )
    mlflow = span(
        trace_id,
        "mlflow.log_artifacts",
        parent=metaflow["span_id"],
        service="mlflow",
        duration_ms=55.0,
        attributes={
            "service.name": "mlflow",
            "mlflow.run_id": "mlflow-demand-20260606",
            "ml.model.name": "daily-demand-forecaster",
            "ml.model.version": "2026.06.06",
            "artifact.name": "model",
            "artifact.bytes": 184320,
        },
    )
    lineage = span(
        trace_id,
        "openlineage.publish",
        parent=mlflow["span_id"],
        service="openlineage",
        duration_ms=22.0,
        attributes={
            "service.name": "openlineage",
            "openlineage.run_id": "ol-demand-20260606",
            "openlineage.dataset.name": "daily_demand_model",
            "partition.date": "2026-06-06",
        },
    )
    spans = [airflow, kueue, metaflow, mlflow, lineage]
    report = {
        "trace_id": trace_id,
        "span_count": len(spans),
        "critical_path_ms": round(sum(item["duration_ms"] for item in spans), 2),
        "root_service": "airflow",
        "leaf_service": "openlineage",
        "spans": spans,
    }
    write_json(root / "reports" / "trace_report.json", report)
    return report
