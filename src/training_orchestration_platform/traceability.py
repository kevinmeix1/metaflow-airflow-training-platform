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
    airflow = span(trace_id, "airflow.backfill_wave", parent=None, service="airflow", duration_ms=120.0, attributes={"pool": "metaflow_training_pool"})
    kueue = span(trace_id, "kueue.admit_workload", parent=airflow["span_id"], service="kueue", duration_ms=35.0, attributes={"queue": "demand-training-queue"})
    metaflow = span(trace_id, "metaflow.partition_train", parent=kueue["span_id"], service="metaflow", duration_ms=640.0, attributes={"flow": "DemandTrainingFlow"})
    mlflow = span(trace_id, "mlflow.log_artifacts", parent=metaflow["span_id"], service="mlflow", duration_ms=55.0, attributes={"artifact": "model"})
    lineage = span(trace_id, "openlineage.publish", parent=mlflow["span_id"], service="openlineage", duration_ms=22.0, attributes={"asset": "daily_demand_model"})
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
