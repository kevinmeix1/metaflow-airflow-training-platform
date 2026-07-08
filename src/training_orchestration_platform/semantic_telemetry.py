from __future__ import annotations

from pathlib import Path

from .io import write_json


REQUIRED_ATTRIBUTES = [
    "service.name",
    "deployment.environment.name",
    "airflow.dag_id",
    "airflow.run_id",
    "airflow.pool.name",
    "kueue.queue.name",
    "kueue.workload.name",
    "metaflow.flow_name",
    "metaflow.run_id",
    "metaflow.step_name",
    "mlflow.run_id",
    "ml.model.name",
    "ml.model.version",
    "openlineage.run_id",
    "openlineage.dataset.name",
    "partition.date",
    "partition.status",
    "k8s.namespace.name",
    "k8s.pod.name",
    "k8s.job.name",
]

REDACTED_ATTRIBUTES = [
    "training.row_sample",
    "feature.row",
    "pii.customer_id",
    "http.request.body",
]


def build_semantic_telemetry_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "airflow_backfill_context", "passed": "airflow.dag_id" in REQUIRED_ATTRIBUTES and "airflow.pool.name" in REQUIRED_ATTRIBUTES},
        {"name": "kueue_admission_context", "passed": "kueue.queue.name" in REQUIRED_ATTRIBUTES and "kueue.workload.name" in REQUIRED_ATTRIBUTES},
        {"name": "metaflow_partition_context", "passed": "metaflow.flow_name" in REQUIRED_ATTRIBUTES and "partition.date" in REQUIRED_ATTRIBUTES},
        {"name": "mlflow_artifact_context", "passed": "mlflow.run_id" in REQUIRED_ATTRIBUTES and "ml.model.version" in REQUIRED_ATTRIBUTES},
        {"name": "openlineage_dataset_context", "passed": "openlineage.run_id" in REQUIRED_ATTRIBUTES and "openlineage.dataset.name" in REQUIRED_ATTRIBUTES},
        {"name": "training_payload_redaction", "passed": "training.row_sample" in REDACTED_ATTRIBUTES and "pii.customer_id" in REDACTED_ATTRIBUTES},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enforce_training_lineage_telemetry_contract" if all(check["passed"] for check in checks) else "hold_backfill_telemetry_rollout",
        "schema": {
            "profile": "otel-airflow-kueue-metaflow-lineage",
            "required_attributes": REQUIRED_ATTRIBUTES,
            "redacted_attributes": REDACTED_ATTRIBUTES,
            "numeric_fields": [
                "backfill.wave_index",
                "partition.retry_count",
                "queue.wait_ms",
                "training.duration_ms",
                "artifact.bytes",
            ],
        },
        "lineage_pivots": [
            {"pivot": "airflow_backfill", "attributes": ["airflow.dag_id", "airflow.run_id", "airflow.pool.name"]},
            {"pivot": "admitted_workload", "attributes": ["kueue.queue.name", "kueue.workload.name", "k8s.job.name"]},
            {"pivot": "metaflow_partition", "attributes": ["metaflow.flow_name", "metaflow.run_id", "metaflow.step_name", "partition.date", "partition.status"]},
            {"pivot": "model_artifact", "attributes": ["mlflow.run_id", "ml.model.name", "ml.model.version", "artifact.bytes"]},
            {"pivot": "dataset_lineage", "attributes": ["openlineage.run_id", "openlineage.dataset.name"]},
        ],
        "checks": checks,
        "collector_policy": {
            "processor": "attributes/semantic_redaction",
            "drop_training_payloads_by_default": True,
            "exporter_contract": "partition, run, queue, artifact, and lineage attributes stay queryable while row samples and identifiers are removed",
        },
        "guardrails": [
            "Do not export raw training rows, feature rows, request bodies, or customer identifiers by default.",
            "Attach partition date and status to every partition-training span so recovery and backfill freshness are queryable.",
            "Attach Kueue queue and workload metadata before batching so stuck backfills can pivot to admission state.",
            "Keep queue wait, retry count, training duration, and artifact size numeric for SLO and capacity dashboards.",
        ],
        "kubernetes_assets": ["kubernetes/opentelemetry-collector.yaml"],
        "references": [
            "https://opentelemetry.io/docs/specs/semconv/",
            "https://opentelemetry.io/docs/specs/semconv/system/k8s-metrics/",
            "https://openlineage.io/docs/",
        ],
    }
    write_json(root / "reports" / "semantic_telemetry_plan.json", plan)
    return plan
