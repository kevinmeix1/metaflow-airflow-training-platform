from __future__ import annotations

from pathlib import Path

from .io import write_json


def build_ai_workload_telemetry_plan(root: str | Path) -> dict:
    root = Path(root)
    workloads = [
        {
            "name": "daily-demand-backfill",
            "kind": "Airflow Asset DAG",
            "queue": "demand-training-queue",
            "asset": "asset://training/daily_demand_model",
            "resource_signals": ["pod.resources.requests.cpu", "pod.resources.limits.memory", "pod.index"],
            "otel_attributes": ["airflow.dag_id", "airflow.asset.uri", "openlineage.dataset.name", "metaflow.run_id"],
            "slo": {"successful_partitions": 7, "queue_wait_p95_seconds": 600, "freshness_minutes": 180},
            "remediation": "resume failed partition by index, preserve cloned Metaflow lineage, and hold bulk backfills until queue pressure clears",
        },
        {
            "name": "metaflow-candidate-grid",
            "kind": "Metaflow Flow",
            "queue": "training-candidates",
            "asset": "metaflow://DemandForecastFlow/candidate-grid",
            "resource_signals": ["pod.resources.requests.cpu", "pod.resources.limits.memory", "dra.resourceclaim.status"],
            "otel_attributes": ["metaflow.flow_name", "metaflow.run_id", "ml.model.family", "ml.training.dataset.digest"],
            "slo": {"candidate_success_rate": 0.95, "artifact_publish_seconds": 90, "freshness_minutes": 240},
            "remediation": "prune expensive candidates, reuse cached OCI artifact volumes, and publish only candidates with passing gates",
        },
        {
            "name": "checkpointed-trainer",
            "kind": "Indexed Job",
            "queue": "gpu-training",
            "asset": "oci://registry.local/demand/checkpoints",
            "resource_signals": ["pod.resources.requests.cpu", "pod.resources.limits.memory", "dra.resource.claim.name"],
            "otel_attributes": ["training.checkpoint.path", "training.resume.lineage", "kueue.workload.name", "ml.model.version"],
            "slo": {"checkpoint_interval_seconds": 120, "resume_minutes": 15, "freshness_minutes": 300},
            "remediation": "restore the latest content-addressed checkpoint and gate resumed artifacts through release admission",
        },
    ]
    required_resource_fields = {field for workload in workloads for field in workload["resource_signals"]}
    required_otel_fields = {field for workload in workloads for field in workload["otel_attributes"]}
    plan = {
        "generated_at": "2026-07-11T00:00:00Z",
        "standard_alignment": {
            "airflow": "Asset-aware orchestration ties each backfill and candidate run to a downstream freshness contract.",
            "metaflow": "Metaflow run and resume lineage are promoted to telemetry fields for recovery and audit.",
            "kubernetes": "Pod-level resources, Indexed Jobs, and DRA claims explain training cost and retry behavior.",
        },
        "workloads": workloads,
        "required_resource_fields": sorted(required_resource_fields),
        "required_otel_fields": sorted(required_otel_fields),
        "checks": [
            {"name": "asset_lineage_mapped", "passed": "airflow.asset.uri" in required_otel_fields},
            {"name": "metaflow_resume_lineage_mapped", "passed": "training.resume.lineage" in required_otel_fields},
            {"name": "indexed_job_resources_mapped", "passed": "pod.index" in required_resource_fields},
            {"name": "dra_health_mapped", "passed": any("dra." in field for field in required_resource_fields)},
        ],
        "runbook": [
            "Correlate Airflow asset freshness, Metaflow run ids, and Kueue queue wait before admitting bulk backfills.",
            "For failed partitions, resume by partition index and attach checkpoint lineage to the release report.",
            "Prefer shrinking candidate grids over starving incident or rollback queues.",
        ],
    }
    plan["passed"] = all(check["passed"] for check in plan["checks"])
    write_json(root / "reports" / "ai_workload_telemetry_plan.json", plan)
    return plan
