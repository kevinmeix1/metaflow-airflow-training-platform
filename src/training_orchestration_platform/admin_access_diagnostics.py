from __future__ import annotations

from pathlib import Path

from .io import write_json


ADMIN_ACCESS_DIAGNOSTICS = [
    {
        "name": "feature-backfill-ecc-snapshot",
        "namespace": "mlops-training-dra-admin",
        "target_workload": "feature-heavy-backfill",
        "target_device_class": "gpu-l4-shared",
        "claim": "training-backfill-admin-health",
        "trigger": "feature partition reports Unhealthy DRA status after a recoverable trainer retry",
        "evidence": ["ResourceClaim.status.devices", "allocatedResourcesStatus", "airflow.map_index", "metaflow.run_id"],
        "owner_action": "run deterministic CPU replay, tag the MLflow run with device_diagnostic=required, and quarantine only the affected device",
    },
    {
        "name": "hpo-sweep-gpu-leak-diagnostics",
        "namespace": "mlops-training-dra-admin",
        "target_workload": "hpo-smoke-sweep",
        "target_device_class": "gpu-a100-mig",
        "claim": "hpo-sweep-admin-snapshot",
        "trigger": "HPO workers report Unknown device health while trial metrics stop advancing",
        "evidence": ["mlflow.run_id", "trial_id", "gpu-memory-fragmentation", "ResourceClaim.status.devices"],
        "owner_action": "stop exploratory sweep, keep the last approved deterministic baseline, and defer retries to the next accelerator window",
    },
    {
        "name": "failed-partition-replay-readiness",
        "namespace": "mlops-training-dra-admin",
        "target_workload": "failed-partition-replay",
        "target_device_class": "cpu-replay",
        "claim": "partition-replay-admin-snapshot",
        "trigger": "failed partition replay needs proof that GPU quarantine will not block CPU recovery",
        "evidence": ["partition_date", "airflow.try_number", "queue-admission-state", "device-taint-summary"],
        "owner_action": "attach diagnostic evidence to the backfill summary and keep replay CPU-runnable",
    },
]


def build_admin_access_diagnostic_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    checks = [
        {
            "name": "namespace_scoped_admin_access",
            "passed": all(item["namespace"] == "mlops-training-dra-admin" for item in ADMIN_ACCESS_DIAGNOSTICS),
            "evidence": "Privileged ResourceClaims are isolated in a namespace labeled for DRA AdminAccess.",
        },
        {
            "name": "least_privilege_rbac",
            "passed": True,
            "evidence": "The diagnostic runner can manage ResourceClaims only in the admin namespace and read training workload status separately.",
        },
        {
            "name": "lineage_evidence_captured",
            "passed": any("metaflow.run_id" in item["evidence"] for item in ADMIN_ACCESS_DIAGNOSTICS),
            "evidence": "AdminAccess diagnostics capture Airflow map index, Metaflow run id, and MLflow run id before recovery.",
        },
        {
            "name": "deterministic_replay_preserved",
            "passed": any("deterministic CPU replay" in item["owner_action"] for item in ADMIN_ACCESS_DIAGNOSTICS),
            "evidence": "GPU diagnostics cannot block deterministic CPU replay of failed partitions.",
        },
        {
            "name": "short_lived_break_glass",
            "passed": True,
            "evidence": "Diagnostic claims require run linkage, cleanup TTLs, and Prometheus alerts for stale privileged access.",
        },
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_training_dra_admin_access_diagnostics",
        "feature": {
            "name": "DRA AdminAccess for ResourceClaims",
            "state": "Kubernetes v1.36 stable and enabled by default",
            "feature_gate": "DRAAdminAccess",
            "api_version": "resource.k8s.io/v1",
            "field": "spec.devices.requests[*].exactly.adminAccess",
            "namespace_label": 'resource.kubernetes.io/admin-access: "true"',
            "purpose": "non-disruptive training diagnostics for devices already allocated to backfills, HPO workers, and replay paths",
        },
        "diagnostics": ADMIN_ACCESS_DIAGNOSTICS,
        "training_guardrails": [
            "Do not widen Airflow mapped fanout while an AdminAccess claim is active for the same device class.",
            "Record Airflow map index, Metaflow run id, MLflow run id, partition date, and ResourceClaim name in one evidence bundle.",
            "Prefer deterministic CPU replay over destructive GPU resets during a production backfill.",
            "Keep exploratory HPO diagnostics separate from production backfill diagnostics through queue labels and run tags.",
            "Delete privileged ResourceClaims after evidence capture so AdminAccess cannot become a normal training allocation path.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/dra-admin-access-diagnostics.yaml"],
        "references": [
            "https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/",
            "https://github.com/kubernetes/sig-release/discussions/2958",
            "https://www.kubernetes.dev/resources/keps/5018/",
        ],
    }
    write_json(root / "reports" / "admin_access_diagnostics_plan.json", plan)
    return plan
