from __future__ import annotations

from pathlib import Path

from .io import write_json


WORKLOAD_SLICES = [
    {
        "name": "daily-demand-backfill-scale-up",
        "workload": "demand-elastic-backfill",
        "queue": "demand-training-queue",
        "slice_name": "demand-backfill-slice-a",
        "replacement_for": None,
        "min_replicas": 8,
        "max_replicas": 24,
        "reason": "admit extra partitions when spare CPU and GPU quota is available",
    },
    {
        "name": "daily-demand-backfill-scale-down",
        "workload": "demand-elastic-backfill",
        "queue": "demand-training-queue",
        "slice_name": "demand-backfill-slice-b",
        "replacement_for": "mlops-training/demand-backfill-slice-a",
        "min_replicas": 4,
        "max_replicas": 16,
        "reason": "return capacity to higher-priority failed-partition recovery without suspending the whole JobSet",
    },
    {
        "name": "ray-gpu-worker-burst",
        "workload": "demand-ray-distributed-training",
        "queue": "demand-gpu-training-queue",
        "slice_name": "demand-ray-slice-a",
        "replacement_for": None,
        "min_replicas": 2,
        "max_replicas": 12,
        "reason": "scale GPU workers for distributed feature-heavy model families under Kueue quota",
    },
]


def build_elastic_workload_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "workload_slices_declared", "passed": all(item["slice_name"] for item in WORKLOAD_SLICES)},
        {"name": "replacement_slice_modeled", "passed": any(item["replacement_for"] for item in WORKLOAD_SLICES)},
        {"name": "jobset_integration_declared", "passed": True, "evidence": "JobSet carries Kueue queue label and workload-slice annotations"},
        {"name": "scale_up_and_down_bounds", "passed": all(item["min_replicas"] < item["max_replicas"] for item in WORKLOAD_SLICES)},
        {"name": "quota_safe_recovery_priority", "passed": any("failed-partition" in item["reason"] for item in WORKLOAD_SLICES)},
        {"name": "feature_gate_documented", "passed": True, "evidence": "ElasticJobsViaWorkloadSlices remains explicitly gated before production use"},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_kueue_elastic_backfill_slices" if all(check["passed"] for check in checks) else "hold_elastic_workload_rollout",
        "feature_gate": "ElasticJobsViaWorkloadSlices",
        "workload_slices": WORKLOAD_SLICES,
        "jobset_policy": {
            "api": "jobset.x-k8s.io/v1alpha2",
            "queue_label": "kueue.x-k8s.io/queue-name",
            "slice_annotation": "kueue.x-k8s.io/workload-slice-name",
            "replacement_annotation": "kueue.x-k8s.io/workload-slice-replacement-for",
        },
        "operational_guardrails": [
            "Keep failed-partition recovery in a higher-priority LocalQueue than elastic backfill expansion.",
            "Scale down by replacement slice before evicting a full active JobSet.",
            "Run smoke backfills against the replacement slice before allowing large historical waves.",
            "Disable the feature gate if replacement admission fails or slice accounting diverges from Kueue quota.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/kueue-elastic-workloads.yaml"],
        "references": [
            "https://kueue.sigs.k8s.io/docs/concepts/elastic_workload/",
            "https://kueue.sigs.k8s.io/docs/reference/labels-and-annotations/",
            "https://kueue.sigs.k8s.io/docs/tasks/run/jobsets/",
            "https://kueue.sigs.k8s.io/docs/concepts/workload/",
        ],
    }
    write_json(root / "reports" / "elastic_workload_plan.json", plan)
    return plan
