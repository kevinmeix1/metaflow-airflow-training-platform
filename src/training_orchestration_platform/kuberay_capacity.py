from __future__ import annotations

from pathlib import Path

from .io import write_json


RAY_WORKLOADS = [
    {
        "name": "demand-elastic-backfill",
        "kind": "RayJob",
        "queue": "demand-training-queue",
        "priority": "backfill-critical",
        "min_workers": 4,
        "max_workers": 24,
        "gpus_per_worker": 1,
        "autoscaling": "elastic",
        "scheduling": "kueue_admitted_rayjob",
        "why": "fan out distributed feature generation and model-family training inside a bounded backfill wave",
        "fallback": "split the date range into smaller Metaflow partitions and preserve partition manifests",
    },
    {
        "name": "feature-quality-sweep",
        "kind": "RayJob",
        "queue": "feature-sweep-queue",
        "priority": "standard",
        "min_workers": 1,
        "max_workers": 12,
        "gpus_per_worker": 0,
        "autoscaling": "elastic",
        "scheduling": "queue_when_capacity_free",
        "why": "parallelize feature freshness, volume, and drift checks before expensive training starts",
        "fallback": "run quality checks through Airflow dynamic task mapping",
    },
    {
        "name": "metaflow-artifact-replay",
        "kind": "RayCluster",
        "queue": "demand-recovery-queue",
        "priority": "recovery-critical",
        "min_workers": 1,
        "max_workers": 8,
        "gpus_per_worker": 0,
        "autoscaling": "elastic",
        "scheduling": "workload_slices",
        "why": "replay failed training artifacts and lineage payloads without reopening the full backfill",
        "fallback": "force a single failed partition with the existing Metaflow recovery command",
    },
]


def build_kuberay_capacity_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "distributed_backfill_declared", "passed": any(workload["name"] == "demand-elastic-backfill" for workload in RAY_WORKLOADS)},
        {"name": "gpu_bounds_declared", "passed": sum(workload["max_workers"] * workload["gpus_per_worker"] for workload in RAY_WORKLOADS) > 0},
        {"name": "recovery_path_declared", "passed": any(workload["priority"] == "recovery-critical" for workload in RAY_WORKLOADS)},
        {"name": "kueue_queue_labels_required", "passed": all(workload["queue"] for workload in RAY_WORKLOADS)},
        {"name": "fallbacks_defined", "passed": all(workload["fallback"] for workload in RAY_WORKLOADS)},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_kuberay_backfill_waves" if all(check["passed"] for check in checks) else "keep_metaflow_partition_backfills",
        "workloads": RAY_WORKLOADS,
        "capacity": {
            "max_workers": sum(workload["max_workers"] for workload in RAY_WORKLOADS),
            "max_gpu_demand": sum(workload["max_workers"] * workload["gpus_per_worker"] for workload in RAY_WORKLOADS),
            "backfill_reserved_workers": 8,
            "autoscaler_idle_timeout_seconds": 120,
        },
        "checks": checks,
        "guardrails": [
            "Admit Ray backfill waves through Kueue before creating workers.",
            "Keep Metaflow partition manifests as the source of idempotency when Ray splits work.",
            "Use recovery-critical Ray capacity only for failed artifact replay, not opportunistic sweeps.",
            "Cap GPU workers per wave and let Airflow reduce mapped-task width when Kueue backlog grows.",
            "Publish Ray job status and worker spill metrics into the training dashboard evidence.",
        ],
        "kubernetes_assets": ["kubernetes/kuberay-kueue-workloads.yaml"],
        "references": [
            "https://kueue.sigs.k8s.io/docs/tasks/run/rayjobs/",
            "https://docs.ray.io/en/latest/cluster/kubernetes/k8s-ecosystem/kueue.html",
            "https://docs.ray.io/en/latest/cluster/kubernetes/examples/rayjob-kueue-gang-scheduling.html",
        ],
    }
    write_json(root / "reports" / "kuberay_capacity_plan.json", plan)
    return plan
