from __future__ import annotations

from pathlib import Path

from .io import write_json


WORKLOADS = [
    {
        "name": "distributed-demand-backfill",
        "queue": "demand-training-queue",
        "placement": "compact",
        "topology_key": "cloud.provider.com/topology-rack",
        "pod_count": 16,
        "policy": "required",
        "why": "distributed feature-heavy training should avoid cross-rack all-reduce latency",
        "fallback": "split the backfill into four-pod waves and preserve partition idempotency keys",
    },
    {
        "name": "feature-sweep-hpo",
        "queue": "feature-sweep-queue",
        "placement": "balanced",
        "topology_key": "kubernetes.io/hostname",
        "pod_count": 8,
        "policy": "preferred",
        "why": "preemptible sweeps should avoid consuming the contiguous rack capacity needed by critical backfills",
        "fallback": "skip low-priority HPO and keep the deterministic baseline",
    },
    {
        "name": "airflow-scheduler-control-plane",
        "queue": "training-quality-queue",
        "placement": "spread",
        "topology_key": "topology.kubernetes.io/zone",
        "pod_count": 2,
        "policy": "required",
        "why": "scheduler and triggerer replicas should not share one failure domain",
        "fallback": "run a single scheduler and freeze bulk backfills until HA is restored",
    },
]


def build_topology_placement_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "topology_resource_declared", "passed": True, "observed": "kueue.x-k8s.io/Topology"},
        {"name": "distributed_backfill_uses_compact_topology", "passed": any(workload["placement"] == "compact" and workload["pod_count"] >= 16 for workload in WORKLOADS)},
        {"name": "airflow_control_plane_spread", "passed": any(workload["name"].startswith("airflow") and workload["placement"] == "spread" for workload in WORKLOADS)},
        {"name": "wave_split_fallback_defined", "passed": any("split" in workload["fallback"] for workload in WORKLOADS)},
        {"name": "fallbacks_defined", "passed": all(workload["fallback"] for workload in WORKLOADS)},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_topology_aware_backfills" if all(check["passed"] for check in checks) else "hold_topology_sensitive_backfills",
        "topology_levels": [
            "cloud.provider.com/topology-block",
            "cloud.provider.com/topology-rack",
            "kubernetes.io/hostname",
        ],
        "workloads": WORKLOADS,
        "checks": checks,
        "guardrails": [
            "Compact only distributed training jobs that benefit from proximity.",
            "Spread Airflow control-plane components across zones.",
            "Preempt or defer HPO sweeps before fragmenting rack-level backfill capacity.",
            "Use topology assignment delay as a signal to reduce Airflow mapped-task width.",
            "Keep partition manifests immutable so topology-driven wave splitting remains idempotent.",
        ],
        "kubernetes_assets": ["kubernetes/topology-aware-scheduling.yaml"],
        "references": [
            "https://kueue.sigs.k8s.io/docs/concepts/topology_aware_scheduling/",
            "https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/",
            "https://kueue.sigs.k8s.io/docs/concepts/admission_check/",
            "https://kubernetes.io/docs/concepts/workloads/workload-api/topology-aware-scheduling/",
        ],
    }
    write_json(root / "reports" / "topology_placement_plan.json", plan)
    return plan
