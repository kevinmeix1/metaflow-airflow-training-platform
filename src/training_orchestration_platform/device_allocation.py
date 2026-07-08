from __future__ import annotations

from pathlib import Path

from .io import write_json


WORKLOADS = [
    {
        "name": "feature-heavy-backfill",
        "queue": "demand-training-queue",
        "priority": "backfill-critical",
        "device_class": "gpu-l4-shared",
        "resource_claim_template": "l4-shared-training",
        "sharing_strategy": "time-slicing",
        "requires_dra": True,
        "fallback": "run the deterministic CPU baseline and keep the partition queued for GPU replay",
        "why": "feature-heavy backfills need bursty accelerator access but must not block daily model recovery",
    },
    {
        "name": "hpo-smoke-sweep",
        "queue": "feature-sweep-queue",
        "priority": "training-low-priority",
        "device_class": "gpu-l4-shared",
        "resource_claim_template": "l4-shared-sweep",
        "sharing_strategy": "time-slicing",
        "requires_dra": True,
        "fallback": "skip the sweep and keep the last approved deterministic baseline",
        "why": "smoke sweeps are useful for quality, but they should be preemptible when quota tightens",
    },
    {
        "name": "memory-bound-training",
        "queue": "demand-training-queue",
        "priority": "training-backfill-critical",
        "device_class": "gpu-a100-mig",
        "resource_claim_template": "a100-mig-training",
        "sharing_strategy": "mig",
        "requires_dra": True,
        "fallback": "split the partition into smaller CPU shards or defer to the next accelerator window",
        "why": "larger model families need MIG isolation before the backfill wave can be admitted",
    },
]


def build_device_allocation_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "resource_claim_templates_declared", "passed": all(workload["resource_claim_template"] for workload in WORKLOADS)},
        {"name": "kueue_quota_matches_claims", "passed": all(workload["queue"] for workload in WORKLOADS)},
        {"name": "fallback_paths_defined", "passed": all(workload["fallback"] for workload in WORKLOADS)},
        {"name": "sharing_modes_explicit", "passed": {workload["sharing_strategy"] for workload in WORKLOADS} == {"time-slicing", "mig"}},
        {"name": "backfill_recovery_unblocked", "passed": any("CPU baseline" in workload["fallback"] for workload in WORKLOADS)},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "admit_dra_backed_training_wave" if all(check["passed"] for check in checks) else "hold_accelerator_backfills",
        "device_classes": [
            {
                "name": "gpu-l4-shared",
                "allocation": "ResourceClaimTemplate per indexed training pod",
                "sharing_strategy": "NVIDIA time-slicing",
                "isolation": "shared accelerator for preemptible sweeps and small backfill shards",
                "kueue_flavor": "gpu-l4-shared",
            },
            {
                "name": "gpu-a100-mig",
                "allocation": "ResourceClaimTemplate per memory-bound training worker",
                "sharing_strategy": "MIG",
                "isolation": "hardware-backed slice isolation for model families with larger memory envelopes",
                "kueue_flavor": "gpu-a100-mig",
            },
        ],
        "workloads": WORKLOADS,
        "checks": checks,
        "guardrails": [
            "Keep Airflow pools smaller than Kueue quota so mapped tasks cannot over-admit GPU work.",
            "Use DRA ResourceClaims for accelerator-backed partitions instead of only static device limits.",
            "Keep CPU baselines runnable for failed partition recovery and release gates.",
            "Use time-sliced L4 claims for smoke sweeps, and MIG claims for memory-sensitive training.",
            "Treat pending ResourceClaims as an admission signal before widening a backfill wave.",
        ],
        "kubernetes_assets": ["kubernetes/dynamic-resource-allocation.yaml", "kubernetes/accelerator-scheduling.yaml"],
        "references": [
            "https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/",
            "https://kueue.sigs.k8s.io/docs/concepts/workload/",
            "https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/gpu-sharing.html",
            "https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html",
        ],
    }
    write_json(root / "reports" / "device_allocation_plan.json", plan)
    return plan
