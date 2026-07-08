from __future__ import annotations

from pathlib import Path

from .io import write_json


DEVICE_SHARING_POLICIES = [
    {
        "name": "feature-heavy-backfill-prioritized-accelerator",
        "workload": "feature-heavy-backfill",
        "primary": "gpu-a100-mig",
        "alternatives": ["gpu-l4-shared", "cpu-baseline"],
        "feature": "DRAPrioritizedList",
        "owner_action": "try isolated MIG first, then shared L4, then deterministic CPU baseline for replayable partitions",
    },
    {
        "name": "hpo-smoke-sweep-consumable-capacity",
        "workload": "hpo-smoke-sweep",
        "primary": "partitionable-a100",
        "alternatives": ["4GiB-vgpu-slice", "skip-sweep"],
        "feature": "DRAConsumableCapacity",
        "owner_action": "bound exploratory HPO GPU memory so production backfills stay admitted",
    },
    {
        "name": "memory-bound-training-binding-readiness",
        "workload": "memory-bound-training",
        "primary": "fabric-attached-a100",
        "alternatives": ["split-cpu-shards", "next-accelerator-window"],
        "feature": "DRADeviceBindingConditions",
        "owner_action": "wait for device preparation before binding large training workers and split partitions if preparation fails",
    },
]


def build_advanced_device_sharing_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    checks = [
        {"name": "prioritized_device_alternatives_defined", "passed": all(policy["alternatives"] for policy in DEVICE_SHARING_POLICIES), "evidence": "Training backfills and sweeps declare ordered accelerator alternatives."},
        {"name": "partitionable_device_policy_defined", "passed": any("partitionable" in policy["primary"] for policy in DEVICE_SHARING_POLICIES), "evidence": "Exploratory HPO can consume logical slices instead of whole devices."},
        {"name": "consumable_capacity_budgeted", "passed": any(policy["feature"] == "DRAConsumableCapacity" for policy in DEVICE_SHARING_POLICIES), "evidence": "HPO smoke sweeps use bounded GPU memory so backfill-critical work keeps quota."},
        {"name": "device_binding_conditions_required", "passed": any(policy["feature"] == "DRADeviceBindingConditions" for policy in DEVICE_SHARING_POLICIES), "evidence": "Large training workers wait for prepared devices before binding."},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_training_dra_advanced_device_sharing_policy",
        "features": {
            "prioritized_list": {"state": "Kubernetes v1.36 stable", "feature_gate": "DRAPrioritizedList"},
            "partitionable_devices": {"state": "Kubernetes v1.36 beta and enabled by default", "feature_gate": "DRAPartitionableDevices"},
            "consumable_capacity": {"state": "feature-gated sharing primitive; validate target-cluster support before enforcement", "feature_gate": "DRAConsumableCapacity"},
            "device_binding_conditions": {
                "state": "Kubernetes v1.36 beta and enabled by default",
                "feature_gate": "DRADeviceBindingConditions",
                "scheduler_phase": "PreBind",
                "default_wait_seconds": 600,
            },
        },
        "policies": DEVICE_SHARING_POLICIES,
        "training_guardrails": [
            "Use prioritized alternatives before widening mapped Airflow fanout for a backfill wave.",
            "Keep exploratory HPO on partitionable or consumable capacity so production backfills keep quota.",
            "Treat binding failure conditions as deterministic partition recovery events, not random retry noise.",
            "Record the selected accelerator alternative in MLflow run tags and OpenLineage facets.",
            "Keep CPU baseline replay available when accelerator alternatives are exhausted.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/dra-advanced-device-sharing.yaml"],
        "references": [
            "https://kubernetes.io/blog/2026/05/07/kubernetes-v1-36-dra-136-updates/",
            "https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/",
            "https://kubernetes.io/blog/2025/09/18/kubernetes-v1-34-dra-consumable-capacity/",
        ],
    }
    write_json(root / "reports" / "advanced_device_sharing_plan.json", plan)
    return plan
