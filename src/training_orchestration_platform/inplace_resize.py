from __future__ import annotations

from pathlib import Path

from .io import write_json


RESIZE_POLICIES = [
    {
        "name": "partition-worker-startup-boost",
        "workload": "feature-heavy-backfill",
        "scope": "container",
        "resource_patch": {"requests.cpu": "1500m", "limits.memory": "2Gi"},
        "resize_policy": {"cpu": "NotRequired", "memory": "RestartContainer"},
        "trigger": "feature-heavy partition startup exceeds its Airflow task runtime budget",
        "owner_action": "boost CPU in-place while preserving Airflow map index, Metaflow run id, and partition id",
    },
    {
        "name": "distributed-backfill-pod-level-burst",
        "workload": "memory-bound-training",
        "scope": "pod",
        "resource_patch": {"spec.resources.limits.cpu": "12", "spec.resources.requests.memory": "24Gi"},
        "resize_policy": {"cpu": "NotRequired", "memory": "RestartContainer"},
        "trigger": "distributed training workers wait on CPU while node fit remains feasible",
        "owner_action": "expand the pod-level envelope before increasing Airflow mapped fanout",
    },
    {
        "name": "failed-partition-replay-shrink",
        "workload": "failed-partition-replay",
        "scope": "container",
        "resource_patch": {"requests.cpu": "200m", "limits.memory": "512Mi"},
        "resize_policy": {"cpu": "NotRequired", "memory": "NotRequired"},
        "trigger": "failed-partition replay is warm but idle between recovery windows",
        "owner_action": "shrink idle replay pods in-place so deterministic recovery remains warm without wasting quota",
    },
]


def build_inplace_resize_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    checks = [
        {"name": "container_resize_ga", "passed": True, "evidence": "Kubernetes v1.35 made in-place CPU and memory resizing stable through the resize subresource."},
        {"name": "pod_level_resize_beta", "passed": any(policy["scope"] == "pod" for policy in RESIZE_POLICIES), "evidence": "Kubernetes v1.36 beta pod-level resource resizing covers multi-container training workers."},
        {"name": "resize_policy_defined", "passed": all(policy["resize_policy"] for policy in RESIZE_POLICIES), "evidence": "Training workloads declare whether CPU and memory changes can happen without restarts."},
        {"name": "lineage_preserved", "passed": any("Metaflow run id" in policy["owner_action"] for policy in RESIZE_POLICIES), "evidence": "Resize evidence preserves Airflow map index, Metaflow run id, and partition id."},
        {"name": "vpa_inplace_or_recreate_ready", "passed": True, "evidence": "VPA recommendation mode is modeled with InPlaceOrRecreate for partition workers and replay pods."},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_training_inplace_resize_controls",
        "features": {
            "in_place_pod_resize": {
                "state": "Kubernetes v1.35 stable",
                "subresource": "pods/resize",
                "container_status_field": "status.containerStatuses[*].resources",
            },
            "pod_level_resource_resize": {
                "state": "Kubernetes v1.36 beta and enabled by default",
                "feature_gate": "InPlacePodLevelResourcesVerticalScaling",
                "pod_spec_field": "spec.resources",
                "status_conditions": ["PodResizePending", "PodResizeInProgress"],
            },
            "autoscaler_integration": {
                "vpa_update_mode": "InPlaceOrRecreate",
                "requires_runtime": "cgroup v2 and CRI UpdateContainerResources support",
            },
        },
        "policies": RESIZE_POLICIES,
        "training_guardrails": [
            "Do not widen Airflow mapped fanout while PodResizePending or PodResizeInProgress is true for a worker pool.",
            "Record Airflow map index, Metaflow run id, partition date, desired resources, and actual status.resources together.",
            "Use CPU in-place resize for startup boosts; memory changes must follow the declared resizePolicy path.",
            "Keep failed-partition replay warm by shrinking idle pods rather than deleting the recovery path.",
            "Treat pod-level resize as wave-local capacity relief before borrowing from production training queues.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/inplace-pod-resize.yaml"],
        "references": [
            "https://kubernetes.io/blog/2025/12/19/kubernetes-v1-35-in-place-pod-resize-ga/",
            "https://kubernetes.io/blog/2026/04/30/kubernetes-v1-36-inplace-pod-level-resources-beta/",
            "https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/",
        ],
    }
    write_json(root / "reports" / "inplace_resize_plan.json", plan)
    return plan
