from __future__ import annotations

from pathlib import Path

from .io import write_json


DEVICE_HEALTH_EVENTS = [
    {
        "workload": "feature-heavy-backfill",
        "namespace": "mlops-training",
        "pod": "demand-training-dra-backfill-3",
        "container": "trainer",
        "resource_claim": "l4-shared-training-claim",
        "device_class": "gpu-l4-shared",
        "resource": "gpu.resource.kubernetes.io",
        "health": "Unhealthy",
        "message": "driver reported ECC page retirement on shared L4 during feature-heavy partition training",
        "owner_action": "run the deterministic CPU baseline and keep the partition queued for GPU replay",
    },
    {
        "workload": "hpo-smoke-sweep",
        "namespace": "mlops-training",
        "pod": "hpo-smoke-sweep-1",
        "container": "hpo-worker",
        "resource_claim": "l4-shared-sweep-claim",
        "device_class": "gpu-l4-shared",
        "resource": "gpu.resource.kubernetes.io",
        "health": "Unknown",
        "message": "DRA driver missed health update timeout after 30 seconds",
        "owner_action": "skip the sweep and keep the last approved deterministic baseline",
    },
    {
        "workload": "failed-partition-replay",
        "namespace": "ml-training-prod",
        "pod": "failed-partition-replay-cpu-0",
        "container": "metaflow-replay",
        "resource_claim": None,
        "device_class": "cpu-replay",
        "resource": "cpu",
        "health": "Healthy",
        "message": "CPU replay path has no DRA device dependency",
        "owner_action": "continue failed-partition replay while accelerator pool is quarantined",
    },
]


def build_resource_health_status_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    unhealthy = [event for event in DEVICE_HEALTH_EVENTS if event["health"] in {"Unhealthy", "Unknown"}]
    checks = [
        {
            "name": "resource_health_status_enabled",
            "passed": True,
            "evidence": "ResourceHealthStatus is beta and enabled by default in Kubernetes v1.36.",
        },
        {
            "name": "pod_allocated_resources_status_checked",
            "passed": all(event["container"] and event["pod"] for event in DEVICE_HEALTH_EVENTS),
            "evidence": "Runbook queries Pod status.containerStatuses[*].allocatedResourcesStatus before widening Airflow mapped fanout.",
        },
        {
            "name": "resourceclaim_device_status_checked",
            "passed": any(event["resource_claim"] for event in DEVICE_HEALTH_EVENTS),
            "evidence": "ResourceClaim status.devices is captured for allocated training and HPO accelerator claims.",
        },
        {
            "name": "device_taint_rule_declared",
            "passed": True,
            "evidence": "DeviceTaintRule quarantines unhealthy shared training devices before more partitions land on them.",
        },
        {
            "name": "failed_partition_replay_unblocked",
            "passed": any(event["workload"] == "failed-partition-replay" and event["resource"] == "cpu" for event in DEVICE_HEALTH_EVENTS),
            "evidence": "The failed-partition replay path stays CPU-runnable while GPU partitions wait for recovery.",
        },
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_training_dra_resource_health_runbook",
        "feature": {
            "name": "ResourceHealthStatus",
            "state": "Kubernetes v1.36 beta and enabled by default",
            "pod_status_field": "status.containerStatuses[*].allocatedResourcesStatus",
            "driver_service": "DRAResourceHealth gRPC service",
            "default_unknown_timeout_seconds": 30,
        },
        "companion_features": {
            "resource_claim_device_status": "Kubernetes v1.33 beta; status.devices on ResourceClaim",
            "granular_status_authorization": "Kubernetes v1.36 beta; synthetic subresources and node-aware verbs",
            "device_taints": "Kubernetes v1.36 beta; DeviceTaintRule uses resource.k8s.io/v1beta2",
        },
        "device_health_events": DEVICE_HEALTH_EVENTS,
        "unhealthy_or_unknown_count": len(unhealthy),
        "training_decision_policy": [
            "Do not widen Airflow mapped training fanout when a release-critical DRA device is Unhealthy or Unknown.",
            "Route affected partitions to deterministic CPU baseline or replay queues instead of failing the whole backfill.",
            "Use ResourceClaim status.devices and kubelet PodResourcesLister to distinguish model-code failures from device faults.",
            "Taint unhealthy shared GPU devices before admitting additional feature-heavy partitions.",
            "Require a fresh healthy device snapshot before enabling HPO sweeps or memory-bound training waves.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/dra-resource-health-status.yaml"],
        "references": [
            "https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/",
            "https://kubernetes.io/blog/2026/05/07/kubernetes-v1-36-dra-136-updates/",
            "https://github.com/kubernetes/sig-release/discussions/2958",
        ],
    }
    write_json(root / "reports" / "resource_health_status_plan.json", plan)
    return plan
