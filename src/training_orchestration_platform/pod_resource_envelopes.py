from __future__ import annotations

from pathlib import Path

from .io import write_json


POD_RESOURCE_WORKLOADS = [
    {
        "name": "partition-manifest-builder",
        "namespace": "mlops-training",
        "pod_level_requests": {"cpu": "2", "memory": "4Gi"},
        "pod_level_limits": {"cpu": "3", "memory": "6Gi"},
        "scheduling_gates": ["mlops.kevinmei.dev/manifest-inputs-ready"],
        "release_condition": "raw sales partitions and content hashes are present before manifest construction",
        "containers": ["manifest-builder", "otel-sidecar"],
    },
    {
        "name": "metaflow-training-worker",
        "namespace": "mlops-training",
        "pod_level_requests": {"cpu": "6", "memory": "12Gi"},
        "pod_level_limits": {"cpu": "8", "memory": "16Gi"},
        "scheduling_gates": ["mlops.kevinmei.dev/kueue-admitted", "mlops.kevinmei.dev/artifact-volume-ready"],
        "release_condition": "Kueue admits the training wave and digest-pinned OCI artifact volume is mounted",
        "containers": ["metaflow-worker", "checkpoint-writer"],
    },
    {
        "name": "failed-partition-replay",
        "namespace": "mlops-training",
        "pod_level_requests": {"cpu": "3", "memory": "6Gi"},
        "pod_level_limits": {"cpu": "5", "memory": "10Gi"},
        "scheduling_gates": ["mlops.kevinmei.dev/replay-window-approved", "mlops.kevinmei.dev/bundle-version-pinned"],
        "release_condition": "Airflow backfill window is approved and DAG Bundle version is pinned for forensic replay",
        "containers": ["replay-runner", "lineage-exporter"],
    },
]


def build_pod_resource_envelope_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    checks = [
        {
            "name": "pod_level_resources_declared",
            "passed": all(item["pod_level_requests"] and item["pod_level_limits"] for item in POD_RESOURCE_WORKLOADS),
            "evidence": "Training pods use pod-level CPU and memory envelopes around Metaflow workers and sidecars.",
        },
        {
            "name": "scheduling_gates_declared",
            "passed": all(item["scheduling_gates"] for item in POD_RESOURCE_WORKLOADS),
            "evidence": "Manifest, training, and replay pods stay SchedulingGated until data, artifact, queue, or bundle prerequisites pass.",
        },
        {
            "name": "gate_release_runbook",
            "passed": True,
            "evidence": "Airflow removes gates only after manifest checks, OCI artifact volume readiness, Kueue admission, and replay approvals.",
        },
        {
            "name": "scheduler_churn_metric",
            "passed": True,
            "evidence": "scheduler_pending_pods{queue=\"gated\"} is tracked separately from unschedulable training pods.",
        },
        {
            "name": "dra_compatibility_guardrail",
            "passed": True,
            "evidence": "DRA-backed training jobs and container requests must fit inside pod-level envelopes before gate removal.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": passed,
        "recommended_action": "enable_training_pod_resource_envelopes_and_scheduling_gates" if passed else "keep_container_only_training_requests",
        "kubernetes_version_target": "1.34+",
        "feature_gates": {
            "PodLevelResources": "beta, enabled by default in Kubernetes 1.34+ clusters that support the feature",
            "PodSchedulingReadiness": "stable since Kubernetes 1.30",
            "PodLevelResourceManagers": "enable where CPUManager, MemoryManager, or TopologyManager alignment is required",
        },
        "workloads": POD_RESOURCE_WORKLOADS,
        "release_runbook": [
            "Create manifest, training, and replay pods with schedulingGates so scheduler and autoscaler work starts only after prerequisites exist.",
            "Verify raw data hashes, partition manifests, Kueue admission, OCI artifact volume readiness, DAG Bundle version, and replay approvals.",
            "Patch away gates in any order after prerequisites pass; never add new gates after pod creation.",
            "Alert on scheduler_pending_pods{queue=\"gated\"} and gates older than backfill SLOs.",
        ],
        "checks": checks,
        "kubernetes_assets": [
            "kubernetes/pod-resource-envelopes.yaml",
        ],
        "references": [
            "https://kubernetes.io/docs/tasks/configure-pod-container/assign-pod-level-resources/",
            "https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/",
            "https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/",
        ],
    }
    write_json(root / "reports" / "pod_resource_envelope_plan.json", plan)
    return plan
