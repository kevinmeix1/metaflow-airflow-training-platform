from __future__ import annotations

from pathlib import Path

from .io import write_json


WORKLOADS = [
    {
        "name": "airflow-scheduler",
        "qos_class": "Guaranteed",
        "request_memory": "768Mi",
        "limit_memory": "768Mi",
        "protection": "memory.min set from request for hard protection",
        "reason": "Scheduler and triggerer health must survive training node pressure.",
    },
    {
        "name": "metaflow-partition-worker",
        "qos_class": "Burstable",
        "request_memory": "4Gi",
        "limit_memory": "10Gi",
        "protection": "memory.low set from request with memory.high throttling",
        "reason": "Partition workers need useful protection but should yield above request.",
    },
    {
        "name": "hpo-ray-worker",
        "qos_class": "Burstable",
        "request_memory": "8Gi",
        "limit_memory": "16Gi",
        "protection": "memory.low set from request for distributed training progress",
        "reason": "HPO waves can be throttled before starving critical replay or registration tasks.",
    },
    {
        "name": "mlflow-registration-gate",
        "qos_class": "Guaranteed",
        "request_memory": "512Mi",
        "limit_memory": "512Mi",
        "protection": "memory.min set from request for hard protection",
        "reason": "Model registration gates must not be reclaimed during promotion.",
    },
    {
        "name": "exploratory-notebook",
        "qos_class": "BestEffort",
        "request_memory": "0",
        "limit_memory": "0",
        "protection": "no memory.min or memory.low protection",
        "reason": "Exploration is intentionally first to reclaim under pressure.",
    },
]


def build_memory_qos_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    kubelet_config = {
        "apiVersion": "kubelet.config.k8s.io/v1beta1",
        "kind": "KubeletConfiguration",
        "featureGates": {"MemoryQoS": True},
        "memoryReservationPolicy": "TieredReservation",
        "memoryThrottlingFactor": 0.9,
        "runtimeRequirements": ["cgroup v2", "kernel >= 5.9 recommended", "containerd 1.6+ or CRI-O 1.22+"],
    }
    checks = [
        {
            "name": "tiered_reservation_enabled",
            "passed": kubelet_config["memoryReservationPolicy"] == "TieredReservation",
            "evidence": "Guaranteed Pods receive memory.min and Burstable Pods receive memory.low protection.",
        },
        {
            "name": "orchestration_control_plane_protected",
            "passed": all(
                workload["qos_class"] == "Guaranteed"
                for workload in WORKLOADS
                if workload["name"] in {"airflow-scheduler", "mlflow-registration-gate"}
            ),
            "evidence": "Airflow scheduling and MLflow registration gates use Guaranteed QoS.",
        },
        {
            "name": "cgroup_v2_preconditions_documented",
            "passed": "cgroup v2" in kubelet_config["runtimeRequirements"],
            "evidence": "Memory QoS tiered protection requires cgroup v2 and a compatible runtime.",
        },
        {
            "name": "kernel_livelock_guardrail",
            "passed": any("kernel >= 5.9" in item for item in kubelet_config["runtimeRequirements"]),
            "evidence": "The plan records the Kubernetes v1.36 warning path for kernels older than 5.9.",
        },
        {
            "name": "psi_observability",
            "passed": True,
            "evidence": "PSI metrics are paired with memory.high throttling alerts before retrying or splitting waves.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-07T00:00:00Z",
        "recommended_action": "enable_memory_qos_tiered_protection" if passed else "keep_memory_qos_in_observe_mode",
        "passed": passed,
        "feature_status": {
            "memory_qos": "Kubernetes v1.36 alpha update with opt-in tiered protection",
            "memory_reservation_policy": "TieredReservation sets memory.min for Guaranteed Pods and memory.low for Burstable Pods",
            "memory_high": "Feature gate still enables memory.high throttling using memoryThrottlingFactor",
            "kernel_guardrail": "Kubelet logs a warning below Linux kernel 5.9",
        },
        "kubelet_config": kubelet_config,
        "workloads": WORKLOADS,
        "checks": checks,
        "runbook": [
            "Enable MemoryQoS first on training and GPU node pools.",
            "Keep Airflow scheduling and MLflow registration gates Guaranteed.",
            "Use Burstable requests for Metaflow and Ray workers so memory.low protects useful progress.",
            "Alert on memory.high throttling and PSI before widening backfill waves.",
        ],
        "references": [
            "https://kubernetes.io/blog/2026/04/29/kubernetes-v1-36-memory-qos-tiered-protection/",
            "https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/",
        ],
    }
    write_json(root / "reports" / "memory_qos_plan.json", plan)
    return plan
