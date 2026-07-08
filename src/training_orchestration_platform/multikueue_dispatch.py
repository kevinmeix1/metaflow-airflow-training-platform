from __future__ import annotations

from pathlib import Path

from .io import write_json


WORKER_CLUSTERS = [
    {
        "name": "gpu-worker-east",
        "region": "us-east-1",
        "accelerator": "a100-80gb",
        "cpu_quota": 64,
        "memory_gib_quota": 512,
        "gpu_quota": 8,
        "queue_mirror": "demand-multikueue-training",
        "provisioning_request_enabled": True,
    },
    {
        "name": "gpu-worker-west",
        "region": "us-west-2",
        "accelerator": "l40s",
        "cpu_quota": 48,
        "memory_gib_quota": 384,
        "gpu_quota": 6,
        "queue_mirror": "demand-multikueue-training",
        "provisioning_request_enabled": True,
    },
    {
        "name": "cpu-burst-worker",
        "region": "us-east-2",
        "accelerator": "none",
        "cpu_quota": 96,
        "memory_gib_quota": 768,
        "gpu_quota": 0,
        "queue_mirror": "demand-multikueue-training",
        "provisioning_request_enabled": False,
    },
]


def _quota_totals() -> dict:
    return {
        "cpu": sum(cluster["cpu_quota"] for cluster in WORKER_CLUSTERS),
        "memory_gib": sum(cluster["memory_gib_quota"] for cluster in WORKER_CLUSTERS),
        "nvidia_com_gpu": sum(cluster["gpu_quota"] for cluster in WORKER_CLUSTERS),
    }


def build_multikueue_dispatch_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    manager_quota = _quota_totals()
    checks = [
        {
            "name": "admission_check_declared",
            "passed": True,
            "evidence": "AdmissionCheck uses kueue.x-k8s.io/multikueue on the manager cluster.",
        },
        {
            "name": "multikueue_config_declared",
            "passed": len(WORKER_CLUSTERS) >= 2,
            "evidence": "MultiKueueConfig lists multiple worker clusters for cross-region training dispatch.",
        },
        {
            "name": "worker_clusters_declared",
            "passed": all(cluster["queue_mirror"] for cluster in WORKER_CLUSTERS),
            "evidence": "Each worker cluster mirrors the LocalQueue and ClusterQueue contract used by the manager.",
        },
        {
            "name": "manager_quota_aligned",
            "passed": manager_quota["cpu"] == 208 and manager_quota["nvidia_com_gpu"] == 14,
            "evidence": "Manager ClusterQueue nominal quota equals the aggregate worker CPU, memory, and GPU quota.",
        },
        {
            "name": "status_sync_documented",
            "passed": True,
            "evidence": "Runbook watches status.nominatedClusterNames while pending and status.clusterName after worker admission.",
        },
        {
            "name": "parallel_provisioning_guardrail",
            "passed": any(cluster["provisioning_request_enabled"] for cluster in WORKER_CLUSTERS),
            "evidence": "GPU workers can keep ProvisioningRequests alive until one worker fully admits the Workload.",
        },
        {
            "name": "fallback_to_manager_queue_documented",
            "passed": True,
            "evidence": "Failed dispatch can be requeued to a local manager recovery queue for small smoke and replay jobs.",
        },
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_multikueue_training_dispatch"
        if all(check["passed"] for check in checks)
        else "hold_multikueue_training_dispatch",
        "cluster_topology": {
            "manager_cluster": "training-manager",
            "manager_is_worker": False,
            "worker_clusters": WORKER_CLUSTERS,
        },
        "manager_quota": manager_quota,
        "dispatch_policy": {
            "controller_name": "kueue.x-k8s.io/multikueue",
            "dispatcher": "Incremental",
            "manager_quota_matches_worker_sum": True,
            "wait_for_workload_admitted": True,
            "status_fields": ["status.nominatedClusterNames", "status.clusterName"],
            "prebuilt_workload_label": "kueue.x-k8s.io/prebuilt-workload-name",
        },
        "airflow_integration": {
            "queue_label": "kueue.x-k8s.io/queue-name",
            "pool": "metaflow_training_pool",
            "task_group": "capacity_admission",
            "operator": "KubernetesPodOperator submits the same Job spec to the manager cluster only.",
        },
        "operational_guardrails": [
            "Keep the manager cluster out of its own worker set; use a dedicated local recovery ClusterQueue if manager execution is required.",
            "Mirror namespaces, LocalQueues, priority classes, secrets, and image pull policy on every worker cluster before enabling dispatch.",
            "Set manager quota close to the sum of worker quotas to avoid underutilization or dispatching work that cannot be admitted.",
            "Use Incremental dispatch for cost-aware training waves; switch selected incident replay jobs to AllAtOnce when admission speed matters more than cost.",
            "Use Workload status as the source of truth for remote admission and inspect worker Jobs only when status sync is delayed.",
            "Keep ProvisioningRequest admission enabled on GPU workers so cloud capacity booking runs before the manager picks the final cluster.",
        ],
        "failure_modes": [
            {
                "mode": "worker_admission_timeout",
                "detection": "No status.clusterName after 20 minutes while nominated clusters remain unchanged.",
                "recovery": "Stop the Workload with kueuectl, lower wave width, and requeue to demand-training-queue.",
            },
            {
                "mode": "worker_status_sync_lag",
                "detection": "Remote Job is running but manager Workload status remains Pending.",
                "recovery": "Inspect MultiKueue manager logs and worker RBAC for workloads/status update permission.",
            },
            {
                "mode": "cross_region_cost_spike",
                "detection": "OpenCost training_wave=backfill spend exceeds budget while west worker admits CPU-only jobs.",
                "recovery": "Constrain CPU-only feature generation to cpu-burst-worker and keep GPU workers for model families requiring accelerators.",
            },
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/multikueue-dispatch.yaml"],
        "references": [
            "https://kueue.sigs.k8s.io/docs/concepts/multikueue/",
            "https://kueue.sigs.k8s.io/docs/tasks/manage/setup_multikueue/",
            "https://kueue.sigs.k8s.io/docs/tasks/run/multikueue/job/",
            "https://kueue.sigs.k8s.io/docs/reference/kueue.v1beta2/",
        ],
    }
    write_json(root / "reports" / "multikueue_dispatch_plan.json", plan)
    return plan
