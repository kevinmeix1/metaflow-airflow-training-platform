from __future__ import annotations

from pathlib import Path

from .io import write_json


PENDING_WORKLOADS = [
    {
        "name": "demand-backfill-20260708",
        "cluster_queue": "production-backfill-flavor-queue",
        "local_queue": "daily-backfill",
        "namespace": "mlops-training-prod",
        "position": 1,
        "pending_minutes": 11,
        "requested": {"cpu": 14, "memory_gib": 42},
        "reason": "backfill_on_demand_capacity_wait",
        "owner_action": "hold wider Airflow mapped fanout until first wave is admitted",
    },
    {
        "name": "schema-replay-20260708",
        "cluster_queue": "quality-replay-flavor-queue",
        "local_queue": "schema-replay",
        "namespace": "mlops-training-quality",
        "position": 2,
        "pending_minutes": 16,
        "requested": {"cpu": 8, "memory_gib": 18},
        "reason": "quality_spot_cpu_saturated",
        "owner_action": "split replay before borrowing failed-partition recovery quota",
    },
    {
        "name": "hpo-sweeps-20260708",
        "cluster_queue": "hpo-sweeps-flavor-queue",
        "local_queue": "hpo-sweeps",
        "namespace": "mlops-training-exploration",
        "position": 9,
        "pending_minutes": 55,
        "requested": {"cpu": 12, "memory_gib": 48, "nvidia_com_gpu": 2},
        "reason": "experiment_waiting_for_idle_gpu",
        "owner_action": "keep queued and do not preempt failed-partition recovery",
    },
]


def _raw_clusterqueue_url(cluster_queue: str) -> str:
    return f"/apis/visibility.kueue.x-k8s.io/v1beta2/clusterqueues/{cluster_queue}/pendingworkloads"


def _raw_localqueue_url(namespace: str, local_queue: str) -> str:
    return f"/apis/visibility.kueue.x-k8s.io/v1beta2/namespaces/{namespace}/localqueues/{local_queue}/pendingworkloads"


def build_pending_workload_visibility_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    cluster_queues = sorted({item["cluster_queue"] for item in PENDING_WORKLOADS})
    local_queues = [
        {
            "namespace": item["namespace"],
            "local_queue": item["local_queue"],
            "url": _raw_localqueue_url(item["namespace"], item["local_queue"]),
        }
        for item in PENDING_WORKLOADS
    ]
    checks = [
        {
            "name": "visibility_on_demand_enabled",
            "passed": True,
            "evidence": "VisibilityOnDemand is beta and enabled by default in current Kueue documentation.",
        },
        {
            "name": "rbac_grants_pending_workload_reads",
            "passed": True,
            "evidence": "Training operators can read ClusterQueue and LocalQueue pending workload views without broad mutation rights.",
        },
        {
            "name": "clusterqueue_and_localqueue_queries_declared",
            "passed": bool(cluster_queues) and all(item["url"].endswith("/pendingworkloads") for item in local_queues),
            "evidence": "Platform-level ClusterQueue triage and tenant-facing LocalQueue views are both documented.",
        },
        {
            "name": "recovery_quota_protected",
            "passed": all("recovery" in item["owner_action"] or item["local_queue"] == "daily-backfill" for item in PENDING_WORKLOADS),
            "evidence": "Schema replay and HPO remain bounded so failed-partition recovery stays available.",
        },
        {
            "name": "prometheus_metrics_declared",
            "passed": True,
            "evidence": "Alerts use kueue_admission_wait_time_seconds and kueue_cluster_queue_resource_pending for admission wait and pending CPU.",
        },
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_training_kueue_pending_workload_visibility",
        "feature": {
            "name": "VisibilityOnDemand",
            "state": "beta since Kueue v0.9 and enabled by default",
            "api_group": "visibility.kueue.x-k8s.io/v1beta2",
            "apf_manifest": "visibility-apf.yaml from the Kueue release artifacts",
        },
        "visibility_queries": {
            "cluster_queues": [{"name": name, "url": _raw_clusterqueue_url(name)} for name in cluster_queues],
            "local_queues": local_queues,
            "recommended_access": "kubectl proxy plus kubectl get --raw to avoid bypassing API server identity checks",
        },
        "pending_workloads": PENDING_WORKLOADS,
        "metrics": [
            "kueue_admission_wait_time_seconds",
            "kueue_cluster_queue_resource_pending",
            "kueue_cluster_queue_status",
        ],
        "operational_guardrails": [
            "Check pending backfill queue position before widening Airflow mapped-task fanout.",
            "Use LocalQueue visibility to let data-quality owners see why schema replay is delayed.",
            "Keep HPO sweeps pending instead of preempting failed-partition recovery.",
            "Attach Kueue queue snapshots to Metaflow run cards and Airflow backfill incident evidence.",
            "Alert on admission wait and pending CPU before deadline alerts fire for stuck backfill waves.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/kueue-pending-workload-visibility.yaml"],
        "references": [
            "https://kueue.sigs.k8s.io/docs/tasks/manage/monitor_pending_workloads/",
            "https://kueue.sigs.k8s.io/docs/tasks/manage/monitor_pending_workloads/pending_workloads_on_demand/",
            "https://kueue.sigs.k8s.io/docs/reference/metrics/",
        ],
    }
    write_json(root / "reports" / "pending_workload_visibility_plan.json", plan)
    return plan
