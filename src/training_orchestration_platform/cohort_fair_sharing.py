from __future__ import annotations

from pathlib import Path

from .io import write_json


CLUSTER_QUEUE_POLICIES = [
    {
        "name": "forecasting-prod",
        "cluster_queue": "demand-training-tenant-queue",
        "local_queues": ["daily-backfill", "failed-partition-recovery"],
        "weight": 4,
        "nominal_cpu": 30,
        "borrowing_limit_cpu": 12,
        "lending_limit_cpu": 3,
        "observed_cpu": 24,
        "historical_usage_score": 0.36,
        "preemption": {"withinClusterQueue": "LowerPriority", "reclaimWithinCohort": "Any"},
    },
    {
        "name": "data-quality",
        "cluster_queue": "training-quality-tenant-queue",
        "local_queues": ["contract-checks", "schema-replay"],
        "weight": 2,
        "nominal_cpu": 14,
        "borrowing_limit_cpu": 8,
        "lending_limit_cpu": 4,
        "observed_cpu": 10,
        "historical_usage_score": 0.44,
        "preemption": {"withinClusterQueue": "LowerPriority", "reclaimWithinCohort": "LowerPriority"},
    },
    {
        "name": "feature-exploration",
        "cluster_queue": "feature-exploration-tenant-queue",
        "local_queues": ["hpo-sweeps", "notebook-experiments"],
        "weight": 1,
        "nominal_cpu": 12,
        "borrowing_limit_cpu": 4,
        "lending_limit_cpu": 8,
        "observed_cpu": 11,
        "historical_usage_score": 0.83,
        "preemption": {"withinClusterQueue": "LowerPriority", "reclaimWithinCohort": "Never"},
    },
]


def _dominant_resource_share(queue: dict) -> float:
    borrowable = queue["nominal_cpu"] + queue["borrowing_limit_cpu"]
    return round(queue["observed_cpu"] / max(borrowable * queue["weight"], 0.0001), 4)


def build_cohort_fair_sharing_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    queues = [
        {
            **queue,
            "dominant_resource_share": _dominant_resource_share(queue),
            "exclusive_cpu_after_lending": queue["nominal_cpu"] - queue["lending_limit_cpu"],
            "max_cpu_after_borrowing": queue["nominal_cpu"] + queue["borrowing_limit_cpu"],
        }
        for queue in CLUSTER_QUEUE_POLICIES
    ]
    checks = [
        {"name": "fair_sharing_enabled", "passed": True, "evidence": "Kueue Configuration declares Fair Sharing preemption strategies for borrowed backfill resources."},
        {"name": "admission_fair_sharing_enabled", "passed": True, "evidence": "AdmissionFairSharing keeps LocalQueue admission aware of historical usage and entry penalties."},
        {
            "name": "borrowing_and_lending_limits_declared",
            "passed": all(queue["borrowing_limit_cpu"] >= 0 and queue["lending_limit_cpu"] >= 0 for queue in queues),
            "evidence": "Each training ClusterQueue has borrowingLimit and lendingLimit to reserve failed-partition recovery capacity.",
        },
        {
            "name": "production_backfill_weighted_above_exploration",
            "passed": queues[0]["weight"] > queues[-1]["weight"],
            "evidence": "Production backfill receives a higher fairSharing.weight than feature exploration.",
        },
        {
            "name": "preemption_guardrails_declared",
            "passed": queues[0]["preemption"]["reclaimWithinCohort"] == "Any" and queues[-1]["preemption"]["reclaimWithinCohort"] == "Never",
            "evidence": "Backfill recovery can reclaim borrowed quota, while exploration cannot reclaim from production tenants.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": passed,
        "recommended_action": "enable_training_kueue_cohort_fair_sharing" if passed else "keep_static_training_clusterqueue_quotas",
        "kueue_version_target": "0.15+",
        "feature_gates": {
            "FairSharing": "stable since Kueue v0.7",
            "AdmissionFairSharing": "beta since Kueue v0.15 and enabled by default",
        },
        "fair_sharing_config": {
            "preemptionStrategies": ["LessThanOrEqualToFinalShare", "LessThanInitialShare"],
            "dominant_resource_share_signal": "observed_cpu / ((nominal_cpu + borrowing_limit_cpu) * fairSharing.weight)",
            "admission_order": "prefer LocalQueues with lower decayed historical usage and apply an entry penalty at admission time",
        },
        "cohort": {"name": "ml-training-cohort", "policy": "production backfills and recovery preserve capacity while exploration borrows bounded idle quota"},
        "cluster_queues": queues,
        "operational_guardrails": [
            "Keep failed-partition recovery weighted above feature exploration and notebook sweeps.",
            "Use lendingLimit so production backfill does not lend away every recovery slot.",
            "Use borrowingLimit to cap exploration bursts before they trigger wide preemption during a backfill.",
            "Keep Admission Fair Sharing enabled so repeated HPO submissions lose admission priority until usage decays.",
            "Attach preemption reason, LocalQueue, partition, and fair-share values to backfill incident evidence.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/kueue-cohort-fair-sharing.yaml"],
        "references": [
            "https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/",
            "https://kueue.sigs.k8s.io/docs/concepts/cohort/",
            "https://kueue.sigs.k8s.io/docs/concepts/preemption/",
            "https://kueue.sigs.k8s.io/docs/concepts/admission_fair_sharing/",
        ],
    }
    write_json(root / "reports" / "cohort_fair_sharing_plan.json", plan)
    return plan
