from __future__ import annotations

from pathlib import Path

from .io import write_json


SCALE_TO_ZERO_WORKLOADS = [
    {
        "name": "metaflow-gpu-training-worker",
        "target_ref": "Deployment/metaflow-gpu-training-worker",
        "min_replicas": 0,
        "max_replicas": 40,
        "metric_type": "External",
        "metric_name": "training_partition_queue_depth",
        "wake_threshold": 1,
        "cold_start_budget_seconds": 120,
        "scale_to_zero_allowed": True,
        "reason": "Partition workers are backlog-driven and can idle between scheduled training waves.",
    },
    {
        "name": "ray-hpo-worker-pool",
        "target_ref": "Deployment/ray-hpo-worker-pool",
        "min_replicas": 0,
        "max_replicas": 20,
        "metric_type": "External",
        "metric_name": "training_hpo_trial_backlog",
        "wake_threshold": 1,
        "cold_start_budget_seconds": 120,
        "scale_to_zero_allowed": True,
        "reason": "HPO trials are expensive and should reserve GPU workers only while trial backlog exists.",
    },
    {
        "name": "failed-partition-replay-worker",
        "target_ref": "Deployment/failed-partition-replay-worker",
        "min_replicas": 0,
        "max_replicas": 8,
        "metric_type": "Object",
        "metric_name": "failed_partition_replay_backlog",
        "metric_object": "Service/failed-partition-replay-queue",
        "wake_threshold": 1,
        "cold_start_budget_seconds": 60,
        "scale_to_zero_allowed": True,
        "reason": "Replay workers are incident-driven and should wake only when recovery partitions are queued.",
    },
]

PROTECTED_WORKLOADS = [
    {
        "name": "airflow-scheduler",
        "min_replicas": 2,
        "reason": "Scheduler and triggerer availability controls all training and backfill execution.",
    },
    {
        "name": "airflow-triggerer",
        "min_replicas": 2,
        "reason": "Deferrable tasks and asset watchers should not wait for HPA cold start.",
    },
    {
        "name": "mlflow-registration-gate",
        "min_replicas": 1,
        "reason": "Model registration and promotion gates are control-plane safety checks.",
    },
]


def build_hpa_scale_to_zero_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    feature_gate = {
        "name": "HPAScaleToZero",
        "minimum_version": "Kubernetes v1.36",
        "stage": "alpha",
        "default": "disabled",
        "requirement": "minReplicas=0 requires at least one Object or External metric in autoscaling/v2",
    }
    checks = [
        {
            "name": "feature_gate_documented",
            "passed": feature_gate["name"] == "HPAScaleToZero" and feature_gate["default"] == "disabled",
            "evidence": "The training platform treats scale-to-zero as an opt-in alpha feature.",
        },
        {
            "name": "all_zero_min_replicas_use_external_or_object_metrics",
            "passed": all(workload["metric_type"] in {"External", "Object"} for workload in SCALE_TO_ZERO_WORKLOADS),
            "evidence": "Elastic training workers wake from queue or replay backlog metrics rather than CPU metrics.",
        },
        {
            "name": "orchestration_control_plane_not_scaled_to_zero",
            "passed": not ({workload["name"] for workload in SCALE_TO_ZERO_WORKLOADS} & {item["name"] for item in PROTECTED_WORKLOADS}),
            "evidence": "Airflow scheduler, triggerer, and MLflow registration gate remain above zero replicas.",
        },
        {
            "name": "wake_metric_contract",
            "passed": all(workload["metric_name"] and workload["wake_threshold"] >= 1 for workload in SCALE_TO_ZERO_WORKLOADS),
            "evidence": "Every idleable training worker declares a metric adapter contract.",
        },
        {
            "name": "cold_start_budget_recorded",
            "passed": all(workload["cold_start_budget_seconds"] <= 120 for workload in SCALE_TO_ZERO_WORKLOADS),
            "evidence": "Backfill and HPO cold-start budgets are explicit before widening Airflow fanout.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-09T00:00:00Z",
        "recommended_action": "enable_hpa_scale_to_zero_for_elastic_training_workers" if passed else "keep_hpa_scale_to_zero_disabled",
        "passed": passed,
        "feature_status": {
            "hpa_scale_to_zero": "Kubernetes v1.36 alpha and disabled by default behind HPAScaleToZero",
            "metric_requirement": "minReplicas=0 is valid only with at least one Object or External metric",
            "api_version": "autoscaling/v2",
        },
        "feature_gate": feature_gate,
        "scale_to_zero_workloads": SCALE_TO_ZERO_WORKLOADS,
        "protected_workloads": PROTECTED_WORKLOADS,
        "checks": checks,
        "runbook": [
            "Enable HPAScaleToZero first for partition and replay workers in a non-production training pool.",
            "Verify external metrics remain available when all worker replicas are zero.",
            "Keep Airflow scheduler, triggerer, and MLflow registration gate above zero replicas.",
            "Hold new backfill waves if backlog is positive and desired replicas remain zero beyond the cold-start budget.",
        ],
        "references": [
            "https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/",
            "https://kubernetes.io/docs/reference/kubernetes-api/autoscaling/horizontal-pod-autoscaler-v2/",
            "https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/",
        ],
    }
    write_json(root / "reports" / "hpa_scale_to_zero_plan.json", plan)
    return plan
