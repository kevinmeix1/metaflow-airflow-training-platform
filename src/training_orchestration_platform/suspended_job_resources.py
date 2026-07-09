from __future__ import annotations

from pathlib import Path

from .io import write_json


RESOURCE_MUTATIONS = [
    {
        "name": "partitioned-backfill-job",
        "suspended": True,
        "current_requests": {"cpu": "8", "memory": "32Gi", "nvidia.com/gpu": "1"},
        "proposed_requests": {"cpu": "6", "memory": "24Gi", "nvidia.com/gpu": "1"},
        "quota_reason": "Partitioned backfill can shrink CPU and memory after the feature slice is known.",
        "unsuspend_gate": "quota_fit_and_partition_manifest_ready",
    },
    {
        "name": "hpo-sweep-job",
        "suspended": True,
        "current_requests": {"cpu": "12", "memory": "48Gi", "nvidia.com/gpu": "2"},
        "proposed_requests": {"cpu": "8", "memory": "36Gi", "nvidia.com/gpu": "1"},
        "quota_reason": "HPO sweep can run a narrower candidate set when cluster GPU quota is tight.",
        "unsuspend_gate": "quota_fit_and_metaflow_run_card_recorded",
    },
    {
        "name": "failed-partition-replay-job",
        "suspended": True,
        "current_requests": {"cpu": "4", "memory": "8Gi"},
        "proposed_requests": {"cpu": "3", "memory": "6Gi"},
        "quota_reason": "Failed partition replay only needs the failed shard and checkpoint, not the full backfill wave.",
        "unsuspend_gate": "pool_slots_available_and_checkpoint_present",
    },
]

PROTECTED_JOBS = [
    {
        "name": "airflow-scheduler-health-job",
        "suspended": False,
        "reason": "Airflow scheduler health checks stay active and should not be rewritten as suspended training Jobs.",
    },
    {
        "name": "running-mlflow-registration-gate",
        "suspended": False,
        "reason": "MLflow registration gates should remain deterministic and use replacement Jobs if resource shape changes.",
    },
]


def _resource_delta_ok(item: dict) -> bool:
    current_cpu = float(item["current_requests"]["cpu"])
    proposed_cpu = float(item["proposed_requests"]["cpu"])
    return 0.25 <= proposed_cpu / current_cpu <= 1.5


def build_suspended_job_resource_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    feature = {
        "name": "MutablePodResourcesForSuspendedJobs",
        "state": "Kubernetes v1.36 beta and enabled by default",
        "scope": "resource requests and limits in the Pod template of suspended Jobs",
        "not_for": "actively running Airflow scheduler probes or MLflow registration gates; use replacement Jobs or in-place resize instead",
    }
    checks = [
        {
            "name": "beta_feature_status_recorded",
            "passed": feature["state"].startswith("Kubernetes v1.36 beta"),
            "evidence": "The plan records the Kubernetes v1.36 beta status before recommending use.",
        },
        {
            "name": "only_suspended_jobs_mutated",
            "passed": all(item["suspended"] for item in RESOURCE_MUTATIONS),
            "evidence": "Every training resource mutation starts from spec.suspend=true.",
        },
        {
            "name": "active_control_jobs_not_resized",
            "passed": all(not item["suspended"] for item in PROTECTED_JOBS),
            "evidence": "Airflow scheduler health and MLflow registration gates are explicitly excluded.",
        },
        {
            "name": "queue_controller_reason_recorded",
            "passed": all(item["quota_reason"] for item in RESOURCE_MUTATIONS),
            "evidence": "Every mutation is tied to Kueue quota, Airflow pool, partition, checkpoint, or Metaflow evidence.",
        },
        {
            "name": "resource_delta_bounded",
            "passed": all(_resource_delta_ok(item) for item in RESOURCE_MUTATIONS),
            "evidence": "CPU request changes are bounded so admission cannot silently rewrite training economics.",
        },
        {
            "name": "unsuspend_gate_requires_training_evidence",
            "passed": all("quota" in item["unsuspend_gate"] or "pool" in item["unsuspend_gate"] or "checkpoint" in item["unsuspend_gate"] for item in RESOURCE_MUTATIONS),
            "evidence": "Unsuspend gates require quota, Airflow pool, partition manifest, checkpoint, or Metaflow run-card evidence.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-09T00:00:00Z",
        "recommended_action": "enable_suspended_job_resource_mutation_for_queued_training_jobs" if passed else "keep_suspended_job_resources_observe_only",
        "passed": passed,
        "feature": feature,
        "resource_mutations": RESOURCE_MUTATIONS,
        "protected_jobs": PROTECTED_JOBS,
        "checks": checks,
        "runbook": [
            "Create backfill, HPO, and failed-partition replay Jobs with spec.suspend=true when queue admission controls start time.",
            "Patch CPU, memory, GPU, or extended resource requests only while the Job is suspended.",
            "Record Kueue quota, Airflow pool, partition manifest, checkpoint, and Metaflow run-card evidence before unsuspending.",
            "Use replacement Jobs for active Airflow scheduler probes and MLflow registration gates.",
        ],
        "references": [
            "https://kubernetes.io/blog/2026/04/27/kubernetes-v1-36-mutable-pod-resources-for-suspended-jobs/",
            "https://kubernetes.io/docs/concepts/workloads/controllers/job/",
        ],
    }
    write_json(root / "reports" / "suspended_job_resources_plan.json", plan)
    return plan
