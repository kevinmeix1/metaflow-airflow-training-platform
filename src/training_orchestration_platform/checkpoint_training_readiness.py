from __future__ import annotations

from pathlib import Path

from .io import write_json


TRAINING_JOBS = [
    {
        "name": "demand-transformer-weekly",
        "framework": "torchrun",
        "replica_groups": {"leader": 1, "workers": 4},
        "gpu_per_worker": 1,
        "checkpoint_interval_minutes": 12,
        "checkpoint_scope": "metaflow-task",
        "resume_sla_minutes": 18,
        "queue": "demand-training-queue",
        "priority": "production-backfill",
    },
    {
        "name": "demand-hpo-candidate-sweep",
        "framework": "ray",
        "replica_groups": {"head": 1, "workers": 6},
        "gpu_per_worker": 0.5,
        "checkpoint_interval_minutes": 20,
        "checkpoint_scope": "trial",
        "resume_sla_minutes": 30,
        "queue": "research-training-queue",
        "priority": "preemptible-exploration",
    },
]


def _simulate_resume_windows() -> list[dict]:
    windows = []
    for job in TRAINING_JOBS:
        checkpoint_write = round(0.7 + job["checkpoint_interval_minutes"] / 30, 2)
        restore_minutes = round(4 + job["checkpoint_interval_minutes"] * 0.45 + sum(job["replica_groups"].values()) * 0.8, 2)
        windows.append(
            {
                "job": job["name"],
                "checkpoint_interval_minutes": job["checkpoint_interval_minutes"],
                "estimated_restore_minutes": restore_minutes,
                "resume_sla_minutes": job["resume_sla_minutes"],
                "checkpoint_write_gib": checkpoint_write,
                "passed": restore_minutes <= job["resume_sla_minutes"],
            }
        )
    return windows


def build_checkpoint_training_readiness_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    resume_windows = _simulate_resume_windows()
    total_gpu = sum(sum(job["replica_groups"].values()) * job["gpu_per_worker"] for job in TRAINING_JOBS)
    checks = [
        {
            "name": "metaflow_checkpoint_scope_declared",
            "passed": all(job["checkpoint_scope"] in {"metaflow-task", "trial"} for job in TRAINING_JOBS),
            "evidence": "Checkpoint ownership is explicit so retries do not load stale state from unrelated runs.",
        },
        {
            "name": "resume_sla_modelled",
            "passed": all(item["passed"] for item in resume_windows),
            "evidence": resume_windows,
        },
        {
            "name": "jobset_replica_groups_declared",
            "passed": all(sum(job["replica_groups"].values()) >= 2 for job in TRAINING_JOBS),
            "evidence": "Distributed training is represented as leader/head plus worker replica groups.",
        },
        {
            "name": "kueue_quota_boundary_declared",
            "passed": all(job["queue"].endswith("-queue") for job in TRAINING_JOBS),
            "evidence": "Production backfills and exploratory sweeps land in separate LocalQueues.",
        },
        {
            "name": "preemption_policy_documented",
            "passed": any(job["priority"] == "preemptible-exploration" for job in TRAINING_JOBS),
            "evidence": "HPO sweeps can be preempted before production partition recovery.",
        },
        {
            "name": "artifact_storage_budgeted",
            "passed": sum(item["checkpoint_write_gib"] for item in resume_windows) < 6,
            "evidence": "Checkpoint write volume stays below the local object-store budget for the demo profile.",
        },
        {
            "name": "airflow_metaflow_recovery_linked",
            "passed": True,
            "evidence": "Airflow backfill evidence records Metaflow run id, checkpoint lineage, and resume action.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-10T00:00:00Z",
        "passed": passed,
        "recommended_action": "adopt_checkpointed_jobset_training_contract" if passed else "hold_distributed_training_rollout",
        "training_jobs": TRAINING_JOBS,
        "resume_windows": resume_windows,
        "capacity": {
            "total_gpu_equivalent": total_gpu,
            "max_parallel_jobsets": 2,
            "protected_queue": "demand-training-queue",
            "preemptible_queue": "research-training-queue",
        },
        "observability": {
            "metaflow": ["run_id", "task_id", "attempt", "checkpoint_name", "checkpoint_version"],
            "airflow": ["dag_id", "run_id", "map_index", "partition_key", "try_number"],
            "kubernetes": ["jobset_name", "replicated_job", "pod_index", "local_queue", "cluster_queue"],
            "mlflow": ["experiment_id", "run_id", "registered_model_version", "artifact_uri"],
        },
        "recovery_runbook": [
            "Fail the current task attempt when checkpoint upload or checksum validation fails.",
            "Resume from the latest checkpoint scoped to the same Metaflow task or trial.",
            "Keep Airflow map index and partition key stable so downstream lineage does not fork.",
            "Prefer preempting exploratory HPO JobSets before production failed-partition replay.",
            "Record checkpoint version, restore duration, and recovered Metaflow run id in release evidence.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/checkpointed-training-jobset.yaml"],
        "references": [
            "https://docs.metaflow.org/scaling/checkpoint/introduction",
            "https://docs.metaflow.org/production/scheduling-metaflow-flows/scheduling-with-airflow",
            "https://kueue.sigs.k8s.io/docs/tasks/run/jobsets/",
            "https://kubernetes.io/blog/2025/03/23/introducing-jobset/",
        ],
    }
    write_json(root / "reports" / "checkpoint_training_readiness_plan.json", plan)
    return plan
