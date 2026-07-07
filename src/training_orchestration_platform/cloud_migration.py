from __future__ import annotations

from pathlib import Path

from .io import write_json


def build_cloud_migration_plan(root: str | Path) -> dict:
    root = Path(root)
    plan = {
        "platform": "metaflow-airflow-training-platform",
        "primary_target": "AWS EKS Auto Mode",
        "managed_service_mapping": {
            "orchestration": "Amazon MWAA or Airflow Helm chart on EKS",
            "training_compute": "EKS Auto Mode managed NodePools for partition jobs",
            "metaflow_artifacts": "S3 datastore with versioned partition manifests",
            "model_tracking": "MLflow on RDS PostgreSQL with S3 artifact storage",
            "queueing": "Kueue on EKS for batch admission and fair sharing",
            "monitoring": "Amazon Managed Service for Prometheus and Grafana",
        },
        "migration_phases": [
            {"phase": "foundation", "tasks": ["provision EKS", "enable IRSA", "create S3 buckets", "install Airflow chart"]},
            {"phase": "training", "tasks": ["install Kueue", "apply training NodePools", "configure Metaflow datastore", "connect MLflow"]},
            {"phase": "operations", "tasks": ["run smoke partition", "run controlled backfill", "publish SLO and governance evidence"]},
        ],
        "portability_controls": [
            "partition manifests use object-store paths and content hashes",
            "Airflow owns schedule and backfill policy while Metaflow owns artifact boundaries",
            "Kubernetes batch specs remain provider-neutral",
            "cloud-specific IAM and storage live under infra/terraform/aws",
        ],
        "cost_controls": [
            "use spot-capable NodePools for retryable partition training",
            "admit bulk backfills through Kueue quotas",
            "expire raw partition snapshots with S3 lifecycle policies",
            "use SLO burn to pause expensive backfills during instability",
        ],
    }
    write_json(root / "reports" / "cloud_migration_plan.json", plan)
    return plan
