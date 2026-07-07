from __future__ import annotations

from pathlib import Path

from .io import write_json


def build_disaster_recovery_plan(root: str | Path) -> dict:
    root = Path(root)
    plan = {
        "platform": "metaflow-airflow-training-platform",
        "rpo_minutes": 30,
        "rto_minutes": 120,
        "backup_policy": {
            "cluster_objects": "Velero training namespace backup every 30 minutes",
            "persistent_volumes": "CSI VolumeSnapshot with Retain deletion policy",
            "airflow_metadata": "Postgres logical dump before DAG or Airflow upgrades",
            "partition_manifests": "object-store versioning for immutable manifests and lineage",
        },
        "restore_sequence": [
            {"order": 1, "asset": "namespace and batch CRDs", "validation": "Kueue and Job APIs available"},
            {"order": 2, "asset": "Airflow metadata database", "validation": "DAG history and pools restored"},
            {"order": 3, "asset": "partition manifests and lineage", "validation": "content fingerprints match"},
            {"order": 4, "asset": "MLflow artifacts", "validation": "training run artifacts resolve"},
            {"order": 5, "asset": "backfill replay", "validation": "single partition replay succeeds idempotently"},
        ],
        "drills": [
            "restore into ml-training-restore namespace monthly",
            "run one smoke partition before unpausing all DAGs",
            "compare restored lineage graph with pre-backup asset catalog",
        ],
    }
    write_json(root / "reports" / "disaster_recovery_plan.json", plan)
    return plan
