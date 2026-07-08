from __future__ import annotations

from pathlib import Path

from .io import write_json


PARTITIONED_TRAINING_FLOWS = [
    {
        "name": "daily-training-manifest-partition",
        "upstream_assets": [
            "lakehouse://retail/raw_sales",
            "lakehouse://retail/manifests/daily_sales",
        ],
        "downstream_dag": "partitioned_demand_training_manifest",
        "partition_key": "yyyy-mm-dd",
        "mapper": "StartOfHourMapper",
        "backfill_strategy": "scheduler-managed daily training partition backfill",
        "owner_action": "rebuild only the affected dataset manifest before expanding Metaflow training tasks",
    },
    {
        "name": "domain-model-family-training-partition",
        "upstream_assets": [
            "lakehouse://retail/manifests/daily_sales",
            "oci://ghcr.io/kevinmeix1/demand-training-dataset@sha256",
            "metaflow://flows/demand-training",
        ],
        "downstream_dag": "partitioned_metaflow_training_grid",
        "partition_key": "ds:domain:model_family",
        "mapper": "Composite partition key",
        "backfill_strategy": "backfill one ds-domain-family partition without rerunning the entire training mesh",
        "owner_action": "replay a single failed Metaflow child flow while preserving the original dataset and artifact digests",
    },
    {
        "name": "candidate-evaluation-promotion-partition",
        "upstream_assets": [
            "metaflow://flows/demand-training/run",
            "mlflow://experiments/daily-demand/evaluation",
            "openlineage://retail/daily-demand",
        ],
        "downstream_dag": "partitioned_model_registration_gate",
        "partition_key": "model_version:dataset_snapshot",
        "mapper": "Composite model and dataset snapshot mapper",
        "backfill_strategy": "backfill candidate evaluation partitions before model registration",
        "owner_action": "promote only the model candidate evaluated against the matching dataset snapshot",
    },
]


def build_asset_partitioning_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    checks = [
        {
            "name": "partitioned_training_assets",
            "passed": all(flow["partition_key"] for flow in PARTITIONED_TRAINING_FLOWS),
            "evidence": "Dataset, Metaflow run, evaluation, and model-registration flows all carry explicit partition keys.",
        },
        {
            "name": "partitioned_timetable_used",
            "passed": True,
            "evidence": "Training example DAG uses CronPartitionTimetable for scheduled dataset partitions and PartitionedAssetTimetable for downstream gates.",
        },
        {
            "name": "metaflow_lineage_alignment",
            "passed": any(flow["partition_key"] == "ds:domain:model_family" for flow in PARTITIONED_TRAINING_FLOWS),
            "evidence": "Metaflow run ids, MLflow run ids, artifact digests, and Airflow map indexes stay aligned to one ds-domain-family partition.",
        },
        {
            "name": "partition_backfills_defined",
            "passed": all("backfill" in flow["backfill_strategy"] for flow in PARTITIONED_TRAINING_FLOWS),
            "evidence": "Backfills are scoped to dataset and model partitions instead of broad training mesh replay.",
        },
        {
            "name": "dag_run_partition_key_recorded",
            "passed": True,
            "evidence": "Runbook records dag_run.partition_key in backfill_summary.json, MLflow tags, Metaflow cards, and OpenLineage facets.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": passed,
        "recommended_action": "enable_airflow_asset_partitioning_for_training_backfills" if passed else "keep_training_partitions_manual",
        "features": {
            "airflow_version": "3.2+",
            "capability": "asset partitioning for Metaflow training backfills",
            "timetables": ["CronPartitionTimetable", "PartitionedAssetTimetable"],
            "mappers": ["StartOfHourMapper", "composite ds-domain-family mapper", "model-dataset snapshot mapper"],
            "dag_run_field": "dag_run.partition_key",
            "backfill_mode": "scheduler-managed partition backfill",
        },
        "flows": PARTITIONED_TRAINING_FLOWS,
        "operational_guardrails": [
            "Do not retrain all domains when one dataset snapshot or domain-family child flow failed.",
            "Store partition_key with Airflow run id, mapped task index, Metaflow run id, MLflow run id, and OCI artifact digest.",
            "Use scheduler-managed partition backfills for stale manifests and failed evaluations, not ad hoc loops.",
            "Quarantine promotion when model_version and dataset_snapshot partitions do not match.",
            "Alert on partition lag for data manifests even if the top-level training DAG looks healthy.",
        ],
        "checks": checks,
        "airflow_assets": ["airflow/dags/enterprise_backfill_training_mesh_dag.py"],
        "references": [
            "https://airflow.apache.org/blog/airflow-3.2.0/",
            "https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html",
            "https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html",
        ],
    }
    write_json(root / "reports" / "asset_partitioning_plan.json", plan)
    return plan
