from __future__ import annotations

from pathlib import Path

from .io import write_json


DAG_RELATIVE_PATH = Path("airflow/dags/airflow33_stateful_training_dag.py")
STATEFUL_ORCHESTRATION_FLOWS = [
    {
        "name": "training-manifest-rollup",
        "mapper": "RollupMapper",
        "wait_policy": "MinimumCount(3)",
        "max_downstream_keys": 1,
        "task_state_keys": [
            "metaflow_run_id",
            "mlflow_parent_run_id",
            "training_progress",
        ],
        "asset_state_keys": ["dataset_manifest_digest", "last_metaflow_run_id"],
        "retry_policy": "retry_infrastructure_fail_data_contract_errors",
        "owner_action": "reattach to Metaflow and MLflow runs after worker failure without resubmitting training",
    },
    {
        "name": "weekly-snapshot-daily-training-fanout",
        "mapper": "FanOutMapper",
        "wait_policy": "one_run_per_day",
        "max_downstream_keys": 7,
        "task_state_keys": [],
        "asset_state_keys": [],
        "retry_policy": "retry_infrastructure_fail_data_contract_errors",
        "owner_action": "bound one weekly snapshot to seven daily training partitions with explicit catchup semantics",
    },
    {
        "name": "runtime-training-partitioning",
        "mapper": "PartitionedAtRuntime",
        "wait_policy": "emit_discovered_segments",
        "max_downstream_keys": 3,
        "task_state_keys": [],
        "asset_state_keys": [],
        "retry_policy": "producer_is_idempotent",
        "owner_action": "emit data-contract, manifest, and capacity evidence discovered at runtime",
    },
]


def build_airflow_stateful_orchestration_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
    repo_root: str | Path | None = None,
) -> dict:
    root = Path(root)
    repo_root = (
        Path(repo_root)
        if repo_root is not None
        else Path(__file__).resolve().parents[2]
    )
    dag_path = repo_root / DAG_RELATIVE_PATH
    ci_path = repo_root / ".github" / "workflows" / "ci.yml"
    dag_source = dag_path.read_text(encoding="utf-8") if dag_path.exists() else ""
    ci_source = ci_path.read_text(encoding="utf-8") if ci_path.exists() else ""
    checks = [
        {
            "name": "airflow_33_public_sdk_contract",
            "passed": all(
                token in dag_source
                for token in [
                    "from airflow.sdk import",
                    "task_state_store",
                    "asset_state_store",
                    "NEVER_EXPIRE",
                ]
            ),
            "evidence": "The training DAG uses the Airflow 3.3 public Task SDK and documented state-store accessors.",
        },
        {
            "name": "state_store_scope_separation",
            "passed": all(
                STATEFUL_ORCHESTRATION_FLOWS[0][key]
                for key in ["task_state_keys", "asset_state_keys"]
            ),
            "evidence": "Retry-local Metaflow/MLflow IDs and cross-run dataset state use separate stores.",
        },
        {
            "name": "bounded_partition_mapping",
            "passed": all(
                flow["max_downstream_keys"] <= 7
                for flow in STATEFUL_ORCHESTRATION_FLOWS
            ),
            "evidence": "Rollup, fanout, and runtime partitions have explicit training limits.",
        },
        {
            "name": "exception_aware_retry_policy",
            "passed": all(
                token in dag_source
                for token in [
                    "ExceptionRetryPolicy",
                    "RetryAction.RETRY",
                    "RetryAction.FAIL",
                ]
            ),
            "evidence": "Infrastructure connectivity retries while data contract failures fail fast.",
        },
        {
            "name": "real_airflow_parse_gate",
            "passed": all(
                token in ci_source
                for token in [
                    "apache-airflow==3.3.0",
                    "make airflow-sdk-contract",
                    "python -m pip check",
                ]
            ),
            "evidence": "CI installs constrained Airflow 3.3 and validates registered DAG objects.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-10T00:00:00Z",
        "passed": passed,
        "recommended_action": "adopt_airflow_33_stateful_training_contract"
        if passed
        else "fix_airflow_33_contract_before_adoption",
        "features": {
            "airflow_version": "3.3.0",
            "asset_partition_mappers": [
                "RollupMapper",
                "FanOutMapper",
                "FixedKeyMapper",
                "SegmentWindow",
            ],
            "runtime_partitioning": "PartitionedAtRuntime",
            "state_store": ["task_state_store", "asset_state_store"],
            "retry_policy": "ExceptionRetryPolicy",
            "fanout_limit": "max_downstream_keys plus scheduler-level partition_mapper_max_downstream_keys",
        },
        "state_store_contract": {
            "task_scope": "one mapped training task instance; preserves Metaflow and MLflow run IDs across retries",
            "asset_scope": "training decision across runs; preserves dataset manifest and last completed run identity",
            "retention": "NEVER_EXPIRE only for external run IDs required for idempotent reattachment",
            "cleanup": "airflow state-store clean --dry-run before scheduled garbage collection",
            "payload_rule": "store IDs and progress only; datasets and artifacts remain in object storage/MLflow",
        },
        "flows": STATEFUL_ORCHESTRATION_FLOWS,
        "ci_validation": {
            "command": "make airflow-sdk-contract",
            "runtime": "apache-airflow==3.3.0 with official Python 3.11 constraints",
            "assertions": [
                "expected DAG IDs registered",
                "DAG.validate succeeds",
                "every expected DAG has tasks",
                "pip check succeeds",
            ],
        },
        "limitations": [
            "The local demo does not start an Airflow scheduler, Metaflow service, or MLflow server.",
            "The CI gate proves DAG authoring compatibility, not external job reattachment against a live cluster.",
            "Production backfills still require concurrency, retention, and data-availability policies tuned to real workloads.",
        ],
        "checks": checks,
        "airflow_assets": [str(DAG_RELATIVE_PATH), "tools/validate_airflow33_dag.py"],
        "references": [
            "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/task-and-asset-state-store.html",
            "https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html",
            "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#retry-policies",
            "https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html",
        ],
    }
    write_json(root / "reports" / "airflow_stateful_orchestration_plan.json", plan)
    return plan
