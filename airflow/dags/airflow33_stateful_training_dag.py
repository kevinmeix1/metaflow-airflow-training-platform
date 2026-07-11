"""Airflow 3.3 stateful Metaflow training orchestration.

CI parses this module against Apache Airflow 3.3. The local dependency-light
demo does not start Airflow services, but this is executable DAG-authoring code.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import (
    NEVER_EXPIRE,
    Asset,
    DAG,
    ExceptionRetryPolicy,
    FanOutMapper,
    FixedKeyMapper,
    MinimumCount,
    PartitionedAssetTimetable,
    PartitionedAtRuntime,
    RetryAction,
    RetryRule,
    RollupMapper,
    SegmentWindow,
    StartOfWeekMapper,
    WeekWindow,
    asset,
    task,
)


AIRFLOW_33_DAG_IDS = {
    "stateful_training_manifest_rollup",
    "weekly_snapshot_daily_training_fanout",
}
TRAINING_SEGMENTS = ["data-contract", "dataset-manifest", "capacity-admission"]

TRAINING_RETRY_POLICY = ExceptionRetryPolicy(
    rules=[
        RetryRule(
            exception=ConnectionError,
            action=RetryAction.RETRY,
            retry_delay=timedelta(minutes=1),
            reason="Transient object-store, MLflow, or Kubernetes API failure",
        ),
        RetryRule(
            exception=ValueError,
            action=RetryAction.FAIL,
            reason="Data contract failures are terminal until the input is corrected",
        ),
    ],
)

TRAINING_EVIDENCE_SEGMENTS = Asset.ref(name="training_evidence_segments")
TRAINING_DECISION = Asset(
    uri="mlflow://models/daily-demand/stateful-training-decision",
    name="stateful_training_decision",
)
WEEKLY_DATASET_SNAPSHOT = Asset(
    uri="s3://ml-training/daily-demand/weekly-snapshot.parquet",
    name="weekly_demand_dataset_snapshot",
)


@asset(
    uri="s3://ml-training/daily-demand/evidence-segments.json",
    schedule=PartitionedAtRuntime(),
)
def training_evidence_segments(self, outlet_events) -> None:
    """Emit evidence partitions discovered while materializing a snapshot."""

    outlet_events[self].add_partitions(TRAINING_SEGMENTS)


with DAG(
    dag_id="stateful_training_manifest_rollup",
    schedule=PartitionedAssetTimetable(
        assets=TRAINING_EVIDENCE_SEGMENTS,
        default_partition_mapper=RollupMapper(
            upstream_mapper=FixedKeyMapper("training-ready"),
            window=SegmentWindow(TRAINING_SEGMENTS),
            wait_policy=MinimumCount(len(TRAINING_SEGMENTS)),
            max_downstream_keys=1,
        ),
    ),
    catchup=False,
    max_active_runs=1,
    params={"dataset_manifest_digest": "sha256:replace-at-trigger-time"},
    tags=["airflow-3.3", "state-store", "metaflow", "mlflow"],
) as stateful_training_manifest_rollup:

    @task(
        inlets=[TRAINING_EVIDENCE_SEGMENTS],
        outlets=[TRAINING_DECISION],
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_policy=TRAINING_RETRY_POLICY,
    )
    def checkpoint_training_submission(**context) -> dict[str, str]:
        task_store = context["task_state_store"]
        metaflow_run_id = task_store.get("metaflow_run_id")
        if metaflow_run_id is None:
            metaflow_run_id = f"metaflow:{context['run_id']}"
            task_store.set("metaflow_run_id", metaflow_run_id, retention=NEVER_EXPIRE)

        mlflow_parent_run_id = task_store.get(
            "mlflow_parent_run_id", default=f"mlflow:{context['run_id']}"
        )
        task_store.set(
            "mlflow_parent_run_id", mlflow_parent_run_id, retention=NEVER_EXPIRE
        )
        task_store.set(
            "training_progress",
            {"stage": "submitted", "attempt": context["ti"].try_number},
        )
        decision_store = context["asset_state_store"][TRAINING_DECISION]
        decision_store.set(
            "dataset_manifest_digest", context["params"]["dataset_manifest_digest"]
        )
        decision_store.set("last_metaflow_run_id", metaflow_run_id)
        return {
            "metaflow_run_id": metaflow_run_id,
            "mlflow_parent_run_id": mlflow_parent_run_id,
        }

    checkpoint_training_submission()


with DAG(
    dag_id="weekly_snapshot_daily_training_fanout",
    schedule=PartitionedAssetTimetable(
        assets=WEEKLY_DATASET_SNAPSHOT,
        default_partition_mapper=FanOutMapper(
            upstream_mapper=StartOfWeekMapper(),
            window=WeekWindow(),
            max_downstream_keys=7,
        ),
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=True,
    max_active_runs=2,
    tags=["airflow-3.3", "asset-fanout", "metaflow", "backfill"],
) as weekly_snapshot_daily_training_fanout:

    @task(
        inlets=[WEEKLY_DATASET_SNAPSHOT], retries=2, retry_policy=TRAINING_RETRY_POLICY
    )
    def train_daily_partition(dag_run=None) -> dict[str, str | None]:
        return {
            "partition_key": dag_run.partition_key if dag_run else None,
            "dataset_asset": WEEKLY_DATASET_SNAPSHOT.uri,
            "launch_mode": "bounded_metaflow_child_run",
        }

    train_daily_partition()
