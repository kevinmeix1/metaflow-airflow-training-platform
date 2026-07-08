# Airflow Multi-Team Readiness

`make multi-team-readiness` writes `.local/reports/multi_team_readiness_plan.json`.

## What It Shows

- `core.multi_team = True` in the Airflow preview profile.
- DAG Bundle `team_name` ownership for Metaflow training DAGs.
- Team-scoped pools with `airflow pools set ... --team-name`.
- Team-scoped variables and connections using `AIRFLOW_VAR__ML_TRAINING___...` and `AIRFLOW_CONN__ML_TRAINING___...`.
- Team-specific executor routing and `airflow triggerer --team-name ml-training`.
- `AssetAccessControl` with `producer_teams`, `consumer_teams`, and `allow_global=False` for cross-team training assets.

## Production Notes

Airflow multi-team support is still preview/experimental, so this project treats it as readiness evidence rather than a mandatory local dependency. In production, create `ml-training` before DAG bundle sync, run a team triggerer for deferrable Kueue and Kubernetes sensors, and keep Metaflow partition pools, MLflow credentials, and replay work scoped to the training team.

This is logical/resource isolation inside one Airflow deployment. For hard training tenant isolation, use separate Airflow deployments, separate metadata databases, and separate Kubernetes namespaces.

## Example Bootstrap

```bash
airflow teams create ml-training
airflow pools set metaflow_training_pool 12 "Metaflow partition training pool" --team-name ml-training
airflow pools set failed_partition_replay_pool 4 "Failed partition replay pool" --team-name ml-training
airflow triggerer --team-name ml-training
```

## Asset Filtering Contract

```python
from airflow.sdk import Asset
from airflow.sdk.definitions.asset import AssetAccessControl

training_partition_asset = Asset(
    "metaflow://sales-forecast/train/partition",
    access_control=AssetAccessControl(
        producer_teams={"ml-training"},
        consumer_teams={"ml-platform", "ml-observability"},
        allow_global=False,
    ),
)
```

## Senior Review Angle

The report explains how partitioned training backfills, failed-partition replay, MLflow registration, and model handoff can be owned by the training team without leaking raw training events to every Airflow team. It also keeps the preview limitation explicit.

References:

- https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/multi-team.html
- https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- https://airflow.apache.org/blog/airflow-3.2.0/
