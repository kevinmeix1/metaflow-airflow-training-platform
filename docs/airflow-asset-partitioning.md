# Airflow Asset Partitioning

`make asset-partitioning-plan` writes `.local/reports/asset_partitioning_plan.json` and pairs it with the partition-aware examples inside `airflow/dags/enterprise_backfill_training_mesh_dag.py`.

## What It Shows

- Airflow 3.2 asset partitioning for dataset snapshots, Metaflow child flows, evaluation gates, and model registration.
- `CronPartitionTimetable` for scheduled daily training partitions.
- `PartitionedAssetTimetable` and `StartOfHourMapper` for aligned raw-data, manifest, OCI artifact, Metaflow, and MLflow partitions.
- `dag_run.partition_key` captured with Airflow run id, mapped task index, Metaflow run id, MLflow run id, OCI artifact digest, and OpenLineage facets.
- scheduler-managed partition backfills instead of broad training mesh replay.

## Production Notes

Training platforms often treat a failed partition as a whole-DAG problem. That creates noisy retries, inconsistent MLflow evidence, and expensive backfills. Partitioned assets make the unit of recovery explicit: one dataset snapshot, one domain, one model family, or one candidate evaluation partition.

The interview signal is that the project separates bulk backfill from forensic replay. A reviewer can see how data freshness, model quality, artifact immutability, and lineage all meet at the partition key before a model is registered.

## References

- Airflow 3.2 release announcement: <https://airflow.apache.org/blog/airflow-3.2.0/>
- Airflow release notes: <https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html>
- Airflow assets: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html>
