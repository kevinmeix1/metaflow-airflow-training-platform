# Airflow DAG Bundles

`make dag-bundle-plan` writes `.local/reports/dag_bundle_versioning_plan.json` and pairs it with `airflow/dag-bundle-config.ini`.

## What It Shows

- Airflow 3 `GitDagBundle` configuration for partitioned training DAGs.
- Bundle versioning kept on with `disable_bundle_versioning = False`.
- Reruns set to `rerun_with_latest_version = False` so failed partition replay uses the original DAG code.
- `sparse_dirs` includes Airflow DAGs, Metaflow flows, Kubernetes manifests, contracts, and source code.
- Git credentials referenced through `git_conn_id`, so tokens live in Airflow Connections or a secrets backend.
- Scheduler-managed backfills split into latest-code bulk backfills and original-bundle incident replay.

## Production Notes

Training reproducibility is more than fixing a random seed. A credible backfill record ties together the partition, DAG Bundle version, mapped task index, Metaflow run id, MLflow run id, OCI artifact digest, and model artifact hash.

This project treats failed-partition replay as forensic evidence. Latest-code backfills are useful for remediation, but they should be separate DAG runs so the incident record still explains the original failure.

## Failure Recovery

- If Git bundle refresh fails, restore the `github_dag_bundle` connection and refresh DAG processors before opening new backfill windows.
- If a bad DAG commit breaks mapped training tasks, revert the commit and launch a fresh backfill wave instead of rewriting the failed run evidence.
- If an artifact digest or bundle version mismatch appears, quarantine that partition and rerun it with the original bundle plus the recorded OCI artifact volume digests.

## References

- Airflow DAG Bundles: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html>
- Airflow `GitDagBundle`: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html#gitdagbundle>
- Airflow rerun behavior: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html#rerun-behavior>
