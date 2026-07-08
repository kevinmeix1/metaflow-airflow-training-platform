# Event-Driven Assets

`make event-driven-assets` writes `.local/reports/event_driven_assets_plan.json`.

## What It Shows

- Airflow 3 event-driven scheduling for raw sales arrival, partition manifest readiness, and candidate model registration.
- `AssetWatcher` contracts for object-store prefixes, partition manifest updates, and MLflow registry webhooks.
- `BaseEventTrigger` compatibility so training catchup does not accidentally create rescheduling loops.
- `shared_stream_key` planning so sibling training DAGs can share object-store and registry polling.
- conditional asset expression: `(RAW_SALES & PARTITION_MANIFESTS) | FAILED_PARTITION_REPLAY`.
- `AssetAlias` usage for runtime Metaflow artifacts and MLflow candidate model URIs.
- Queued asset event inspection and deletion steps for stale manifests and incident replays.

## Production Notes

Raw files alone should not start a retraining wave. The DAG should wait until a manifest records the complete partition set, hash, and expected row counts. A failed-partition replay remains an explicit incident action so recovery does not get hidden inside normal catchup.

Candidate model registration is tracked as the event that closes the training loop into promotion and serving handoff. That event should carry model version, artifact digest, source partition range, and Metaflow run id.

## References

- Airflow event-driven scheduling: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html>
- Airflow asset-aware scheduling: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html>
- Airflow asset definitions and AssetAlias: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html>
