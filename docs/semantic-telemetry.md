# Semantic Telemetry Contract

`make semantic-telemetry-plan` writes `.local/reports/semantic_telemetry_plan.json` and validates the training lineage fields used by the demo traces.

## What It Shows

- Airflow DAG, run, pool, and backfill wave attributes on orchestration spans.
- Kueue queue and workload attributes for admission and pending-workload debugging.
- Metaflow flow, run, step, partition date, partition status, and retry count for recovery analysis.
- MLflow run, model name, model version, and artifact size for reproducibility.
- OpenLineage run and dataset attributes for downstream lineage inspection.
- Collector-side redaction of raw row samples, feature rows, request bodies, and customer identifiers.

## Production Notes

Training incidents usually become hard when the same partition is retried, skipped, recovered, and promoted through different systems. The contract keeps partition, queue, run, artifact, and dataset identifiers queryable while removing payload rows and identifiers before export.
