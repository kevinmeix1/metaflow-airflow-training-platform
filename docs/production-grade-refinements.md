# Production-Grade Refinements

This project focuses on orchestration maturity rather than model complexity.

## Airflow

- The DAG is asset-aware and keyed to the raw sales asset.
- Catchup remains enabled so historical partitions can be backfilled intentionally.
- `max_active_runs=2` protects shared training resources.
- Retries and retry delay are explicit.

## Partition Integrity

- Every raw partition writes a manifest with a SHA-256 content fingerprint.
- The manifest becomes an MLflow-style artifact, connecting model runs to immutable input evidence.
- Backfills skip successful partitions unless forced.
- Failure and recovery runs are both retained in run history.

## Lineage

- The asset catalog records raw partitions, model artifacts, and orchestration health.
- The lineage graph connects raw data, validation, Metaflow training, MLflow runs, and serving artifacts.

## Why This Matters

Senior data and ML engineers are often judged by how they handle reruns, backfills, and failures. This project makes those operational semantics explicit and testable.
