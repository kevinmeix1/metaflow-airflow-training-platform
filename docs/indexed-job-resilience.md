# Indexed Job Resilience

This project uses Airflow for orchestration and Kubernetes Jobs for finite partition work. The production hard part is not launching a batch job; it is making shard ownership, retries, partial failure, and backfill behavior predictable when hundreds of partitions are running.

## Kubernetes Controls

- `completionMode: Indexed` gives each partition worker a deterministic `JOB_COMPLETION_INDEX`.
- `backoffLimitPerIndex` limits retry storms per shard instead of letting one bad partition waste the whole wave.
- `maxFailedIndexes` stops pathological waves when too many shards fail.
- `podFailurePolicy` treats exit code `42` as `FailIndex`, exit codes `78` and `126` as `FailJob`, and `DisruptionTarget` as ignorable infrastructure churn.
- `successPolicy` lets the controller declare success after a quorum of shards while preserving failed-index evidence for targeted recovery.

The important interview point: per-index retry semantics let unrelated partitions keep running. Without them, one bad shard can delay a large backfill and inflate GPU or CPU cost.

## Airflow Backfill Create

Airflow owns the backfill lifecycle:

```bash
airflow backfill create \
  --dag-id enterprise_backfill_training_mesh \
  --from-date 2026-06-01 \
  --to-date 2026-06-07 \
  --reprocess-behavior failed \
  --max-active-runs 2 \
  --run-backwards
```

Use `--reprocess-behavior failed` for recovery, run a dry run first, and keep backfill concurrency lower than normal DAG concurrency so fresh partitions are not starved.

## Failure Semantics

| Failure | Policy | Outcome |
| --- | --- | --- |
| Bad data contract | `FailIndex` | Mark the shard failed and continue other indexes. |
| Missing image or command error | `FailJob` | Stop the whole wave because retrying is wasteful. |
| Node drain or preemption | `Ignore` | Do not charge the disruption against the retry budget. |
| Too many failed indexes | `maxFailedIndexes` | Stop remaining pods and trigger targeted recovery. |

## Recovery Flow

1. Inspect `status.failedIndexes` and `status.completedIndexes`.
2. Rebuild only failed partition manifests.
3. Launch Airflow backfill with failed-only reprocessing.
4. Keep Kueue quota and Airflow pools below the documented concurrency cap.
5. Attach the generated `indexed_job_resilience_plan.json` to the release evidence.
