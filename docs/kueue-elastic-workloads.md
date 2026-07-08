# Kueue Elastic Workloads

`make elastic-workload-plan` writes `.local/reports/elastic_workload_plan.json` and documents how the training platform would use Kueue Workload Slices for elastic backfill and distributed training waves.

## What It Shows

- Kueue `ElasticJobsViaWorkloadSlices` rollout plan with explicit feature-gate notes.
- JobSet integration for grouped distributed training and partitioned backfill jobs.
- Workload Slice annotations for original and replacement slices.
- Bounded scale-up and scale-down replicas for daily backfills and Ray GPU workers.
- Failed-partition recovery priority so urgent repair work can reclaim quota from elastic expansion.
- Prometheus alerts for pending slices, replacement lag, and JobSet replica lag.

## Production Notes

Elastic Workloads are powerful because they let Kueue change admitted job scale without suspending or requeueing the entire workload. That is a good fit for backfills: use spare quota to widen the historical wave, then shrink by replacement slice when failed-partition recovery or production training needs capacity back.

The operational risk is accounting drift. The runbook should compare Kueue admitted Workload Slices, JobSet replicas, and actual pods before widening the next backfill wave. If replacement admission fails, disable the feature gate and fall back to fixed-size JobSet waves.

## Current Research Basis

- Kueue Elastic Workloads use Workload Slices to track partial allocations during scale-up and scale-down.
- Kueue labels and annotations include workload-slice identifiers and replacement links.
- Kueue can schedule JobSet workloads by using the `kueue.x-k8s.io/queue-name` label.
- Kueue Workload objects represent the resource requirements that Kueue admits into a queue.
