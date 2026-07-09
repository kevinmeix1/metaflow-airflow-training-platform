# Suspended Job Resource Mutation

`make suspended-job-resources` writes `.local/reports/suspended_job_resources_plan.json`.

This project uses Kubernetes v1.36 `MutablePodResourcesForSuspendedJobs` for queued
training Jobs such as partitioned backfills, HPO sweeps, and failed-partition replay.
The feature allows resource requests and limits to change while a Job is still
`spec.suspend: true`, before Pods start or resume.

The training guardrail is strict: active Airflow scheduler health probes and MLflow
registration gates are excluded. Those controls should use replacement Jobs or
in-place Pod resize because suspended Job resource mutation is only for work that is
still under queue admission.

Operational gates before unsuspend:

- Kueue quota fit is recorded for CPU, memory, GPU, and extended resources.
- Airflow pool slots are available for the backfill wave.
- Partition manifest or failed-shard checkpoint exists.
- Metaflow run-card evidence records the modified resource shape.
- MLflow registration remains outside the mutable suspended Job path.

References:

- https://kubernetes.io/blog/2026/04/27/kubernetes-v1-36-mutable-pod-resources-for-suspended-jobs/
- https://kubernetes.io/docs/concepts/workloads/controllers/job/
