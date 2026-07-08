# Kueue Pending Workload Visibility

`make pending-workload-visibility` writes `.local/reports/pending_workload_visibility_plan.json` and pairs it with `kubernetes/kueue-pending-workload-visibility.yaml`.

## What It Shows

- Kueue `VisibilityOnDemand` for ClusterQueue and LocalQueue pending workload queries.
- RBAC for `visibility.kueue.x-k8s.io` `clusterqueues/pendingworkloads` and `localqueues/pendingworkloads`.
- API Priority and Fairness setup via the Kueue release `visibility-apf.yaml`.
- Prometheus signals for admission wait time and pending requested resources.
- Queue triage actions for production backfills, schema replay, failed-partition recovery, and HPO sweeps.

## Production Notes

Training orchestration breaks down when every stuck partition looks the same. Pending-workload visibility separates "first backfill wave is waiting on on-demand CPU" from "retryable schema replay is full on spot" and "HPO is waiting for idle GPU." That lets the operator decide whether to hold fanout, split replay, or keep experiments queued.

The demo records those queue snapshots beside Airflow backfill and Metaflow run evidence so failed-run recovery can explain why a partition was delayed before it decides to widen fanout or preempt lower-priority work.

## References

- Kueue monitor pending workloads: <https://kueue.sigs.k8s.io/docs/tasks/manage/monitor_pending_workloads/>
- Kueue pending workloads on demand: <https://kueue.sigs.k8s.io/docs/tasks/manage/monitor_pending_workloads/pending_workloads_on_demand/>
- Kueue Prometheus metrics: <https://kueue.sigs.k8s.io/docs/reference/metrics/>
