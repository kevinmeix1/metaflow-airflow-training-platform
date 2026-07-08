# MultiKueue Dispatch

`make multikueue-dispatch` writes `.local/reports/multikueue_dispatch_plan.json` and pairs it with `kubernetes/multikueue-dispatch.yaml`.

This is the training platform's multi-cluster dispatch plan. Airflow submits the same Kueue-managed training Job to the manager cluster. The manager owns the `ClusterQueue`, reserves quota, and lets the MultiKueue controller create remote Workloads on worker clusters. Once a worker fully admits the Workload, Kueue sets `status.clusterName`, resets `status.nominatedClusterNames`, and creates the remote Job with `kueue.x-k8s.io/prebuilt-workload-name` linking it to the admitted Workload.

## Why It Matters

Large training backfills often need more GPU capacity than one cluster can safely provide. MultiKueue gives the platform one admission surface while keeping execution capacity spread across worker clusters. That lets the team expand to regional GPU fleets, burst CPU-only feature generation, and recover failed partitions without teaching Airflow about every worker kubeconfig.

## Operating Model

- The manager cluster is not one of its own workers.
- Worker clusters mirror namespaces, LocalQueues, priority classes, image policies, and training secrets.
- Manager quota matches aggregate worker quota so jobs are neither throttled too early nor dispatched into impossible capacity.
- Incremental dispatch is the default for cost-aware training waves.
- `status.nominatedClusterNames` is watched while a Workload is pending.
- `status.clusterName` is recorded in run metadata after a worker admits the Workload.
- GPU worker clusters keep ProvisioningRequest admission enabled so physical autoscaler capacity is proven before a final worker is selected.

## Failure Recovery

If no worker sets `status.clusterName` inside the queue SLO, stop the Workload, shrink the wave, and requeue it to the regular `demand-training-queue`. If the worker Job is running but manager status is stale, check MultiKueue manager logs and worker RBAC for `workloads/status` permission. If cross-region spend spikes, constrain CPU-only feature generation to the CPU burst worker and reserve GPU workers for model families that actually need accelerators.

## References

- Kueue MultiKueue concept: <https://kueue.sigs.k8s.io/docs/concepts/multikueue/>
- MultiKueue setup: <https://kueue.sigs.k8s.io/docs/tasks/manage/setup_multikueue/>
- Kubernetes Job in Multi-Cluster: <https://kueue.sigs.k8s.io/docs/tasks/run/multikueue/job/>
