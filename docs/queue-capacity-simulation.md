# Queue Capacity Simulation

The local queue simulator writes `.local/reports/queue_simulation.json`. It models Kueue-style admission for indexed backfills, model-family sweeps, failed-partition recovery, GPU use, and Airflow pool slots.

## What It Demonstrates

- Backfill waves compete on CPU, memory, GPU, and pool slots.
- Failed-partition smoke replay can preempt low-priority feature sweeps.
- Kueue quota protects recovery work from exploratory training load.
- Pending experiments do not block deterministic partition recovery.

## Current References

- Kueue ClusterQueue borrowing and cohorts: <https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/>
- Kueue WorkloadPriorityClass: <https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/>
- Kueue preemption: <https://kueue.sigs.k8s.io/docs/concepts/preemption/>
- Airflow pools: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html>
- Kubernetes pod priority and preemption: <https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/>

Run `make queue-simulation` after `make demo` to regenerate only this report.
