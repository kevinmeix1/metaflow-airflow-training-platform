# Memory QoS Tiered Protection

`make memory-qos` writes `.local/reports/memory_qos_plan.json`.

## What It Shows

- Kubernetes v1.36 Memory QoS with `memoryReservationPolicy: TieredReservation`.
- cgroup v2 and kernel guardrails for training node pools.
- `memory.min` hard protection for Airflow scheduling and MLflow registration gates.
- `memory.low` soft protection for Metaflow partition workers and Ray HPO workers.
- PSI and `memory.high` throttling alerts before backfill retries or wave splitting.

## Production Notes

Training platforms can waste a lot of compute if memory pressure kills orchestration or registration tasks after expensive training work completes. This plan protects the control plane first, then gives useful soft protection to large training workers.

The v1.36 update separates throttling from reservation. Enabling `MemoryQoS` turns on `memory.high` throttling, while `TieredReservation` opts into `memory.min` and `memory.low` protection.

## Senior Review Angle

This shows that training reliability is not only retry logic. It also depends on node-level memory protection, realistic requests, cgroup v2 behavior, PSI signals, and separating critical orchestration from elastic training fanout.

References:

- https://kubernetes.io/blog/2026/04/29/kubernetes-v1-36-memory-qos-tiered-protection/
- https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/
