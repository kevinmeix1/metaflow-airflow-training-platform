# Dynamic Resource Allocation

`make device-plan` writes `.local/reports/device_allocation_plan.json` and pairs it with `kubernetes/dynamic-resource-allocation.yaml`.

## What It Shows

- Kubernetes `DeviceClass` and `ResourceClaimTemplate` resources for accelerator-backed training pods.
- Kueue queue and priority annotations for indexed backfill admission.
- Time-sliced L4 claims for preemptible smoke sweeps and feature-heavy partitions.
- MIG claims for memory-sensitive model families that need stronger isolation.
- CPU baseline fallback so failed partition recovery is not blocked by scarce accelerator capacity.

## Production Notes

DRA is strongest when the scheduler needs to choose a device by capability, health, sharing mode, or isolation requirement. For training orchestration, the important production decision is not only "GPU or no GPU"; it is whether the Airflow mapped task should be admitted at all, whether the Kueue queue has enough quota, and whether a failed partition can still recover on CPU.

Use time-slicing for low-risk, preemptible work. Use MIG or exclusive claims when noisy-neighbor risk or memory isolation matters. Treat pending `ResourceClaim` status as a backfill-throttling signal before increasing Airflow pool slots.

References: Kubernetes DRA docs, Kueue workload admission docs, NVIDIA GPU Operator sharing docs, and Airflow dynamic task mapping docs.
