# DRA Resource Health Status For Training

`make resource-health-status` writes `.local/reports/resource_health_status_plan.json` and pairs it with `kubernetes/dra-resource-health-status.yaml`.

## What It Shows

- Kubernetes v1.36 `ResourceHealthStatus` for DRA device health in Pod status.
- `ResourceClaim` `status.devices` as companion evidence for training and HPO accelerator claims.
- Kubelet `PodResourcesLister` and `DynamicResource` telemetry as the runtime cross-check.
- `DeviceTaintRule` quarantine for unhealthy shared L4 training devices.
- Airflow fanout decisions when a partition GPU is `Unhealthy` or an HPO sweep device becomes `Unknown`.

## Production Notes

Training failures should not all collapse into "retry the task." A bad accelerator can create noisy retry storms, waste quota, and hide the actual root cause. This runbook first inspects `status.containerStatuses[*].allocatedResourcesStatus`, compares it with `ResourceClaim.status.devices`, then correlates the claim with kubelet PodResourcesLister telemetry.

The platform freezes mapped Airflow fanout while device health is unhealthy or unknown, quarantines the bad shared GPU, and replays only affected partitions through the deterministic CPU path. That keeps recovery targeted without invalidating the whole backfill.

## References

- Kubernetes Dynamic Resource Allocation: <https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/>
- Kubernetes v1.36 DRA update: <https://kubernetes.io/blog/2026/05/07/kubernetes-v1-36-dra-136-updates/>
- Kubernetes v1.36 release highlights: <https://github.com/kubernetes/sig-release/discussions/2958>
