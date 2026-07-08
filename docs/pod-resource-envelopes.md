# Pod Resource Envelopes

`make pod-resource-envelopes` writes `.local/reports/pod_resource_envelope_plan.json` and pairs it with `kubernetes/pod-resource-envelopes.yaml`.

## What It Shows

- Kubernetes `PodLevelResources` with pod-level `spec.resources` for manifest builders, Metaflow workers, and failed-partition replay pods.
- Stable Pod Scheduling Readiness through `schedulingGates`.
- Gate removal only after partition manifests, Kueue admission, OCI artifact volume readiness, DAG Bundle version pinning, and replay approvals exist.
- Scheduler observability with `scheduler_pending_pods{queue="gated"}`.
- Dynamic Resource Allocation guardrails so accelerator-backed training workers fit inside the pod-level envelope.

## Production Notes

Training backfills are especially prone to scheduler churn because a mapped task can fan out before manifests, artifact mounts, or quota admission are ready. Scheduling gates let Airflow create an auditable pod object without asking the scheduler to place it yet.

Pod-level resources make Metaflow workers easier to reason about when checkpoint writers, OpenTelemetry collectors, and lineage exporters share a pod. Use `PodLevelResourceManagers` when CPUManager, MemoryManager, or TopologyManager alignment matters for accelerator-heavy training waves.

## References

- Kubernetes pod-level resources: <https://kubernetes.io/docs/tasks/configure-pod-container/assign-pod-level-resources/>
- Kubernetes Pod Scheduling Readiness: <https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/>
- Kubernetes Dynamic Resource Allocation: <https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/>
