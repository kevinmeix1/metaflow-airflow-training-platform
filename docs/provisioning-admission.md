# Kueue Provisioning Admission

Kueue quota is a logical promise. A training job can hold enough quota and still sit unscheduled if the cluster has no matching GPU nodes. This project models the production control that closes that gap: a Kueue `AdmissionCheck` backed by `ProvisioningRequest`.

## Admission Flow

1. Airflow submits a partitioned training wave with a Kueue queue label and a bounded `provreq.kueue.x-k8s.io/maxRunDurationSeconds` annotation.
2. Kueue reserves ClusterQueue quota for the requested flavor.
3. The built-in provisioning admission controller creates a `ProvisioningRequest`.
4. Cluster Autoscaler checks real capacity, provisions nodes if possible, and reports the capacity state.
5. Kueue admits the workload only after the admission check is `Ready`.

The interview point is simple: quota reservation protects fairness, while ProvisioningRequest protects runtime reality.

## Controls

- `AdmissionCheck` uses `kueue.x-k8s.io/provisioning-request`.
- `ProvisioningRequestConfig` defines `provisioningClassName`, `managedResources`, retry backoff, `podSetMergePolicy`, and optional `podSetUpdates`.
- `admissionChecksStrategy` scopes provisioning checks to expensive flavors.
- Job annotations pass workload-specific runtime bounds to the generated ProvisioningRequest.
- Prometheus alerts catch pending admission, retry exhaustion, and booking expiry before Airflow backfills pile up.

## Failure Recovery

| Failure | Response |
| --- | --- |
| `Provisioned=false` for too long | Keep the workload suspended, page capacity owner, and preserve quota evidence. |
| `Failed=true` | Release quota, requeue with backoff, and try a fallback queue if the DAG recovery branch allows it. |
| `BookingExpired=true` | Re-run a small smoke wave before allowing a large backfill. |
| `CapacityRevoked=true` | Treat active work as an incident, stop promotion, and replay from the partition manifest. |

## Production Notes

- Use current Kueue `v1beta2` examples for the admission resources, while keeping older queue resources isolated until a real cluster migration validates all CRDs.
- Configure the cloud provider or Cluster Autoscaler provisioning class explicitly; `check-capacity.autoscaling.x-k8s.io` is the portable capacity-check class used in Kueue examples.
- Keep pending-admission SLOs separate from training-runtime SLOs because they are owned by different teams.
