# Training DRA AdminAccess Diagnostics

`make admin-access-diagnostics` writes `.local/reports/admin_access_diagnostics_plan.json` and pairs it with `kubernetes/dra-admin-access-diagnostics.yaml`.

## What It Shows

- Kubernetes v1.36 DRA `AdminAccess` ResourceClaims in a namespace labeled `resource.kubernetes.io/admin-access: "true"`.
- Break-glass diagnostics for GPU-backed backfills, HPO workers, and failed-partition replay.
- Least-privilege RBAC that separates privileged ResourceClaim creation from read-only training workload inspection.
- Evidence capture for `ResourceClaim.status.devices`, Pod `allocatedResourcesStatus`, Airflow map index, Metaflow run id, and MLflow run id.
- Cleanup deadlines and Prometheus alerts when privileged claims outlive their training incident window.

## Production Notes

AdminAccess is useful when training still has a live device allocation and the platform needs deeper evidence before tainting a device, rerouting a partition, or retrying HPO. The runbook keeps the privileged claim short-lived and makes deterministic CPU replay the safe recovery path while the accelerator pool is inspected.

The diagnostic output should be attached to the backfill summary, MLflow run tags, and OpenLineage facets so an interviewer can see the exact bridge between Airflow scheduling, Metaflow execution, MLflow tracking, and Kubernetes device state.

## References

- Kubernetes Dynamic Resource Allocation: <https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/>
- Kubernetes v1.36 release highlights: <https://github.com/kubernetes/sig-release/discussions/2958>
- KEP-5018 DRA Admin Access: <https://www.kubernetes.dev/resources/keps/5018/>
