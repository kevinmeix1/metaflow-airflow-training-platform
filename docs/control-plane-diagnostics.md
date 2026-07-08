# Control Plane Diagnostics

`make control-plane-diagnostics` writes `.local/reports/control_plane_diagnostics_plan.json`.

## What It Shows

- Kubernetes v1.36 controller staleness mitigation for Airflow and Metaflow backfill automation.
- Component `/statusz` and `/flagz` readiness for API server, controller manager, scheduler, and kubelet.
- PSI metrics for CPU, memory, and IO stall detection on training nodes.
- native histogram readiness for high-resolution scheduler, backfill, and training latency metrics.
- Fail-closed behavior when backfill, model-registration, or failed-partition replay controllers read stale cache state.

## Production Notes

Partitioned training can over-expand or incorrectly clear a failed partition if controller cache state is stale. This plan gives backfill, registration, and replay controllers explicit freshness budgets and requires direct API reads before expanding the next wave or promoting a model.

`/statusz` shows the component build and health. `/flagz` shows the effective flags after an upgrade. Together they make Kubernetes feature-gate drift visible before Airflow and Metaflow automation trusts new scheduling, resource, security, or metrics behavior.

## Senior Review Angle

This is the operator layer for training orchestration: it shows how the platform detects stale watches, feature-gate drift, node pressure, and metrics-cardinality risk before those issues corrupt partition replay, model registration, or backfill decisions.

References:

- https://kubernetes.io/blog/2026/04/28/kubernetes-v1-36-staleness-mitigation-for-controllers/
- https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/
- https://kubernetes.io/blog/2026/04/25/kubernetes-v1-36-psi-metrics/
