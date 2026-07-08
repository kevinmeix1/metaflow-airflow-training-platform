# Kubernetes Workload-Aware Scheduling

`make workload-aware-scheduling` writes `.local/reports/workload_aware_scheduling_plan.json`.

## What It Shows

- Kubernetes v1.36 `scheduling.k8s.io/v1alpha2` Workload and PodGroup readiness.
- `WorkloadWithJob` fixed-shape Indexed Job integration for Metaflow backfills and failed-partition replay.
- PodGroup atomic gang scheduling with `schedulingPolicy.gang.minCount`.
- Topology constraints for rack, zone, or host placement.
- Workload-aware preemption using PodGroup `priority` and `disruptionMode: PodGroup`.
- DRA ResourceClaim sharing at PodGroup scope for high-cardinality training workloads.
- A recovery boundary that keeps failed-partition replay schedulable when HPO or backfill gangs wait for capacity.

## Production Notes

Workload-Aware Scheduling is alpha in Kubernetes v1.36 and should be treated as readiness evidence. This repo uses it for fixed-shape training backfills, HPO sweeps, and failed-partition replay examples where all-or-nothing scheduling improves reproducibility and cost control.

The first production candidate is a fixed-shape Indexed Job where `.spec.parallelism == .spec.completions`, `.spec.completionMode` is `Indexed`, and the pod template does not set `schedulingGroup` manually. Elastic training shapes should stay on Kueue, JobSet, and Airflow pool controls until the upstream API graduates.

## Senior Review Angle

This demonstrates that training backfills and HPO sweeps are modeled as coherent workloads instead of individual pods fighting one by one. The report connects Airflow backfill gates, Metaflow partition lineage, Kueue admission, PodGroup scheduling, DRA ResourceClaims, failed-run recovery, and release evidence.

References:

- https://kubernetes.io/blog/2026/05/13/kubernetes-v1-36-advancing-workload-aware-scheduling/
- https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/
- https://kubernetes.io/blog/2026/05/07/kubernetes-v1-36-dra-136-updates/
