# Release Admission Control

This project writes `reports/release_admission_decision.json`, a fail-closed admission record for Airflow and Metaflow training backfills. It combines SLO burn, performance budgets, Kueue and Airflow queue safety, governance approval, supply-chain provenance, and the backfill capacity plan.

The controller distinguishes between broad backfill expansion and recovery work. If the SLO budget is stressed, the action becomes `throttle_bulk_backfill`; if failed-partition recovery capacity is threatened, the action becomes `reserve_failed_partition_recovery`. A normal wave is admitted only when every check passes.

## Production Shape

- Airflow treats the decision as an asset before expanding mapped partition tasks.
- Kubernetes `ValidatingAdmissionPolicy` requires release-decision and evidence-sha annotations on training Jobs.
- Argo Rollouts analysis gates training control-plane image changes on SLO burn and Airflow queue wait.
- Kueue priority keeps failed-partition recovery ahead of exploratory feature sweeps and bulk backfill work.

## Why This Is Senior-Level

Backfills are operationally risky because they turn old data, large compute, and retry logic into one blast radius. This controller gives reviewers a concrete story: partition training is idempotent, capacity planned, admitted through batch policy, and blocked by the same evidence that powers the dashboard.

## Current References

- Kubernetes `ValidatingAdmissionPolicy`: https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/
- Argo Rollouts analysis: https://argo-rollouts.readthedocs.io/en/stable/features/analysis/
- Airflow assets: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html
- Kueue workload priorities: https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/
