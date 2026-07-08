# Performance Budgets

The training platform writes `.local/reports/performance_budget.json` to turn orchestration behavior into a release gate. It focuses on partitioned backfills where Airflow, Metaflow, and Kubernetes most often fail in production.

## What Is Gated

- Successful partition count for the demo backfill.
- Idempotent skip count from checkpointed partition manifests.
- Backfill wave count and max wave CPU from the capacity planner.
- Airflow queue wait p95.
- Failed partition recovery time.

## Production Mapping

- Airflow dynamic task mapping fans out only work that still needs execution.
- Deferrable wait patterns avoid wasting workers while Kubernetes Jobs run.
- Kueue admits indexed batch jobs against fair-share quota.
- KEDA backlog scaling is kept separate from final admission so the platform does not launch more pods than the cluster can schedule.

## Current References

- Airflow dynamic task mapping: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html>
- Airflow deferrable tasks: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>
- Kubernetes resource management: <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/>
- KEDA Prometheus scaler: <https://keda.sh/docs/2.20/scalers/prometheus/>

Run `make performance-budget` after `make demo` to regenerate only this evidence.
