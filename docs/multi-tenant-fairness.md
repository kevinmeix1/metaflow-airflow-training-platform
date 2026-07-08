# Multi-Tenant Fairness

The demo writes `reports/tenancy_fairness_report.json`, which models training tenants for production forecasting, data quality, and feature exploration. It shows how partition backfills can share a cluster without letting exploratory work starve recovery jobs.

## Controls

- `ResourceQuota` and `LimitRange` cap namespace-level CPU, memory, and pod growth.
- Kueue `Cohort` and `ClusterQueue` resources let feature sweeps borrow idle quota while failed-partition recovery keeps priority.
- Airflow pools reserve backfill and recovery slots before mapped feature sweeps fan out.
- Cost-center labels make long-running training cost attributable.
- Default-deny `NetworkPolicy` separates exploratory jobs from production artifacts.

## References

- Kubernetes multi-tenancy: https://kubernetes.io/docs/concepts/security/multi-tenancy/
- Kubernetes ResourceQuota: https://kubernetes.io/docs/concepts/policy/resource-quotas/
- Kueue Cohorts: https://kueue.sigs.k8s.io/docs/concepts/cohort/
- Airflow Pools: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html
