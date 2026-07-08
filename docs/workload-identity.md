# Workload Identity and Secretless Access

This training platform models production access without static cloud keys in Airflow or Metaflow pods. The scheduler, partition worker, and MLflow registrar each get a dedicated Kubernetes `ServiceAccount`, namespace-scoped RBAC, projected one-hour tokens, and a federated cloud role.

## Controls

- `kubernetes/workload-identity.yaml` disables default service account token automounting and documents projected token expectations.
- `SecretStore` and `ExternalSecret` examples synchronize Airflow, MLflow, and registry tokens with a 30 minute refresh window.
- Airflow mapped tasks pin the `metaflow-partition-worker` identity so backfills do not inherit scheduler-level permissions.
- SPIFFE IDs document service identity boundaries for scheduler, worker, and registrar workloads.
- `.local/reports/identity_access_report.json` proves that token TTL, ExternalSecret refresh, RBAC scope, SPIFFE IDs, and static-key avoidance pass.

## Production Notes

Map these service accounts to cloud roles with separate data-read, artifact-write, and registry-promotion permissions. In a real cluster, production backfills should fail closed if a mapped task launches with the scheduler service account or if an ExternalSecret falls out of refresh.

References: Kubernetes service account token projection, External Secrets Operator, SPIFFE/SPIRE, and Airflow KubernetesPodOperator service-account configuration.
