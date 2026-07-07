# Kubernetes And Airflow Robustness Layer

This repo now models a partitioned training mesh suitable for larger backfills.

## Airflow Features

- Enterprise training mesh DAG.
- Dynamic task mapping across domains and model families.
- TaskGroups for validation and Metaflow child-flow launch.
- Branching for champion selection and partition quarantine.
- KubernetesPodOperator execution.
- Asset-aware scheduling and lineage outlets.

## Kubernetes Features

- Indexed CronJob for parallel partition workers.
- ResourceQuota and LimitRange for compute governance.
- PriorityClass for backfill and recovery jobs.
- Airflow KubernetesExecutor pod template with init container, emptyDir workspace, nodeSelector, and tolerations.
- Namespace, service account, RBAC, pod security labels, and PodDisruptionBudget.

## Why It Matters

Large model-training workflows are mostly hard because of reruns, backfills, resource contention, and traceability. This repo now shows those concerns directly.
