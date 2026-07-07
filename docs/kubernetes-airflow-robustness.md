# Kubernetes And Airflow Robustness Layer

This repo now models a partitioned training mesh suitable for larger backfills.

## Airflow Features

- Enterprise training mesh DAG.
- Dynamic task mapping across domains and model families.
- TaskGroups for validation and Metaflow child-flow launch.
- Branching for champion selection and partition quarantine.
- KubernetesPodOperator execution.
- Deferrable KubernetesPodOperator settings for long-running partition workers.
- Capacity-admission TaskGroup before validation and training fanout.
- Asset-aware scheduling and lineage outlets.

## Kubernetes Features

- Indexed CronJob for parallel partition workers.
- Kueue ResourceFlavor, ClusterQueue, LocalQueue, and WorkloadPriorityClass for fair backfill admission.
- ResourceQuota and LimitRange for compute governance.
- PriorityClass for backfill and recovery jobs.
- Airflow KubernetesExecutor pod template with init container, emptyDir workspace, nodeSelector, and tolerations.
- Namespace, service account, RBAC, pod security labels, and PodDisruptionBudget.

## Why It Matters

Large model-training workflows are mostly hard because of reruns, backfills, resource contention, and traceability. This repo now shows those concerns directly.

The latest pass adds queue-aware training admission: backfills carry a queue label, a workload priority, indexed-completion settings, and bounded per-index retries. This is the kind of control that prevents one large backfill from overwhelming shared Kubernetes capacity.
