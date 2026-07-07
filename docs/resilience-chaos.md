# Resilience and Chaos Drills

The training platform includes Chaos Mesh examples for partitioned batch training. The emphasis is recoverable failure: indexed jobs, idempotent manifests, retryable Metaflow steps, and Airflow backfill controls.

## Drills

- `partition_worker_kill`: kills one indexed training worker and expects per-index retry plus partition idempotency.
- `mlflow_latency`: injects artifact-path latency and expects retries and promotion quarantine.
- `backfill_cpu_pressure`: adds CPU pressure to batch nodes and expects capacity wave packing plus Kueue queue admission.

Run the local evidence generator:

```bash
make chaos-drill
```

Apply the cluster experiments after installing Chaos Mesh:

```bash
kubectl apply -f kubernetes/chaos-experiments.yaml
```

## Production Notes

- Use indexed Kubernetes Jobs for partitioned backfills so a single failed index can retry without repeating the full wave.
- Keep partition manifests immutable and content-hashed before retries.
- Couple each chaos run to a model promotion quarantine decision.
- Schedule recurring drills with `concurrencyPolicy: Forbid` to avoid overlapping with long backfills.

References: Chaos Mesh supports pod, network, stress, and scheduled experiments; Kubernetes disruption controls and indexed jobs help make batch recovery explicit.
