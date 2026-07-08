# OCI Artifact Volumes

Kubernetes image volumes let a training pod mount an OCI image or artifact as a read-only filesystem. In this project they are used for immutable training inputs: dataset snapshots, feature contracts, candidate model bundles, and evaluation baselines.

## Why This Matters

Airflow and Metaflow can be perfectly orchestrated and still produce unreproducible training if workers download mutable artifacts at runtime. The image-volume pattern moves those artifacts into content-addressed OCI references, so the partition, feature contract, model bundle, and evaluation baseline can be reviewed as release inputs.

## Production Contract

- Kubernetes image volumes are stable in Kubernetes v1.36 and require a server at least v1.31 plus runtime support.
- Every artifact bundle is referenced by digest, not by `latest`.
- The pod mounts the bundles read-only and writes training outputs somewhere else.
- Airflow runs a small smoke job before expanding mapped Metaflow training tasks.
- A PVC/S3 fallback path stays available for clusters that do not yet support image volumes.
- Startup latency is watched because image-volume pull failures block container startup.

## Airflow Integration

The advanced DAG pins its training worker image and adds an artifact-locality smoke step in the capacity-admission phase:

```bash
kubectl apply -f kubernetes/oci-artifact-volumes.yaml
kubectl wait --for=condition=Complete job/training-artifact-volume-smoke -n ml-training --timeout=10m
```

Only after that smoke step passes should Airflow open the dynamic task mapping fanout for model families and domains.

## Failure Recovery

| Failure | Detection | Recovery |
| --- | --- | --- |
| Registry auth or pull failure | Pod stuck before containers start | Freeze the backfill wave and switch to the PVC/S3 artifact template. |
| Runtime lacks image-volume support | Kubelet or admission rejects `spec.volumes[*].image` | Keep initContainer downloads until nodes are upgraded. |
| Digest mismatch | Mounted manifest hash differs from governance evidence | Quarantine the partition and rebuild the artifact image through the attested workflow. |
| High cold-start latency | Startup p95 exceeds the backfill budget | Warm nodes with the cache CronJob before increasing Airflow mapped-task concurrency. |

## Interview Talking Points

- Why training reproducibility needs artifact immutability, not only Docker image pinning.
- How OCI artifact mounts differ from copying files in an initContainer.
- Why the fallback path matters during Kubernetes and container-runtime upgrades.
- How artifact locality changes Airflow fanout and Kubernetes startup latency budgets.
- Why read-only mounts force a clean split between input bundles and model outputs.
