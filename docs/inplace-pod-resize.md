# Training In-Place Pod Resize Controls

`make inplace-resize-plan` writes `.local/reports/inplace_resize_plan.json` and pairs it with `kubernetes/inplace-pod-resize.yaml`.

## What It Shows

- Kubernetes v1.35 stable in-place CPU and memory resizing through the `pods/resize` subresource.
- Kubernetes v1.36 beta in-place vertical scaling for pod-level resources through `spec.resources`.
- Training-specific resize policies for partition workers, distributed backfill workers, and failed-partition replay.
- VPA `InPlaceOrRecreate` wiring for autoscaler-compatible training recommendations.
- Alerts for `PodResizePending` and `PodResizeInProgress` so Airflow fanout does not expand while resources are unsettled.

## Production Notes

Partitioned training needs resource flexibility without losing lineage. In-place CPU boosts are useful for slow-starting Metaflow workers, but the platform should preserve Airflow map index, Metaflow run id, and partition date across the resize. Pod-level resizing is useful for multi-container workers, but `PodResizePending` must be treated as a capacity signal before widening mapped task fanout.

The demo also keeps failed-partition replay warm by shrinking idle pods rather than deleting the recovery path.

## References

- Kubernetes v1.35 in-place Pod Resize GA: <https://kubernetes.io/blog/2025/12/19/kubernetes-v1-35-in-place-pod-resize-ga/>
- Kubernetes v1.36 pod-level resource resize beta: <https://kubernetes.io/blog/2026/04/30/kubernetes-v1-36-inplace-pod-level-resources-beta/>
- Kubernetes resize container resources task: <https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/>
