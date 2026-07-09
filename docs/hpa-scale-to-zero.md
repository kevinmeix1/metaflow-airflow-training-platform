# HPA Scale To Zero

`make hpa-scale-zero` writes `.local/reports/hpa_scale_to_zero_plan.json`.

## What It Shows

- Kubernetes v1.36 `HPAScaleToZero` as an alpha, disabled-by-default feature gate.
- `autoscaling/v2` HorizontalPodAutoscaler objects with `minReplicas: 0`.
- External and Object metric wakeups for partition training, HPO, and failed-partition replay workers.
- Protected replica floors for Airflow scheduler, Airflow triggerer, and MLflow registration gates.
- Cold-start guardrails before Airflow widens training fanout.

## Production Notes

Training platforms often waste money on idle workers, but the scheduler and model-registration gate are not optional capacity. This project scopes scale-to-zero to workers that have a durable backlog metric and can tolerate a short cold start.

The risky dependency is the metrics adapter. If queue-depth metrics disappear while workers are at zero, training appears idle even though partitions are waiting. The manifest adds alerts for missing metrics, failed wakeups, and cold-start budget breaches.

## Senior Review Angle

This demonstrates Kubernetes cost control without sacrificing orchestration reliability. It shows the difference between elastic batch capacity and control-plane services, understands the HPA scale-to-zero metric restriction, and documents the operational rollback path for stuck training queues.

References:

- https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/
- https://kubernetes.io/docs/reference/kubernetes-api/autoscaling/horizontal-pod-autoscaler-v2/
- https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/
