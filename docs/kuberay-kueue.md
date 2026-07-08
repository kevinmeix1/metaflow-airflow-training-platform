# KubeRay and Kueue

`make kuberay-plan` writes `.local/reports/kuberay_capacity_plan.json` and pairs it with `kubernetes/kuberay-kueue-workloads.yaml`.

## What It Shows

- Kueue-admitted `RayJob` backfill waves for distributed feature generation and training.
- Elastic GPU worker bounds for large training waves.
- A lower-priority Ray quality sweep before expensive model-family training.
- Recovery-oriented replay capacity that does not reopen the full backfill.
- Airflow DAG tasks that submit Ray work and wait deferrably before champion selection.

## Production Notes

Metaflow remains the source of training artifacts and partition manifests. Ray provides bursty distributed execution for large waves, while Airflow owns scheduling, admission, retries, and release decisions. Kueue keeps Ray autoscaling bounded by quota so a large backfill cannot starve recovery or production serving work.

References: Kueue RayJob integration, Ray KubeRay with Kueue, and RayJob gang-scheduling examples.
