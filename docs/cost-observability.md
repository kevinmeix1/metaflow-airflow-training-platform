# Cost Observability and FinOps

`make cost-observability` writes `.local/reports/cost_observability_report.json` and validates the training platform cost-allocation contract.

## What It Shows

- OpenCost exporter metrics scraped by Prometheus every minute.
- Cost allocation by Airflow DAG, Airflow task, Metaflow flow, partition date, model family, and training wave.
- Separate budgets for daily training, historical backfills, KubeRay distributed training, and failed-partition recovery.
- Cost per successful partition as a unit-economics guardrail.
- Prometheus alerts for backfill budget overrun, idle GPU training spend, retry storms, and missing allocation labels.
- PVC artifact storage cost visibility for model and lineage evidence.

## Production Notes

Training cost failures often come from orchestration behavior rather than model code: a backfill wave is too wide, a poison partition retries forever, a Ray cluster keeps GPU workers warm after the useful work ends, or artifact PVCs grow without retention. OpenCost gives allocation evidence; Airflow pools, Kueue quotas, `ResourceQuota`, and `LimitRange` remain the admission controls.

The portfolio pattern links cost to partition lineage. A reviewer should be able to answer which DAG, task, Metaflow flow, partition, model family, and training wave spent the money before approving more parallelism.

## Current Research Basis

- OpenCost can run as a Prometheus metric exporter and expose allocation metrics without requiring the full UI.
- OpenCost requires Prometheus for metric scraping and storage.
- OpenCost generated metrics include CPU, RAM, GPU, node, PVC, and load balancer cost signals.
- Kubernetes `ResourceQuota` constrains namespace consumption, and `LimitRange` can supply default requests or limits that make quota enforcement practical.
