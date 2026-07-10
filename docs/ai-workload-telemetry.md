# AI Workload Telemetry Readiness

`make demo` emits `reports/ai_workload_telemetry_plan.json`, which maps the
training platform's Airflow assets, Metaflow runs, checkpointed trainers, and
Indexed Jobs to resource signals, lineage fields, SLOs, and recovery actions.

The report is designed for a senior-level review: it explains exactly how a
failed partition, a costly candidate grid, or a stale training asset would be
diagnosed without guessing from logs.

Current practice reflected here:
- Airflow assets make freshness and downstream model dependencies explicit.
- Metaflow run ids and cloned resume lineage are promoted to audit evidence.
- Kubernetes Indexed Jobs, pod-level resources, and DRA claim status explain training retries and cost.
- Checkpoints are treated as content-addressed release artifacts, not incidental files.
