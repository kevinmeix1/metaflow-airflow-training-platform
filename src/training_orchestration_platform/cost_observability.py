from __future__ import annotations

from pathlib import Path

from .io import write_json


ALLOCATION_DIMENSIONS = [
    "namespace",
    "airflow_dag",
    "airflow_task",
    "metaflow_flow",
    "partition_date",
    "label_cost_center",
    "label_model_family",
    "label_training_wave",
]

OPENCOST_METRICS = [
    "container_cpu_allocation",
    "container_memory_allocation_bytes",
    "node_cpu_hourly_cost",
    "node_ram_hourly_cost",
    "node_gpu_hourly_cost",
    "node_total_hourly_cost",
    "kube_persistentvolumeclaim_resource_requests_storage_bytes",
]

TRAINING_BUDGETS = [
    {
        "workload": "daily-partition-training",
        "training_wave": "daily",
        "monthly_budget_usd": 540.0,
        "unit_metric": "cost_per_successful_partition",
        "guardrail": "keep mapped Airflow partitions behind pools and Kueue quota before widening daily fanout",
    },
    {
        "workload": "historical-backfill-wave",
        "training_wave": "backfill",
        "monthly_budget_usd": 880.0,
        "unit_metric": "backfill_cost_per_partition",
        "guardrail": "split long backfills into admitted Kueue waves and pause on SLO burn or queue pressure",
    },
    {
        "workload": "kuberay-distributed-training",
        "training_wave": "distributed",
        "monthly_budget_usd": 760.0,
        "unit_metric": "gpu_hourly_cost",
        "guardrail": "bound elastic Ray workers and prefer CPU retry when GPU diagnostics are not required",
    },
    {
        "workload": "failed-partition-recovery",
        "training_wave": "recovery",
        "monthly_budget_usd": 220.0,
        "unit_metric": "retry_cost",
        "guardrail": "cap retry storms by partition fingerprint and force manual review after repeated poison data failures",
    },
]


def build_cost_observability_report(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "opencost_exporter_scraped", "passed": "node_total_hourly_cost" in OPENCOST_METRICS},
        {"name": "partition_cost_attribution", "passed": "partition_date" in ALLOCATION_DIMENSIONS},
        {"name": "backfill_wave_budget", "passed": any(item["training_wave"] == "backfill" for item in TRAINING_BUDGETS)},
        {"name": "gpu_training_attribution", "passed": "node_gpu_hourly_cost" in OPENCOST_METRICS},
        {"name": "retry_storm_cost_guardrail", "passed": any(item["training_wave"] == "recovery" for item in TRAINING_BUDGETS)},
        {"name": "artifact_storage_cost_visible", "passed": "kube_persistentvolumeclaim_resource_requests_storage_bytes" in OPENCOST_METRICS},
    ]
    report = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_training_opencost_guardrails" if all(check["passed"] for check in checks) else "complete_training_cost_contract",
        "monthly_budget_usd": round(sum(item["monthly_budget_usd"] for item in TRAINING_BUDGETS), 2),
        "allocation_dimensions": ALLOCATION_DIMENSIONS,
        "required_metrics": OPENCOST_METRICS,
        "training_budgets": TRAINING_BUDGETS,
        "prometheus": {
            "scrape_interval": "1m",
            "scrape_timeout": "10s",
            "metrics_path": "/metrics",
            "target": "opencost.opencost-exporter:9003",
        },
        "unit_economics": {
            "primary_kpi": "cost_per_successful_partition",
            "formula": "allocated batch cost / successful partition count",
            "alert_threshold_usd": 38.0,
        },
        "guardrails": [
            "Attribute spend by DAG, task, Metaflow flow, partition date, model family, and training wave.",
            "Track backfill and failed-partition recovery budgets separately from normal daily training.",
            "Watch GPU hourly cost and PVC artifact allocation before increasing Ray worker bounds.",
            "Review cost regressions in the same admission decision as SLOs, queue pressure, governance, and provenance.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/opencost-finops.yaml"],
        "references": [
            "https://opencost.io/docs/integrations/opencost-exporter/",
            "https://opencost.io/docs/integrations/metrics/",
            "https://opencost.io/docs/installation/install/",
            "https://kubernetes.io/docs/concepts/policy/resource-quotas/",
        ],
    }
    write_json(root / "reports" / "cost_observability_report.json", report)
    return report
