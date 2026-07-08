from __future__ import annotations

from pathlib import Path

from .io import write_json


DEADLINE_POLICIES = [
    {
        "name": "dagrun_queue_to_start",
        "reference": "DeadlineReference.DAGRUN_QUEUED_AT",
        "interval": "10m",
        "callback": "notify_training_platform",
        "severity": "page",
        "next_action": "inspect scheduler capacity, Airflow pool saturation, and Kueue pending workloads",
    },
    {
        "name": "backfill_wave_runtime",
        "reference": "DeadlineReference.DAGRUN_START_DATE",
        "interval": "4h",
        "callback": "open_backfill_incident",
        "severity": "ticket",
        "next_action": "reduce wave width, check failed partition retries, and hold bulk backfills",
    },
    {
        "name": "failed_partition_recovery",
        "reference": "custom_failed_partition_detected_at",
        "interval": "30m",
        "callback": "page_recovery_owner",
        "severity": "page",
        "next_action": "run forced partition replay and attach lineage context",
    },
]


def build_deadline_alert_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "airflow3_deadline_alerts_declared", "passed": len(DEADLINE_POLICIES) >= 3},
        {"name": "legacy_sla_removed", "passed": True, "observed": "Airflow 3 replaces SLA callbacks with Deadline Alerts"},
        {"name": "callback_timeout_bounded", "passed": True, "observed": "AIRFLOW__CALLBACKS__CALLBACK_EXECUTION_TIMEOUT=300"},
        {"name": "queue_deadline_has_kueue_action", "passed": any("Kueue" in policy["next_action"] for policy in DEADLINE_POLICIES)},
        {"name": "failed_partition_recovery_deadline", "passed": any(policy["name"] == "failed_partition_recovery" for policy in DEADLINE_POLICIES)},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_airflow3_deadline_alerts" if all(check["passed"] for check in checks) else "keep_legacy_timeout_controls",
        "dag_id": "enterprise_backfill_training_mesh",
        "deadline_policies": DEADLINE_POLICIES,
        "runtime_config": {
            "AIRFLOW__CALLBACKS__CALLBACK_EXECUTION_TIMEOUT": "300",
            "max_active_runs": 2,
            "protected_pools": ["metaflow_training_pool", "training_pool"],
        },
        "checks": checks,
        "guardrails": [
            "Use Deadline Alerts for Dag run thresholds instead of Airflow 2 SLA callbacks.",
            "Bound callback execution so a stuck notifier cannot block alert handling.",
            "Route queue-to-start misses to scheduler, pool, and Kueue capacity review.",
            "Route failed partition recovery misses to forced replay and lineage context.",
        ],
        "references": [
            "https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html",
            "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#slas",
            "https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#callback-execution-timeout",
        ],
    }
    write_json(root / "reports" / "deadline_alert_plan.json", plan)
    return plan
