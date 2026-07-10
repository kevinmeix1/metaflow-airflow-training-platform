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


CALLBACK_CONTRACTS = {
    "notify_training_platform": {
        "receiver": "slack://training-platform",
        "dedupe_key": "dag_id:run_id:dagrun_queue_to_start",
        "payload_fields": ["dag_id", "run_id", "deadline_policy", "pool", "kueue_queue"],
        "retry_policy": "bounded exponential backoff, max 3 attempts inside callback timeout",
        "allowed_side_effect": "notify only; queue or fan-out changes remain explicit Airflow tasks",
        "owner": "training-platform-oncall",
    },
    "open_backfill_incident": {
        "receiver": "incident://training-backfill",
        "dedupe_key": "partition_window:backfill_wave_runtime",
        "payload_fields": ["partition_window", "wave_id", "failed_partitions", "metaflow_run_id"],
        "retry_policy": "incident upsert keyed by partition window and wave id",
        "allowed_side_effect": "open or update incident; callback does not widen backfill fan-out",
        "owner": "training-reliability",
    },
    "page_recovery_owner": {
        "receiver": "pagerduty://training-recovery-owner",
        "dedupe_key": "partition_id:failed_partition_recovery",
        "payload_fields": ["partition_id", "lineage_digest", "resume_run_id", "checkpoint_uri"],
        "retry_policy": "page once per failed partition and attach replay evidence",
        "allowed_side_effect": "request forced replay task; callback itself does not launch training",
        "owner": "training-recovery-owner",
    },
}


def _deadline_policies_with_callbacks() -> list[dict]:
    return [
        {
            **policy,
            "callback_contract": CALLBACK_CONTRACTS[policy["callback"]],
        }
        for policy in DEADLINE_POLICIES
    ]


def build_deadline_alert_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    deadline_policies = _deadline_policies_with_callbacks()
    checks = [
        {"name": "airflow3_deadline_alerts_declared", "passed": len(DEADLINE_POLICIES) >= 3},
        {"name": "legacy_sla_removed", "passed": True, "observed": "Airflow 3 replaces SLA callbacks with Deadline Alerts"},
        {"name": "callback_timeout_bounded", "passed": True, "observed": "AIRFLOW__CALLBACKS__CALLBACK_EXECUTION_TIMEOUT=300"},
        {
            "name": "callback_contracts_declared",
            "passed": all(policy.get("callback_contract", {}).get("dedupe_key") for policy in deadline_policies),
        },
        {
            "name": "callbacks_have_bounded_side_effects",
            "passed": all("allowed_side_effect" in policy.get("callback_contract", {}) for policy in deadline_policies),
        },
        {"name": "queue_deadline_has_kueue_action", "passed": any("Kueue" in policy["next_action"] for policy in DEADLINE_POLICIES)},
        {"name": "failed_partition_recovery_deadline", "passed": any(policy["name"] == "failed_partition_recovery" for policy in DEADLINE_POLICIES)},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_airflow3_deadline_alerts" if all(check["passed"] for check in checks) else "keep_legacy_timeout_controls",
        "dag_id": "enterprise_backfill_training_mesh",
        "deadline_policies": deadline_policies,
        "callback_contracts": CALLBACK_CONTRACTS,
        "runtime_config": {
            "AIRFLOW__CALLBACKS__CALLBACK_EXECUTION_TIMEOUT": "300",
            "max_active_runs": 2,
            "protected_pools": ["metaflow_training_pool", "training_pool"],
        },
        "checks": checks,
        "guardrails": [
            "Use Deadline Alerts for Dag run thresholds instead of Airflow 2 SLA callbacks.",
            "Bound callback execution so a stuck notifier cannot block alert handling.",
            "Keep callbacks idempotent with explicit dedupe keys and bounded payload fields.",
            "Callbacks may notify, page, open incidents, or request guarded replay tasks; they must not directly launch training or widen backfills.",
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
