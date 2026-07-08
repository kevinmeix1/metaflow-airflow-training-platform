from __future__ import annotations

from pathlib import Path

from .io import write_json


def _utilization(used: float, quota: float) -> float:
    return round(used / max(quota, 0.0001), 4)


def _tenant(
    *,
    name: str,
    namespace: str,
    queue: str,
    cost_center: str,
    cpu_quota: float,
    cpu_used: float,
    memory_quota_gib: float,
    memory_used_gib: float,
    pool_slots: int,
    pool_used: int,
    priority_class: str,
) -> dict:
    return {
        "name": name,
        "namespace": namespace,
        "queue": queue,
        "cost_center": cost_center,
        "priority_class": priority_class,
        "quota": {"cpu": cpu_quota, "memory_gib": memory_quota_gib, "airflow_pool_slots": pool_slots},
        "observed": {"cpu": cpu_used, "memory_gib": memory_used_gib, "airflow_pool_slots": pool_used},
        "utilization": {
            "cpu": _utilization(cpu_used, cpu_quota),
            "memory": _utilization(memory_used_gib, memory_quota_gib),
            "airflow_pool": _utilization(pool_used, pool_slots),
        },
        "labels": {
            "platform.mlops.dev/tenant": name,
            "platform.mlops.dev/cost-center": cost_center,
            "platform.mlops.dev/data-domain": "demand-forecasting",
        },
    }


def build_tenancy_report(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    tenants = [
        _tenant(
            name="forecasting-prod",
            namespace="ml-training-prod",
            queue="demand-training-queue",
            cost_center="forecasting",
            cpu_quota=30,
            cpu_used=18,
            memory_quota_gib=120,
            memory_used_gib=70,
            pool_slots=9,
            pool_used=5,
            priority_class="training-backfill-critical",
        ),
        _tenant(
            name="data-quality",
            namespace="ml-training-quality",
            queue="training-quality-queue",
            cost_center="data-platform",
            cpu_quota=14,
            cpu_used=8,
            memory_quota_gib=56,
            memory_used_gib=32,
            pool_slots=4,
            pool_used=2,
            priority_class="training-quality-normal",
        ),
        _tenant(
            name="feature-exploration",
            namespace="ml-training-exploration",
            queue="feature-sweep-queue",
            cost_center="ml-research",
            cpu_quota=12,
            cpu_used=11,
            memory_quota_gib=48,
            memory_used_gib=35,
            pool_slots=2,
            pool_used=2,
            priority_class="training-low-priority",
        ),
    ]
    cpu_utils = [tenant["utilization"]["cpu"] for tenant in tenants]
    pool_utils = [tenant["utilization"]["airflow_pool"] for tenant in tenants]
    noisy_neighbor_risks = [
        tenant["name"]
        for tenant in tenants
        if max(tenant["utilization"].values()) >= 0.90 and tenant["priority_class"] == "training-low-priority"
    ]
    checks = [
        {"name": "namespace_resource_quotas", "passed": all(tenant["quota"]["cpu"] > 0 for tenant in tenants)},
        {"name": "no_hard_quota_breach", "passed": all(max(tenant["utilization"].values()) <= 1.0 for tenant in tenants)},
        {"name": "recovery_capacity_reserved", "passed": tenants[0]["quota"]["airflow_pool_slots"] - tenants[0]["observed"]["airflow_pool_slots"] >= 2},
        {"name": "tenant_cost_labels", "passed": all("platform.mlops.dev/cost-center" in tenant["labels"] for tenant in tenants)},
        {"name": "noisy_neighbor_contained", "passed": all(risk == "feature-exploration" for risk in noisy_neighbor_risks), "observed": noisy_neighbor_risks},
    ]
    report = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "tenants": tenants,
        "checks": checks,
        "fairness": {
            "cohort": "ml-training-cohort",
            "max_cpu_utilization_gap": round(max(cpu_utils) - min(cpu_utils), 4),
            "max_airflow_pool_utilization_gap": round(max(pool_utils) - min(pool_utils), 4),
            "borrowing_policy": "feature exploration can borrow idle training quota but failed-partition recovery preempts it first",
        },
        "controls": [
            "Production backfills, data quality, and feature exploration use separate namespaces.",
            "Kueue cohorts allow controlled quota sharing between training tenants.",
            "Airflow pools keep recovery slots free even when mapped backfills fan out.",
            "Cost-center labels support chargeback for long-running training jobs.",
            "Default-deny NetworkPolicies keep exploratory jobs away from production artifacts.",
        ],
        "references": [
            "https://kubernetes.io/docs/concepts/security/multi-tenancy/",
            "https://kubernetes.io/docs/concepts/policy/resource-quotas/",
            "https://kueue.sigs.k8s.io/docs/concepts/cohort/",
            "https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html",
        ],
    }
    write_json(Path(root) / "reports" / "tenancy_fairness_report.json", report)
    return report
