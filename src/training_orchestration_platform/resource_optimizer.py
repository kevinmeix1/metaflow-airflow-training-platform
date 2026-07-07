from __future__ import annotations

from math import ceil
from pathlib import Path

from .io import write_json


CPU_MONTHLY_USD = 28.0
MEMORY_GIB_MONTHLY_USD = 3.5


WORKLOAD_PROFILES = [
    {
        "name": "metaflow-training-worker",
        "class": "indexed_batch_job",
        "current_cpu_m": 3000,
        "current_memory_mib": 8192,
        "current_replicas": 4,
        "cpu_p95_m": 2100,
        "memory_p99_mib": 6200,
        "forecast_units": 9,
        "target_units_per_replica": 3,
        "min_replicas": 1,
        "max_replicas": 8,
        "min_cpu_m": 1500,
        "min_memory_mib": 4096,
    },
    {
        "name": "airflow-scheduler-triggerer",
        "class": "orchestration_control_plane",
        "current_cpu_m": 1000,
        "current_memory_mib": 2048,
        "current_replicas": 2,
        "cpu_p95_m": 480,
        "memory_p99_mib": 900,
        "forecast_units": 2,
        "target_units_per_replica": 1,
        "min_replicas": 2,
        "max_replicas": 4,
        "min_cpu_m": 500,
        "min_memory_mib": 768,
    },
    {
        "name": "backfill-validator",
        "class": "data_quality_job",
        "current_cpu_m": 1500,
        "current_memory_mib": 3072,
        "current_replicas": 2,
        "cpu_p95_m": 800,
        "memory_p99_mib": 1600,
        "forecast_units": 4,
        "target_units_per_replica": 2,
        "min_replicas": 1,
        "max_replicas": 4,
        "min_cpu_m": 750,
        "min_memory_mib": 1024,
    },
]


def _round_up(value: float, step: int) -> int:
    return int(ceil(value / step) * step)


def _resource_cost(cpu_m: int, memory_mib: int, replicas: int) -> float:
    cpu_cost = (cpu_m / 1000) * replicas * CPU_MONTHLY_USD
    memory_cost = (memory_mib / 1024) * replicas * MEMORY_GIB_MONTHLY_USD
    return round(cpu_cost + memory_cost, 2)


def _action(current: int, recommended: int, resource: str) -> str:
    if recommended < current * 0.85:
        return f"lower_{resource}_request"
    if recommended > current * 1.15:
        return f"raise_{resource}_request"
    return f"keep_{resource}_request"


def _recommend(profile: dict) -> dict:
    cpu_request_m = _round_up(max(profile["min_cpu_m"], profile["cpu_p95_m"] * 1.35), 25)
    memory_request_mib = _round_up(max(profile["min_memory_mib"], profile["memory_p99_mib"] * 1.20), 32)
    memory_limit_mib = _round_up(memory_request_mib * 1.25, 32)
    replicas = max(profile["min_replicas"], ceil(profile["forecast_units"] / profile["target_units_per_replica"]))
    replicas = min(profile["max_replicas"], replicas)
    current_cost = _resource_cost(profile["current_cpu_m"], profile["current_memory_mib"], profile["current_replicas"])
    recommended_cost = _resource_cost(cpu_request_m, memory_request_mib, replicas)
    return {
        "workload": profile["name"],
        "class": profile["class"],
        "current": {
            "cpu_m": profile["current_cpu_m"],
            "memory_mib": profile["current_memory_mib"],
            "replicas": profile["current_replicas"],
            "monthly_cost_usd": current_cost,
        },
        "observed": {
            "cpu_p95_m": profile["cpu_p95_m"],
            "memory_p99_mib": profile["memory_p99_mib"],
            "forecast_units": profile["forecast_units"],
        },
        "recommended": {
            "cpu_request_m": cpu_request_m,
            "memory_request_mib": memory_request_mib,
            "memory_limit_mib": memory_limit_mib,
            "replicas": replicas,
            "monthly_cost_usd": recommended_cost,
        },
        "monthly_cost_delta_usd": round(recommended_cost - current_cost, 2),
        "actions": [
            _action(profile["current_cpu_m"], cpu_request_m, "cpu"),
            _action(profile["current_memory_mib"], memory_request_mib, "memory"),
            "reduce_wave_width" if replicas < profile["current_replicas"] else "keep_backfill_wave_width",
        ],
    }


def build_resource_optimization_report(root: str | Path) -> dict:
    root = Path(root)
    recommendations = [_recommend(profile) for profile in WORKLOAD_PROFILES]
    report = {
        "platform": "metaflow-airflow-training-platform",
        "method": {
            "cpu_request": "ceil(max(min_cpu, cpu_p95 * 1.35), 25m)",
            "memory_request": "ceil(max(min_memory, memory_p99 * 1.20), 32Mi)",
            "memory_limit": "ceil(memory_request * 1.25, 32Mi)",
            "parallelism": "ceil(forecast_partitions / target_partitions_per_worker) bounded by min/max replicas",
        },
        "guardrails": [
            "run VPA in Off mode before changing indexed job templates",
            "prefer reducing backfill wave width before lowering model quality gates",
            "protect Airflow triggerer capacity with pools and deferrable sensors",
            "align Kueue nominal quota with the capacity planner wave size",
        ],
        "recommendations": recommendations,
        "summary": {
            "workload_count": len(recommendations),
            "estimated_monthly_delta_usd": round(sum(item["monthly_cost_delta_usd"] for item in recommendations), 2),
            "needs_human_review": any("raise_memory_request" in item["actions"] for item in recommendations),
        },
    }
    write_json(root / "reports" / "resource_optimization.json", report)
    return report
