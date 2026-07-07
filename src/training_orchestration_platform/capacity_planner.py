from __future__ import annotations

from pathlib import Path

from .data import date_range
from .io import write_json
from .orchestrator import completed_dates


MODEL_FAMILIES = ["baseline", "promo_lift", "inventory_capped"]


def partition_workload(ds: str, model_family: str) -> dict:
    weights = {"baseline": (1.0, 1.5), "promo_lift": (1.5, 2.0), "inventory_capped": (2.0, 3.0)}
    cpu, memory = weights[model_family]
    return {
        "partition": ds,
        "model_family": model_family,
        "cpu": cpu,
        "memory_gib": memory,
        "priority": "backfill-critical" if model_family != "baseline" else "normal",
        "tasks": ["extract", "validate", "train", "evaluate", "register"],
    }


def pack_waves(workloads: list[dict], *, max_cpu: float = 6.0, max_memory_gib: float = 10.0, max_parallelism: int = 4) -> list[dict]:
    waves: list[dict] = []
    current: list[dict] = []
    cpu_used = 0.0
    memory_used = 0.0
    for workload in workloads:
        would_exceed = (
            len(current) >= max_parallelism
            or cpu_used + workload["cpu"] > max_cpu
            or memory_used + workload["memory_gib"] > max_memory_gib
        )
        if current and would_exceed:
            waves.append(
                {
                    "wave": len(waves) + 1,
                    "workloads": current,
                    "cpu": round(cpu_used, 2),
                    "memory_gib": round(memory_used, 2),
                }
            )
            current = []
            cpu_used = 0.0
            memory_used = 0.0
        current.append(workload)
        cpu_used += workload["cpu"]
        memory_used += workload["memory_gib"]
    if current:
        waves.append(
            {
                "wave": len(waves) + 1,
                "workloads": current,
                "cpu": round(cpu_used, 2),
                "memory_gib": round(memory_used, 2),
            }
        )
    return waves


def build_backfill_plan(root: str | Path, start: str, end: str, *, force: bool = False) -> dict:
    root = Path(root)
    completed = completed_dates(root)
    partitions = list(date_range(start, end))
    requested = [
        partition_workload(ds, family)
        for ds in partitions
        if force or ds not in completed
        for family in MODEL_FAMILIES
    ]
    requested.sort(key=lambda item: (item["priority"] != "backfill-critical", item["partition"], item["model_family"]))
    waves = pack_waves(requested)
    plan = {
        "start": start,
        "end": end,
        "force": force,
        "skipped_partitions": sorted(set(partitions) & completed) if not force else [],
        "workload_count": len(requested),
        "wave_count": len(waves),
        "waves": waves,
        "queue": "demand-training-queue",
        "airflow_pool": "metaflow_training_pool",
        "max_active_runs": 2,
    }
    write_json(root / "reports" / "backfill_capacity_plan.json", plan)
    return plan
