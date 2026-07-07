from __future__ import annotations

from pathlib import Path

from .io import read_jsonl, write_json


def burn_rate(error_ratio: float, target: float) -> float:
    return round(error_ratio / max(1.0 - target, 0.0001), 4)


def remaining_budget_pct(error_ratio: float, target: float) -> float:
    budget = max(1.0 - target, 0.0001)
    return round(max(0.0, 100.0 * (1.0 - error_ratio / budget)), 2)


def _slo(name: str, *, target: float, error_ratio: float, owner: str) -> dict:
    burn = burn_rate(error_ratio, target)
    if burn >= 14.4:
        status = "page"
    elif burn >= 6.0:
        status = "hold_backfill"
    elif burn >= 1.0:
        status = "ticket"
    else:
        status = "healthy"
    return {
        "name": name,
        "target": target,
        "error_ratio": round(error_ratio, 6),
        "burn_rate": burn,
        "remaining_error_budget_pct": remaining_budget_pct(error_ratio, target),
        "status": status,
        "owner": owner,
    }


def build_slo_report(root: str | Path) -> dict:
    root = Path(root)
    runs = read_jsonl(root / "orchestration" / "run_history.jsonl")
    pipeline_runs = [row for row in runs if row.get("task") == "pipeline" and row.get("status") in {"success", "failed"}]
    failed = [row for row in pipeline_runs if row.get("status") == "failed"]
    successful = [row for row in pipeline_runs if row.get("status") == "success"]
    failed_dates = {row["ds"] for row in failed}
    recovered_dates = {row["ds"] for row in successful} & failed_dates
    total = max(len(pipeline_runs), 1)
    lineage_exists = (root / "orchestration" / "lineage.json").exists()
    slos = [
        _slo("partition_training_success", target=0.98, error_ratio=len(failed) / total, owner="training-platform"),
        _slo("failed_partition_recovery", target=0.99, error_ratio=0.0 if failed_dates <= recovered_dates else 1.0, owner="training-platform"),
        _slo("lineage_catalog_freshness", target=0.99, error_ratio=0.0 if lineage_exists else 1.0, owner="data-platform"),
        _slo("backfill_capacity_admission", target=0.995, error_ratio=0.0, owner="orchestration"),
    ]
    max_burn = max(item["burn_rate"] for item in slos)
    if max_burn >= 14.4:
        action = "freeze_backfills_and_page"
    elif max_burn >= 6.0:
        action = "hold_bulk_backfills"
    elif max_burn >= 1.0:
        action = "open_recovery_ticket"
    else:
        action = "allow_backfill"
    report = {
        "platform": "metaflow-airflow-training-platform",
        "policy": {
            "window": "30d",
            "multiwindow_burn_rates": [
                {"name": "fast_page", "short_window": "5m", "long_window": "1h", "burn_rate": 14.4, "budget_consumed": "2%"},
                {"name": "slow_page", "short_window": "30m", "long_window": "6h", "burn_rate": 6.0, "budget_consumed": "5%"},
                {"name": "ticket", "short_window": "6h", "long_window": "3d", "burn_rate": 1.0, "budget_consumed": "10%"},
            ],
        },
        "run_counts": {"success": len(successful), "failed": len(failed), "recovered_failed_dates": len(recovered_dates)},
        "slos": slos,
        "max_burn_rate": max_burn,
        "recommended_action": action,
        "release_freeze": action in {"freeze_backfills_and_page", "hold_bulk_backfills"},
    }
    write_json(root / "reports" / "slo_error_budget.json", report)
    return report
