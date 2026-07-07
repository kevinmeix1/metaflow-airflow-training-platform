from __future__ import annotations

import uuid
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from .data import date_range, generate_partition, validate_rows
from .io import append_jsonl, read_csv, read_jsonl, write_json
from .model import evaluate, evaluate_gates, train_forecaster


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_log_path(root: str | Path) -> Path:
    return Path(root) / "orchestration" / "run_history.jsonl"


def completed_dates(root: str | Path) -> set[str]:
    return {
        row["ds"]
        for row in read_jsonl(run_log_path(root))
        if row.get("status") == "success" and row.get("task") == "pipeline"
    }


def log_task(root: str | Path, *, run_id: str, ds: str, task: str, status: str, details: dict | None = None) -> dict:
    payload = {
        "run_id": run_id,
        "ds": ds,
        "task": task,
        "status": status,
        "timestamp": utc_iso(),
        "details": details or {},
    }
    append_jsonl(run_log_path(root), payload)
    return payload


def split_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    split_at = int(len(rows) * 0.72)
    return rows[:split_at], rows[split_at:]


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_partition(root: str | Path, ds: str, *, force: bool = False, fail_task: str | None = None) -> dict:
    root = Path(root)
    if ds in completed_dates(root) and not force:
        return {"ds": ds, "status": "skipped", "reason": "already_successful"}
    run_id = str(uuid.uuid4())
    try:
        raw_path = root / "data" / "raw" / f"ds={ds}" / "sales.csv"
        log_task(root, run_id=run_id, ds=ds, task="extract_partition", status="started")
        if fail_task == "extract_partition":
            raise RuntimeError("simulated extract failure")
        generate_partition(raw_path, ds)
        manifest = {
            "partition": ds,
            "path": str(raw_path),
            "content_sha256": file_sha256(raw_path),
            "created_at": utc_iso(),
            "idempotency_key": f"daily-demand:{ds}",
        }
        write_json(root / "data" / "manifests" / f"ds={ds}.json", manifest)
        log_task(root, run_id=run_id, ds=ds, task="extract_partition", status="success", details=manifest)

        rows = read_csv(raw_path)
        log_task(root, run_id=run_id, ds=ds, task="validate_partition", status="started")
        validation = validate_rows(rows)
        write_json(root / "reports" / f"validation_{ds}.json", validation)
        if fail_task == "validate_partition":
            raise RuntimeError("simulated validation failure")
        if not validation["passed"]:
            raise RuntimeError("validation failed")
        log_task(root, run_id=run_id, ds=ds, task="validate_partition", status="success", details=validation)

        train_rows, eval_rows = split_rows(rows)
        version = f"demand-{ds}"
        log_task(root, run_id=run_id, ds=ds, task="metaflow_train", status="started")
        if fail_task == "metaflow_train":
            raise RuntimeError("simulated training failure")
        model = train_forecaster(train_rows, version=version)
        metrics = evaluate(model, eval_rows)
        gate_report = evaluate_gates(metrics, validation)
        write_json(root / "models" / version / "model.json", model)
        write_json(root / "reports" / f"metrics_{ds}.json", metrics)
        write_json(root / "reports" / f"gates_{ds}.json", gate_report)
        log_task(root, run_id=run_id, ds=ds, task="metaflow_train", status="success", details={"version": version, "metrics": metrics})

        mlflow_run = {
            "run_id": run_id,
            "experiment_name": "daily-demand-forecasting",
            "model_version": version,
            "params": {"ds": ds, "algorithm": "sku_mean_with_promo_lift"},
            "metrics": metrics,
            "artifacts": {
                "model": str(root / "models" / version / "model.json"),
                "gate_report": str(root / "reports" / f"gates_{ds}.json"),
                "input_manifest": str(root / "data" / "manifests" / f"ds={ds}.json"),
            },
            "created_at": utc_iso(),
        }
        write_json(root / "mlruns" / "daily-demand-forecasting" / run_id / "run.json", mlflow_run)

        status = "success" if gate_report["passed"] else "failed"
        result = {"ds": ds, "run_id": run_id, "status": status, "model_version": version, "metrics": metrics, "gates": gate_report}
        log_task(root, run_id=run_id, ds=ds, task="pipeline", status=status, details=result)
        update_assets(root)
        return result
    except Exception as exc:
        log_task(root, run_id=run_id, ds=ds, task="pipeline", status="failed", details={"error": str(exc)})
        update_assets(root)
        return {"ds": ds, "run_id": run_id, "status": "failed", "error": str(exc)}


def backfill(root: str | Path, start: str, end: str, *, force: bool = False, fail_date: str | None = None) -> dict:
    results = []
    for ds in date_range(start, end):
        fail_task = "metaflow_train" if ds == fail_date else None
        results.append(run_partition(root, ds, force=force, fail_task=fail_task))
    summary = {
        "start": start,
        "end": end,
        "requested_dates": len(results),
        "success_count": sum(1 for row in results if row["status"] == "success"),
        "failed_count": sum(1 for row in results if row["status"] == "failed"),
        "skipped_count": sum(1 for row in results if row["status"] == "skipped"),
        "results": results,
    }
    write_json(Path(root) / "reports" / "backfill_summary.json", summary)
    return summary


def update_assets(root: str | Path) -> dict:
    root = Path(root)
    runs = read_jsonl(run_log_path(root))
    successful = [row for row in runs if row.get("task") == "pipeline" and row.get("status") == "success"]
    failed = [row for row in runs if row.get("task") == "pipeline" and row.get("status") == "failed"]
    assets = {
        "raw_sales_partition": {
            "type": "dataset",
            "owner": "data-platform",
            "successful_partitions": sorted({row["ds"] for row in successful}),
        },
        "daily_demand_model": {
            "type": "model",
            "owner": "ml-platform",
            "latest_successful_partition": successful[-1]["ds"] if successful else None,
        },
        "airflow_training_dag": {"type": "orchestrator", "owner": "ml-platform", "failed_runs": len(failed)},
    }
    lineage = {
        "raw_sales_partition": ["validated_sales_partition"],
        "validated_sales_partition": ["metaflow_training_flow"],
        "metaflow_training_flow": ["mlflow_run", "daily_demand_model"],
        "daily_demand_model": ["forecast_serving_artifact"],
    }
    write_json(root / "orchestration" / "asset_catalog.json", assets)
    write_json(root / "orchestration" / "lineage.json", lineage)
    return {"assets": assets, "lineage": lineage}
