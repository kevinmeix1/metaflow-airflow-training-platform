from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from .io import read_json, read_jsonl, write_json


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_optional_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    return read_json(path)


def _sha256(path: Path) -> dict:
    if not path.exists() or not path.is_file():
        return {"path": str(path), "exists": False, "sha256": None}
    return {"path": str(path), "exists": True, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _latest_successful_partition(root: Path, assets: dict) -> str | None:
    latest = assets.get("daily_demand_model", {}).get("latest_successful_partition")
    if latest:
        return latest
    successful = [
        row["ds"]
        for row in read_jsonl(root / "orchestration" / "run_history.jsonl")
        if row.get("task") == "pipeline" and row.get("status") == "success"
    ]
    return successful[-1] if successful else None


def build_governance_bundle(root: str | Path) -> dict:
    root = Path(root)
    assets = _read_optional_json(root / "orchestration" / "asset_catalog.json", {})
    lineage = _read_optional_json(root / "orchestration" / "lineage.json", {})
    backfill_summary = _read_optional_json(root / "reports" / "backfill_summary.json", {})
    partition = _latest_successful_partition(root, assets) or "unknown"
    model_version = f"demand-{partition}" if partition != "unknown" else "unknown"
    model = _read_optional_json(root / "models" / model_version / "model.json", {})
    metrics = _read_optional_json(root / "reports" / f"metrics_{partition}.json", {})
    gates = _read_optional_json(root / "reports" / f"gates_{partition}.json", {"passed": False, "checks": []})
    manifest = _read_optional_json(root / "data" / "manifests" / f"ds={partition}.json", {})

    artifact_paths = [
        root / "orchestration" / "run_history.jsonl",
        root / "orchestration" / "asset_catalog.json",
        root / "orchestration" / "lineage.json",
        root / "reports" / "backfill_summary.json",
        root / "data" / "manifests" / f"ds={partition}.json",
        root / "models" / model_version / "model.json",
        root / "reports" / f"gates_{partition}.json",
    ]
    reproducibility_manifest = {
        "generated_at": _utc_iso(),
        "platform": "metaflow-airflow-training-platform",
        "latest_successful_partition": partition,
        "model_version": model_version,
        "input_content_sha256": manifest.get("content_sha256"),
        "artifact_hashes": [_sha256(path) for path in artifact_paths],
        "environment": {
            "scheduler": "Airflow with catchup, Kueue admission, and KubernetesPodOperator",
            "trainer": "Metaflow-style step boundary with MLflow-style run metadata",
            "idempotency_key": manifest.get("idempotency_key"),
        },
    }

    model_card = {
        "name": model.get("name", "daily-demand-forecaster"),
        "version": model.get("version", model_version),
        "intended_use": "Generate daily SKU demand forecasts for operational planning.",
        "out_of_scope_use": "Do not use for financial reporting without reconciliation to source-of-record sales data.",
        "features": ["sku", "price", "promo", "inventory"],
        "metrics": metrics,
        "quality_gate": gates,
        "limitations": [
            "Synthetic retail demand data is used for deterministic review.",
            "Simple baseline model is intentionally inspectable; production should compare richer challengers.",
            "Backfills must be capacity-admitted before increasing parallelism.",
        ],
    }

    data_card = {
        "dataset": "partitioned_daily_sales",
        "owner": "data-platform",
        "source": "deterministic partition generator in src/training_orchestration_platform/data.py",
        "latest_partition": partition,
        "schema_contract": "contracts/demand_training_contract.yml",
        "input_manifest": manifest,
        "successful_partitions": assets.get("raw_sales_partition", {}).get("successful_partitions", []),
        "retention": "Keep partition manifests and content hashes with model artifacts for replay and audit.",
    }

    risk_register = [
        {
            "risk": "non-idempotent backfill overwrites trusted partitions",
            "impact": "historical model artifacts cannot be reproduced",
            "control": "partition idempotency keys and force flag for explicit recovery",
            "evidence": "data/manifests/",
            "status": "controlled",
        },
        {
            "risk": "failed training silently blocks downstream forecasts",
            "impact": "serving artifact is stale while the DAG appears complete",
            "control": "pipeline run history, failure records, and recovery run",
            "evidence": "orchestration/run_history.jsonl",
            "status": "controlled" if backfill_summary else "needs_backfill_summary",
        },
        {
            "risk": "lineage does not explain downstream impact",
            "impact": "operators cannot answer which assets use a partition",
            "control": "asset catalog and lineage graph generated after every partition run",
            "evidence": "orchestration/lineage.json",
            "status": "controlled" if lineage else "missing_lineage",
        },
        {
            "risk": "weak model artifact promoted from a backfill",
            "impact": "forecast quality degrades after bulk retraining",
            "control": "per-partition model gates for RMSE, MAPE, and SKU error",
            "evidence": f"reports/gates_{partition}.json",
            "status": "controlled" if gates.get("passed") else "blocked",
        },
    ]

    approval_record = {
        "approval_id": f"daily-demand-{partition}",
        "decision": "approved_training_artifact" if gates.get("passed") else "blocked",
        "generated_at": _utc_iso(),
        "approvers": ["training-platform-owner", "forecasting-owner"],
        "required_evidence": [
            "partition manifest has a content hash",
            "data quality gates passed",
            "model quality gates passed",
            "asset lineage generated",
            "backfill history is available",
        ],
        "gate_summary": gates,
    }

    bundle = {
        "platform": "metaflow-airflow-training-platform",
        "framework_alignment": {
            "nist_ai_rmf": ["Govern", "Map", "Measure", "Manage"],
            "mlflow_registry": "model version evidence is tied to the partition and run artifact",
            "model_transparency": "model card plus partitioned data card and reproducibility hashes",
        },
        "release": {
            "model_name": model_card["name"],
            "model_version": model_card["version"],
            "partition": partition,
            "decision": approval_record["decision"],
        },
        "evidence_files": {
            "model_card": "governance/model_card.json",
            "data_card": "governance/data_card.json",
            "risk_register": "governance/risk_register.json",
            "approval_record": "governance/approval_record.json",
            "reproducibility_manifest": "governance/reproducibility_manifest.json",
        },
    }

    write_json(root / "governance" / "model_card.json", model_card)
    write_json(root / "governance" / "data_card.json", data_card)
    write_json(root / "governance" / "risk_register.json", risk_register)
    write_json(root / "governance" / "approval_record.json", approval_record)
    write_json(root / "governance" / "reproducibility_manifest.json", reproducibility_manifest)
    write_json(root / "reports" / "governance_evidence_bundle.json", bundle)
    return bundle
