from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .io import append_jsonl, read_jsonl
from .model import evaluate, evaluate_gates, train_forecaster
from .orchestrator import run_log_path


RUNTIME_CONTRACT_VERSION = "1.0"
CANDIDATE_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")

DEFAULT_CANDIDATE_GRID = [
    {
        "name": "balanced",
        "price_coefficient": -1.25,
        "promo_lift_scale": 1.0,
        "inventory_cap_enabled": True,
    },
    {
        "name": "price-sensitive",
        "price_coefficient": -1.6,
        "promo_lift_scale": 1.0,
        "inventory_cap_enabled": True,
    },
    {
        "name": "promo-sensitive",
        "price_coefficient": -1.15,
        "promo_lift_scale": 1.2,
        "inventory_cap_enabled": True,
    },
    {
        "name": "uncapped-control",
        "price_coefficient": -1.25,
        "promo_lift_scale": 1.0,
        "inventory_cap_enabled": False,
    },
]


class CandidateSelectionError(RuntimeError):
    pass


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_candidate(
    item: Any,
    *,
    seen_names: set[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError("every candidate must be an object")
    name = str(item.get("name", ""))
    if not CANDIDATE_NAME.fullmatch(name):
        raise ValueError(f"invalid candidate name: {name!r}")
    if seen_names is not None:
        if name in seen_names:
            raise ValueError(f"duplicate candidate name: {name}")
        seen_names.add(name)

    price_coefficient = float(item.get("price_coefficient", -1.25))
    promo_lift_scale = float(item.get("promo_lift_scale", 1.0))
    inventory_cap_enabled = item.get("inventory_cap_enabled", True)
    if not isinstance(inventory_cap_enabled, bool):
        raise ValueError(f"candidate {name} inventory_cap_enabled must be a boolean")
    if not -4.0 <= price_coefficient <= 0.0:
        raise ValueError(
            f"candidate {name} price_coefficient must be between -4.0 and 0.0"
        )
    if not 0.0 <= promo_lift_scale <= 3.0:
        raise ValueError(
            f"candidate {name} promo_lift_scale must be between 0.0 and 3.0"
        )
    return {
        "name": name,
        "price_coefficient": price_coefficient,
        "promo_lift_scale": promo_lift_scale,
        "inventory_cap_enabled": inventory_cap_enabled,
    }


def normalize_candidate_grid(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not 2 <= len(value) <= 12:
        raise ValueError("candidate grid must contain between 2 and 12 entries")
    seen_names: set[str] = set()
    return [_normalize_candidate(item, seen_names=seen_names) for item in value]


def evaluate_candidate(
    *,
    ds: str,
    config: dict[str, Any],
    train_rows: list[dict],
    eval_rows: list[dict],
    validation: dict,
) -> dict[str, Any]:
    normalized = _normalize_candidate(config)
    config_digest = canonical_hash(normalized)
    version = f"demand-{ds}-{normalized['name']}-{config_digest[:6]}"
    model = train_forecaster(
        train_rows,
        version=version,
        price_coefficient=normalized["price_coefficient"],
        promo_lift_scale=normalized["promo_lift_scale"],
        inventory_cap_enabled=normalized["inventory_cap_enabled"],
    )
    metrics = evaluate(model, eval_rows)
    gates = evaluate_gates(metrics, validation)
    return {
        "name": normalized["name"],
        "config": normalized,
        "config_digest": config_digest,
        "model": model,
        "metrics": metrics,
        "gates": gates,
    }


def select_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    passing = [item for item in candidates if item.get("gates", {}).get("passed")]
    if not passing:
        raise CandidateSelectionError("no candidate passed the evaluation gates")
    return min(
        passing,
        key=lambda item: (
            float(item["metrics"]["rmse"]),
            float(item["metrics"]["mape"]),
            float(item["metrics"]["max_sku_mae"]),
            str(item["name"]),
        ),
    )


def candidate_comparison(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [
            {
                "name": item["name"],
                "config": item["config"],
                "config_digest": item["config_digest"],
                "model_version": item["model"]["version"],
                "metrics": item["metrics"],
                "gates": item["gates"],
            }
            for item in candidates
        ],
        key=lambda item: item["name"],
    )


def write_json_atomic(path: Path, payload: dict | list) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return path


def _append_once(path: Path, payload: dict, *, key: str, value: str) -> None:
    if any(str(item.get(key)) == value for item in read_jsonl(path)):
        return
    append_jsonl(path, payload)


def publish_runtime_artifacts(
    *,
    root: str | Path,
    ds: str,
    metaflow_run_id: str,
    metaflow_pathspec: str,
    manifest: dict,
    validation: dict,
    candidates: list[dict[str, Any]],
    selected: dict[str, Any],
    step_contract: list[str],
    card_count: int,
) -> dict[str, Any]:
    root = Path(root).expanduser().resolve()
    comparison = candidate_comparison(candidates)
    model = selected["model"]
    metrics = selected["metrics"]
    gates = selected["gates"]
    model_digest = canonical_hash(model)
    registration_key = canonical_hash(
        {
            "ds": ds,
            "input_manifest": manifest["content_sha256"],
            "candidate_config": selected["config_digest"],
            "runtime_contract": RUNTIME_CONTRACT_VERSION,
        }
    )

    model_path = root / "models" / model["version"] / "model.json"
    metrics_path = root / "reports" / f"metaflow_metrics_{ds}.json"
    gates_path = root / "reports" / f"metaflow_gates_{ds}.json"
    comparison_path = root / "reports" / f"candidate_comparison_{ds}.json"
    contract_path = (
        root
        / "metaflow"
        / "runs"
        / str(metaflow_run_id)
        / "runtime_contract.json"
    )

    write_json_atomic(model_path, model)
    write_json_atomic(metrics_path, metrics)
    write_json_atomic(gates_path, gates)
    write_json_atomic(comparison_path, comparison)

    def relative(path: Path) -> str:
        return path.relative_to(root).as_posix()

    runtime_contract = {
        "contract_version": RUNTIME_CONTRACT_VERSION,
        "engine": "metaflow",
        "metaflow_run_id": str(metaflow_run_id),
        "metaflow_pathspec": metaflow_pathspec,
        "partition": ds,
        "input_manifest": manifest,
        "validation": validation,
        "step_contract": step_contract,
        "candidate_count": len(candidates),
        "passing_candidate_count": sum(
            1 for item in candidates if item["gates"]["passed"]
        ),
        "selected_candidate": selected["name"],
        "selected_model_version": model["version"],
        "selected_config_digest": selected["config_digest"],
        "model_digest": model_digest,
        "registration_idempotency_key": registration_key,
        "card_count": card_count,
        "artifacts": {
            "model": relative(model_path),
            "metrics": relative(metrics_path),
            "gates": relative(gates_path),
            "candidate_comparison": relative(comparison_path),
        },
        "published_at": utc_iso(),
    }
    write_json_atomic(contract_path, runtime_contract)
    write_json_atomic(root / "metaflow" / "latest.json", runtime_contract)

    mlflow_style_run = {
        "run_id": f"metaflow-{metaflow_run_id}",
        "experiment_name": "daily-demand-forecasting",
        "source": "executable-metaflow-runtime",
        "metaflow_pathspec": metaflow_pathspec,
        "model_version": model["version"],
        "params": {"ds": ds, **selected["config"]},
        "metrics": metrics,
        "artifacts": runtime_contract["artifacts"],
        "registration_idempotency_key": registration_key,
        "created_at": runtime_contract["published_at"],
    }
    write_json_atomic(
        root
        / "mlruns"
        / "daily-demand-forecasting"
        / f"metaflow-{metaflow_run_id}"
        / "run.json",
        mlflow_style_run,
    )

    pipeline_event = {
        "run_id": f"metaflow-{metaflow_run_id}",
        "ds": ds,
        "task": "pipeline",
        "status": "success",
        "timestamp": runtime_contract["published_at"],
        "details": {
            "engine": "metaflow",
            "model_version": model["version"],
            "metrics": metrics,
            "gates": gates,
            "selected_candidate": selected["name"],
            "candidate_count": len(candidates),
            "registration_idempotency_key": registration_key,
        },
    }
    _append_once(
        run_log_path(root),
        pipeline_event,
        key="run_id",
        value=pipeline_event["run_id"],
    )
    return runtime_contract
