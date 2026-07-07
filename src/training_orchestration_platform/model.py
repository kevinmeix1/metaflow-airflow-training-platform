from __future__ import annotations

import math
import statistics

from .data import SKUS


def train_forecaster(rows: list[dict], *, version: str) -> dict:
    sku_means = {}
    promo_lifts = {}
    global_mean = statistics.mean(float(row["units_sold"]) for row in rows)
    for sku in SKUS:
        sku_rows = [row for row in rows if row["sku"] == sku]
        sku_means[sku] = round(statistics.mean(float(row["units_sold"]) for row in sku_rows), 4)
        promo_rows = [float(row["units_sold"]) for row in sku_rows if str(row["promo"]) == "1"]
        non_promo_rows = [float(row["units_sold"]) for row in sku_rows if str(row["promo"]) == "0"]
        promo_lifts[sku] = round((statistics.mean(promo_rows) if promo_rows else sku_means[sku]) - (statistics.mean(non_promo_rows) if non_promo_rows else sku_means[sku]), 4)
    return {
        "name": "daily-demand-forecaster",
        "version": version,
        "global_mean": round(global_mean, 4),
        "sku_means": sku_means,
        "promo_lifts": promo_lifts,
        "price_coefficient": -1.25,
        "inventory_cap_enabled": True,
    }


def predict(model: dict, row: dict) -> float:
    sku = row["sku"]
    baseline = float(model["sku_means"].get(sku, model["global_mean"]))
    promo_lift = float(model["promo_lifts"].get(sku, 0.0)) if str(row["promo"]) == "1" else 0.0
    centered_price = float(row["price"]) - {"coffee": 6.5, "tea": 5.2, "juice": 4.6, "water": 2.4, "snack": 3.8}.get(sku, 4.5)
    estimate = baseline + promo_lift + centered_price * float(model["price_coefficient"])
    if model.get("inventory_cap_enabled"):
        estimate = min(estimate, float(row["inventory"]))
    return round(max(0.0, estimate), 4)


def evaluate(model: dict, rows: list[dict]) -> dict:
    errors = []
    absolute_pct_errors = []
    by_sku: dict[str, list[float]] = {}
    for row in rows:
        actual = float(row["units_sold"])
        prediction = predict(model, row)
        error = prediction - actual
        errors.append(error)
        absolute_pct_errors.append(abs(error) / max(actual, 1.0))
        by_sku.setdefault(row["sku"], []).append(abs(error))
    rmse = math.sqrt(sum(error * error for error in errors) / max(len(errors), 1))
    mae_by_sku = {sku: round(sum(values) / max(len(values), 1), 4) for sku, values in sorted(by_sku.items())}
    return {
        "rmse": round(rmse, 4),
        "mape": round(sum(absolute_pct_errors) / max(len(absolute_pct_errors), 1), 4),
        "mae_by_sku": mae_by_sku,
        "max_sku_mae": round(max(mae_by_sku.values(), default=0.0), 4),
        "row_count": len(rows),
    }


def evaluate_gates(metrics: dict, validation: dict) -> dict:
    checks = [
        {"name": "data_validation", "passed": validation.get("passed", False), "observed": validation.get("row_count")},
        {"name": "rmse", "passed": metrics.get("rmse", 999) <= 7.5, "observed": metrics.get("rmse"), "threshold": 7.5},
        {"name": "mape", "passed": metrics.get("mape", 999) <= 0.26, "observed": metrics.get("mape"), "threshold": 0.26},
        {"name": "max_sku_mae", "passed": metrics.get("max_sku_mae", 999) <= 6.8, "observed": metrics.get("max_sku_mae"), "threshold": 6.8},
    ]
    return {"passed": all(check["passed"] for check in checks), "checks": checks}
