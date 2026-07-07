from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

from .io import write_csv


REQUIRED_COLUMNS = ["ds", "store_id", "sku", "price", "promo", "inventory", "units_sold"]
SKUS = ["coffee", "tea", "juice", "water", "snack"]


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def date_range(start: str, end: str) -> list[str]:
    current = parse_date(start)
    stop = parse_date(end)
    output = []
    while current <= stop:
        output.append(current.isoformat())
        current += timedelta(days=1)
    return output


def generate_partition(path: str | Path, ds: str, *, rows: int = 180, seed: int = 17) -> Path:
    rng = random.Random(seed + int(ds.replace("-", "")))
    weekday = parse_date(ds).weekday()
    records = []
    for idx in range(rows):
        sku = SKUS[idx % len(SKUS)]
        store_id = f"store_{idx % 12:02d}"
        base = {"coffee": 42, "tea": 30, "juice": 26, "water": 55, "snack": 36}[sku]
        weekend_lift = 1.16 if weekday >= 5 else 1.0
        promo = 1 if rng.random() < (0.22 if sku in {"coffee", "snack"} else 0.14) else 0
        price = {"coffee": 6.5, "tea": 5.2, "juice": 4.6, "water": 2.4, "snack": 3.8}[sku] * rng.uniform(0.92, 1.08)
        inventory = max(5, int(rng.gauss(base * 2.2, 14)))
        expected = base * weekend_lift + promo * base * 0.23 - price * 1.6 + rng.gauss(0, 4.0)
        units = max(0, min(inventory, int(expected)))
        records.append(
            {
                "ds": ds,
                "store_id": store_id,
                "sku": sku,
                "price": round(price, 2),
                "promo": promo,
                "inventory": inventory,
                "units_sold": units,
            }
        )
    return write_csv(path, records)


def validate_rows(rows: list[dict]) -> dict:
    checks = []
    checks.append({"name": "row_count", "passed": len(rows) >= 100, "observed": len(rows), "threshold": 100})
    observed = set(rows[0]) if rows else set()
    missing = sorted(set(REQUIRED_COLUMNS) - observed)
    checks.append({"name": "required_columns", "passed": not missing, "observed": missing})
    numeric_errors = 0
    for row in rows:
        for column in ["price", "promo", "inventory", "units_sold"]:
            try:
                float(row[column])
            except Exception:
                numeric_errors += 1
    checks.append({"name": "numeric_columns", "passed": numeric_errors == 0, "observed": numeric_errors})
    sku_values = {row.get("sku") for row in rows}
    checks.append({"name": "known_skus", "passed": sku_values <= set(SKUS), "observed": sorted(sku_values)})
    return {"passed": all(check["passed"] for check in checks), "checks": checks, "row_count": len(rows)}
