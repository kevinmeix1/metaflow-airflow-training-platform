from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.decorators import dag, task
    from airflow.sdk import Asset
except Exception:
    class Asset:  # type: ignore
        def __init__(self, uri: str):
            self.uri = uri

    def dag(*args, **kwargs):
        def wrapper(func):
            return func

        return wrapper

    def task(func):
        return func


DEFAULT_ARGS = {
    "owner": "ml-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

RAW_SALES = Asset("lakehouse://retail/raw_sales")
DEMAND_MODEL = Asset("mlflow://models/daily-demand-forecaster")


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=[RAW_SALES],
    catchup=True,
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    tags=["metaflow", "training", "backfill", "asset-aware"],
)
def demand_training_orchestration():
    @task
    def run_partition(ds=None):
        return {
            "command": f"PYTHONPATH=src python3 -m training_orchestration_platform run --date {ds}",
            "partition": ds,
            "outlet": str(DEMAND_MODEL.uri if hasattr(DEMAND_MODEL, "uri") else DEMAND_MODEL),
            "idempotency_key": f"daily-demand:{ds}",
        }

    @task
    def publish_dashboard(context):
        return {**context, "command": "make dashboard"}

    publish_dashboard(run_partition())


demand_training_orchestration()
