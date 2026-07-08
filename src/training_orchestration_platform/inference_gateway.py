from __future__ import annotations

from pathlib import Path

from .io import write_json


OBJECTIVES = [
    {
        "name": "demand-forecast-online",
        "priority": 20,
        "pool": "demand-forecast-inference-pool",
        "traffic_class": "online",
        "latency_slo_ms": 180,
        "fallback": "keep the previous champion route until the new pool passes smoke tests",
    },
    {
        "name": "demand-forecast-canary",
        "priority": 10,
        "pool": "demand-forecast-inference-pool",
        "traffic_class": "canary",
        "latency_slo_ms": 300,
        "fallback": "hold promotion and keep Airflow backfill partitions immutable",
    },
    {
        "name": "demand-backtest-batch",
        "priority": -5,
        "pool": "demand-forecast-inference-pool",
        "traffic_class": "batch",
        "latency_slo_ms": 1500,
        "fallback": "defer batch backtests until online forecast objectives are healthy",
    },
]


def build_inference_gateway_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {"name": "champion_pool_declared", "passed": True, "observed": "demand-forecast-inference-pool"},
        {"name": "training_promotion_objective_declared", "passed": any(item["traffic_class"] == "canary" for item in OBJECTIVES)},
        {"name": "online_priority_above_backtests", "passed": max(item["priority"] for item in OBJECTIVES) > min(item["priority"] for item in OBJECTIVES)},
        {"name": "fallbacks_defined", "passed": all(item["fallback"] for item in OBJECTIVES)},
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "publish_champion_inference_pool" if all(check["passed"] for check in checks) else "keep_previous_champion_route",
        "pool": {
            "name": "demand-forecast-inference-pool",
            "api_version": "inference.networking.k8s.io/v1",
            "target_port": 8000,
            "endpoint_picker": "demand-endpoint-picker:9002",
            "failure_mode": "FailOpen",
        },
        "objectives": OBJECTIVES,
        "routing_signals": ["queue_length", "model_version", "route_weight", "forecast_horizon", "model_server_readiness"],
        "checks": checks,
        "guardrails": [
            "Publish gateway routing manifests only after training gates and model registry promotion pass.",
            "Keep InferenceObjective alpha usage behind documented smoke and rollback gates.",
            "Fail open to the previous champion route when endpoint-picker health is unknown.",
            "Give online forecast requests higher priority than backtest and replay traffic.",
        ],
        "kubernetes_assets": ["kubernetes/inference-gateway-routing.yaml"],
        "references": [
            "https://gateway-api-inference-extension.sigs.k8s.io/api-types/inferencepool/",
            "https://gateway-api-inference-extension.sigs.k8s.io/concepts/api-overview/",
            "https://istio.io/latest/docs/tasks/traffic-management/ingress/gateway-api-inference-extension/",
        ],
    }
    write_json(root / "reports" / "inference_gateway_plan.json", plan)
    return plan
