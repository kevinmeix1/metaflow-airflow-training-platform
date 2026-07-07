from __future__ import annotations

from pathlib import Path

from .io import write_json


def build_gitops_plan(root: str | Path) -> dict:
    root = Path(root)
    plan = {
        "platform": "metaflow-airflow-training-platform",
        "deployment_controller": "Argo CD",
        "progressive_delivery": "Argo Rollouts controller rollout with backfill SLO analysis",
        "config_repo_pattern": "separate training-platform manifests with pinned worker images",
        "sync_waves": [
            {"wave": -3, "name": "security-and-network", "resources": ["NetworkPolicy", "PeerAuthentication", "AuthorizationPolicy"]},
            {"wave": -2, "name": "quota-and-batch-capacity", "resources": ["Kueue queues", "Airflow pools", "VPA recommender"]},
            {"wave": -1, "name": "pre-sync-training-gates", "resources": ["partition dry-run", "lineage check", "policy audit"]},
            {"wave": 0, "name": "training-control-plane", "resources": ["Airflow DAGs", "Metaflow worker image", "batch templates"]},
            {"wave": 1, "name": "post-sync-backfill-analysis", "resources": ["small backfill smoke test", "artifact lineage verification"]},
        ],
        "promotion_stages": [
            {"environment": "dev", "sync": "automated", "self_heal": True, "approval": "pull request"},
            {"environment": "staging", "sync": "automated", "self_heal": True, "approval": "training owner approval"},
            {"environment": "prod", "sync": "manual", "self_heal": False, "approval": "change ticket plus successful smoke backfill"},
        ],
        "gates": [
            "partition dry-run creates immutable manifests",
            "lineage catalog contains expected assets",
            "resource plan does not exceed Kueue nominal quota",
            "smoke backfill succeeds",
            "MLflow artifact logging latency stays below threshold",
        ],
        "rollback": {
            "command": "argocd app rollback metaflow-airflow-training-platform <history-id>",
            "runtime": "argo rollouts abort training-control-plane -n ml-training",
            "evidence": ".local/orchestration/run_log.jsonl and .local/reports/backfill_capacity_plan.json",
        },
    }
    write_json(root / "reports" / "gitops_plan.json", plan)
    return plan
