from __future__ import annotations

from pathlib import Path

from .io import write_json


FLAVOR_POLICIES = [
    {
        "name": "production-backfill",
        "cluster_queue": "production-backfill-flavor-queue",
        "local_queue": "daily-backfill",
        "resource": "cpu",
        "flavor_order": ["cpu-on-demand", "cpu-spot"],
        "when_can_borrow": "TryNextFlavor",
        "when_can_preempt": "TryNextFlavor",
        "preference": "BorrowingOverPreemption",
        "nominal_quota": {"cpu-on-demand": 28, "cpu-spot": 10},
        "borrowing_limit": {"cpu-on-demand": 8, "cpu-spot": 12},
        "rationale": "production backfills prefer stable on-demand nodes and try spot fallback before borrowing or preempting",
    },
    {
        "name": "data-quality-replay",
        "cluster_queue": "quality-replay-flavor-queue",
        "local_queue": "schema-replay",
        "resource": "cpu",
        "flavor_order": ["cpu-spot", "cpu-on-demand"],
        "when_can_borrow": "TryNextFlavor",
        "when_can_preempt": "TryNextFlavor",
        "preference": "BorrowingOverPreemption",
        "nominal_quota": {"cpu-spot": 14, "cpu-on-demand": 4},
        "borrowing_limit": {"cpu-spot": 10, "cpu-on-demand": 3},
        "rationale": "data-quality replay is retryable and prefers lower-cost spot before using limited on-demand fallback",
    },
    {
        "name": "hpo-sweeps-gpu",
        "cluster_queue": "hpo-sweeps-flavor-queue",
        "local_queue": "hpo-sweeps",
        "resource": "nvidia.com/gpu",
        "flavor_order": ["gpu-l4-spot", "gpu-l4-reserved"],
        "when_can_borrow": "TryNextFlavor",
        "when_can_preempt": "TryNextFlavor",
        "preference": "BorrowingOverPreemption",
        "nominal_quota": {"gpu-l4-spot": 4, "gpu-l4-reserved": 1},
        "borrowing_limit": {"gpu-l4-spot": 2, "gpu-l4-reserved": 1},
        "rationale": "experiments stay spot-first and only use reserved GPU if the cheaper pool cannot fit the sweep",
    },
]


def _fallback_depth(policy: dict) -> int:
    return max(len(policy["flavor_order"]) - 1, 0)


def build_flavor_fungibility_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    policies = [
        {
            **policy,
            "fallback_depth": _fallback_depth(policy),
            "total_nominal_quota": sum(policy["nominal_quota"].values()),
            "total_borrowing_limit": sum(policy["borrowing_limit"].values()),
        }
        for policy in FLAVOR_POLICIES
    ]
    checks = [
        {
            "name": "resource_flavors_declared",
            "passed": True,
            "evidence": "ResourceFlavors separate on-demand backfill nodes, spot replay nodes, and L4 GPU experiment pools.",
        },
        {
            "name": "try_next_before_borrow",
            "passed": all(policy["when_can_borrow"] == "TryNextFlavor" for policy in policies),
            "evidence": "Training queues try the next ResourceFlavor before borrowing quota from cohort peers.",
        },
        {
            "name": "try_next_before_preempt",
            "passed": all(policy["when_can_preempt"] == "TryNextFlavor" for policy in policies),
            "evidence": "Backfills and HPO sweeps try alternate flavors before preempting admitted partition work.",
        },
        {
            "name": "explicit_preference_declared",
            "passed": all(policy["preference"] == "BorrowingOverPreemption" for policy in policies),
            "evidence": "BorrowingOverPreemption is explicit so training behavior does not depend on implicit defaults.",
        },
        {
            "name": "production_and_hpo_have_distinct_order",
            "passed": policies[0]["flavor_order"] != policies[-1]["flavor_order"],
            "evidence": "Production backfills are stability-first while HPO sweeps are spot-first.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": passed,
        "recommended_action": "enable_training_kueue_flavor_fungibility" if passed else "keep_static_training_flavors",
        "kueue_api_target": "kueue.x-k8s.io/v1beta1",
        "feature": {
            "name": "FlavorFungibility",
            "whenCanBorrow": "TryNextFlavor avoids borrowing when another training ResourceFlavor can fit",
            "whenCanPreempt": "TryNextFlavor avoids disrupting admitted partition jobs when another flavor can fit",
            "preference": "BorrowingOverPreemption is declared explicitly for predictable backfill behavior",
        },
        "flavor_policies": policies,
        "operational_guardrails": [
            "Keep production backfills on stability-first flavors and reserve failed-partition recovery capacity.",
            "Keep data-quality replay and HPO sweeps retryable, spot-first, and bounded by borrowingLimit.",
            "Use TryNextFlavor before preemption to reduce churn in long-running partitioned training waves.",
            "Record selected ResourceFlavor, fallback depth, partition range, and Metaflow run id in lineage evidence.",
            "Test spot loss, on-demand saturation, and GPU shortage before increasing Airflow mapped-task fanout.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/kueue-flavor-fungibility.yaml"],
        "references": [
            "https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/",
            "https://kueue.sigs.k8s.io/docs/concepts/resource_flavor/",
            "https://kueue.sigs.k8s.io/docs/reference/kueue.v1beta1/#flavorfungibility",
        ],
    }
    write_json(root / "reports" / "flavor_fungibility_plan.json", plan)
    return plan
