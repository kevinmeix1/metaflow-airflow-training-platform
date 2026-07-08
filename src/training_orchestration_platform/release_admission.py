from __future__ import annotations

from pathlib import Path

from .io import read_json, write_json


def _load(path: Path, default: dict) -> dict:
    return read_json(path) if path.exists() else default


def _check(name: str, passed: bool, observed: object, *, owner: str, action: str) -> dict:
    return {
        "name": name,
        "passed": passed,
        "observed": observed,
        "owner": owner,
        "action": action if not passed else "none",
    }


def evaluate_release_admission(
    *,
    slo: dict,
    performance: dict,
    queue: dict,
    governance: dict,
    supply_chain: dict,
    capacity_plan: dict,
) -> dict:
    max_burn = float(slo.get("max_burn_rate", 0.0))
    release_freeze = bool(slo.get("release_freeze", False))
    performance_passed = bool(performance.get("passed", False))
    queue_passed = bool(queue.get("passed", False))
    recovery_pending = [
        item["name"]
        for item in queue.get("simulation", {}).get("pending", [])
        if int(item.get("priority", 0)) >= 900
    ]
    governance_decision = governance.get("release", {}).get("decision", "unknown")
    attestation_ready = (
        int(supply_chain.get("artifact_count", 0)) > 0
        and supply_chain.get("subject", {}).get("attestation_action") == "actions/attest@v4"
    )
    wave_count = int(capacity_plan.get("wave_count", 0))
    checks = [
        _check(
            "training_slo_error_budget",
            max_burn < 6.0 and not release_freeze,
            {"max_burn_rate": max_burn, "recommended_action": slo.get("recommended_action")},
            owner="training-platform",
            action="throttle_bulk_backfill",
        ),
        _check(
            "performance_budget",
            performance_passed,
            {"failed": [check["name"] for check in performance.get("checks", []) if not check.get("passed")]},
            owner="ml-platform",
            action="hold_backfill_wave",
        ),
        _check(
            "failed_partition_recovery_capacity",
            queue_passed and not recovery_pending,
            {"pending_count": queue.get("pending_count", 0), "critical_pending": recovery_pending},
            owner="orchestration",
            action="reserve_recovery_capacity",
        ),
        _check(
            "partition_governance",
            governance_decision == "approved_training_artifact",
            governance_decision,
            owner="risk",
            action="require_partition_approval",
        ),
        _check(
            "supply_chain_attestation",
            attestation_ready,
            supply_chain.get("subject", {}),
            owner="platform-security",
            action="wait_for_provenance",
        ),
        _check(
            "capacity_plan_exists",
            wave_count > 0,
            {"wave_count": wave_count, "workload_count": capacity_plan.get("workload_count", 0)},
            owner="airflow",
            action="dry_run_backfill_capacity",
        ),
    ]
    if max_burn >= 14.4:
        action = "freeze_backfills_and_page"
    elif release_freeze or max_burn >= 6.0:
        action = "throttle_bulk_backfill"
    elif not queue_passed or recovery_pending:
        action = "reserve_failed_partition_recovery"
    elif all(check["passed"] for check in checks):
        action = "admit_backfill_wave"
    else:
        action = "hold_backfill_wave"
    return {
        "recommended_action": action,
        "admitted": action == "admit_backfill_wave",
        "unsafe_allow": action == "admit_backfill_wave" and not all(check["passed"] for check in checks),
        "checks": checks,
        "failure_policy": "fail_closed",
    }


def build_release_admission_decision(root: str | Path) -> dict:
    root = Path(root)
    decision = evaluate_release_admission(
        slo=_load(root / "reports" / "slo_error_budget.json", {}),
        performance=_load(root / "reports" / "performance_budget.json", {}),
        queue=_load(root / "reports" / "queue_simulation.json", {}),
        governance=_load(root / "reports" / "governance_evidence_bundle.json", {}),
        supply_chain=_load(root / "reports" / "supply_chain_evidence.json", {}),
        capacity_plan=_load(root / "reports" / "backfill_capacity_plan.json", {}),
    )
    record = {
        "project": "Metaflow Airflow Training Platform",
        "target": "airflow://ml-training/demand-training-backfill",
        "evaluated_at": "2026-07-08T00:00:00Z",
        "decision": decision,
        "policy_inputs": {
            "slo": "reports/slo_error_budget.json",
            "performance": "reports/performance_budget.json",
            "queue": "reports/queue_simulation.json",
            "governance": "reports/governance_evidence_bundle.json",
            "supply_chain": "reports/supply_chain_evidence.json",
            "capacity": "reports/backfill_capacity_plan.json",
        },
        "enforcement_points": [
            "Airflow asset-aware backfill DAG admits only planned waves with fresh capacity evidence.",
            "Kubernetes ValidatingAdmissionPolicy requires training jobs to carry release-decision and evidence-sha annotations.",
            "Argo Rollouts analysis is used for the training control plane before worker image promotion.",
            "Kueue priority keeps failed-partition recovery ahead of bulk backfill expansion.",
        ],
        "references": [
            "https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/",
            "https://argo-rollouts.readthedocs.io/en/stable/features/analysis/",
            "https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html",
            "https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/",
        ],
    }
    write_json(root / "reports" / "release_admission_decision.json", record)
    return record
