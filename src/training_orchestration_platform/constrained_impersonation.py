from __future__ import annotations

from pathlib import Path

from .io import write_json


DELEGATIONS = [
    {
        "name": "airflow-debugger-read-training",
        "namespace": "mlops-training",
        "impersonator": "system:serviceaccount:mlops-training:airflow-debugger",
        "target_service_account": "training-reader",
        "identity_permission": "impersonate:serviceaccount",
        "action_permissions": [
            "impersonate-on:serviceaccount:get",
            "impersonate-on:serviceaccount:list",
            "impersonate-on:serviceaccount:watch",
        ],
        "resources": ["jobs/status", "pods/log", "configmaps"],
        "risk_control": "Airflow support tooling can inspect training partitions without inheriting trainer create/delete authority.",
    },
    {
        "name": "backfill-controller-patch-partition-status",
        "namespace": "mlops-training",
        "impersonator": "system:serviceaccount:mlops-training:backfill-controller",
        "target_service_account": "partition-status-writer",
        "identity_permission": "impersonate:serviceaccount",
        "action_permissions": [
            "impersonate-on:serviceaccount:get",
            "impersonate-on:serviceaccount:patch",
        ],
        "resources": ["jobs/status", "events"],
        "risk_control": "Backfill automation can patch partition status but cannot create unbounded training Jobs.",
    },
]


def build_constrained_impersonation_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    feature = {
        "name": "ConstrainedImpersonation",
        "state": "Kubernetes v1.36 beta and enabled by default",
        "scope": "two-step authorization for impersonated identity plus scoped actions",
        "not_for": "broad legacy impersonate grants that inherit every target permission",
    }
    checks = [
        {
            "name": "beta_feature_status_recorded",
            "passed": feature["state"].startswith("Kubernetes v1.36 beta"),
            "evidence": "The plan records the feature status before relying on constrained impersonation.",
        },
        {
            "name": "identity_permission_is_separate",
            "passed": all(item["identity_permission"] == "impersonate:serviceaccount" for item in DELEGATIONS),
            "evidence": "Each delegation has an explicit service account identity grant.",
        },
        {
            "name": "action_permissions_are_constrained",
            "passed": all(
                all(verb.startswith("impersonate-on:serviceaccount:") for verb in item["action_permissions"])
                for item in DELEGATIONS
            ),
            "evidence": "Every delegated action uses impersonate-on service account verbs.",
        },
        {
            "name": "dangerous_verbs_excluded",
            "passed": all(
                not {"delete", "create"}.intersection({verb.rsplit(":", 1)[-1] for verb in item["action_permissions"]})
                for item in DELEGATIONS
            ),
            "evidence": "Debug and backfill delegation excludes broad create/delete authority.",
        },
        {
            "name": "audit_requirement_recorded",
            "passed": all(item["risk_control"] for item in DELEGATIONS),
            "evidence": "Each delegation documents the production risk it is narrowing.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-09T00:00:00Z",
        "recommended_action": "enable_constrained_impersonation_for_training_operations" if passed else "keep_legacy_impersonation_blocked",
        "passed": passed,
        "feature": feature,
        "delegations": DELEGATIONS,
        "checks": checks,
        "runbook": [
            "Grant impersonate:serviceaccount only for the exact training support target service account.",
            "Grant impersonate-on:serviceaccount verbs only for Airflow support and partition-status actions.",
            "Alert on legacy impersonate verbs that are not paired with constrained action grants.",
            "Inspect audit authenticationMetadata.impersonationConstraint during backfill investigations.",
        ],
        "references": [
            "https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/",
            "https://kubernetes.io/docs/reference/access-authn-authz/user-impersonation/",
        ],
    }
    write_json(root / "reports" / "constrained_impersonation_plan.json", plan)
    return plan
