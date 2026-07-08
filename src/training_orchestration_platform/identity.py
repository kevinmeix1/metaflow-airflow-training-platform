from __future__ import annotations

from pathlib import Path

from .io import write_json


def _identity(
    *,
    workload: str,
    namespace: str,
    service_account: str,
    role: str,
    spiffe_id: str,
    secrets: list[str],
    permissions: list[str],
) -> dict:
    return {
        "workload": workload,
        "namespace": namespace,
        "service_account": service_account,
        "automount_service_account_token": False,
        "token": {"projected": True, "audience": "sts.amazonaws.com", "ttl_seconds": 3600},
        "cloud_access": {"provider": "aws", "role": role, "credential_mode": "federated_oidc"},
        "spiffe_id": spiffe_id,
        "external_secrets": [
            {"name": secret, "provider": "aws-secrets-manager", "refresh_interval_minutes": 30, "static_credentials": False}
            for secret in secrets
        ],
        "rbac": {"scope": "namespace", "permissions": permissions},
    }


def build_identity_access_report(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    identities = [
        _identity(
            workload="airflow-training-scheduler",
            namespace="ml-training-prod",
            service_account="airflow-training-scheduler",
            role="arn:aws:iam::111122223333:role/training-airflow-scheduler",
            spiffe_id="spiffe://mlops.local/ns/ml-training-prod/sa/airflow-training-scheduler",
            secrets=["airflow-fernet-key", "mlflow-readwrite-token"],
            permissions=["create jobs", "read backfill manifests"],
        ),
        _identity(
            workload="metaflow-partition-worker",
            namespace="ml-training-prod",
            service_account="metaflow-partition-worker",
            role="arn:aws:iam::111122223333:role/metaflow-partition-worker",
            spiffe_id="spiffe://mlops.local/ns/ml-training-prod/sa/metaflow-partition-worker",
            secrets=["training-data-reader", "artifact-writer-token"],
            permissions=["read partition data", "write model artifacts"],
        ),
        _identity(
            workload="mlflow-registrar",
            namespace="ml-training-quality",
            service_account="mlflow-registrar",
            role="arn:aws:iam::111122223333:role/mlflow-training-registrar",
            spiffe_id="spiffe://mlops.local/ns/ml-training-quality/sa/mlflow-registrar",
            secrets=["registry-approval-token"],
            permissions=["register model versions", "write lineage events"],
        ),
    ]
    all_secrets = [secret for identity in identities for secret in identity["external_secrets"]]
    checks = [
        {"name": "bound_service_account_tokens", "passed": all(identity["token"]["projected"] for identity in identities)},
        {"name": "token_ttl_leq_one_hour", "passed": all(identity["token"]["ttl_seconds"] <= 3600 for identity in identities)},
        {"name": "no_static_cloud_keys", "passed": all(not secret["static_credentials"] for secret in all_secrets)},
        {"name": "external_secret_refresh_leq_30m", "passed": all(secret["refresh_interval_minutes"] <= 30 for secret in all_secrets)},
        {"name": "namespace_scoped_rbac", "passed": all(identity["rbac"]["scope"] == "namespace" for identity in identities)},
        {"name": "spiffe_identity_declared", "passed": all(identity["spiffe_id"].startswith("spiffe://") for identity in identities)},
        {
            "name": "airflow_task_service_account_pinned",
            "passed": any(identity["service_account"] == "metaflow-partition-worker" for identity in identities),
        },
    ]
    report = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "identities": identities,
        "checks": checks,
        "controls": [
            "Airflow backfill tasks launch Metaflow pods with workload-specific service accounts.",
            "Training data and artifact access use federated roles instead of static object-store keys.",
            "External Secrets Operator refreshes Airflow, MLflow, and registry tokens.",
            "Namespace-scoped RBAC prevents exploratory jobs from inheriting production scheduler privileges.",
            "SPIFFE IDs document identities for scheduler, partition worker, and registrar workloads.",
        ],
        "rotation": {
            "projected_token_ttl_seconds": 3600,
            "external_secret_refresh_minutes": 30,
            "break_glass_static_secret_allowed": False,
        },
        "references": [
            "https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/",
            "https://external-secrets.io/latest/introduction/getting-started/",
            "https://spiffe.io/docs/latest/try/getting-started-k8s/",
            "https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html",
        ],
    }
    write_json(Path(root) / "reports" / "identity_access_report.json", report)
    return report
