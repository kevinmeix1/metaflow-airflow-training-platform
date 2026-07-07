from __future__ import annotations

from pathlib import Path

from .io import write_json


ALLOWED_FLOWS = [
    {
        "source": "airflow-scheduler",
        "destination": "metaflow-training-worker",
        "port": 8793,
        "protocol": "worker log/control over mesh mTLS",
        "justification": "launch and monitor partition training tasks",
    },
    {
        "source": "metaflow-training-worker",
        "destination": "mlflow-tracking",
        "port": 5000,
        "protocol": "HTTP over mesh mTLS",
        "justification": "log experiment metrics and model artifacts",
    },
    {
        "source": "metaflow-training-worker",
        "destination": "object-storage",
        "port": 9000,
        "protocol": "S3 API over mesh mTLS",
        "justification": "read partition data and write model artifacts",
    },
]


DENIED_FLOWS = [
    {
        "source": "validation-job",
        "destination": "model-registry",
        "reason": "validators cannot promote models",
    },
    {
        "source": "training-worker",
        "destination": "airflow-metadata-db",
        "reason": "workers report through Airflow APIs and logs, not direct DB access",
    },
]


def build_network_security_report(root: str | Path) -> dict:
    root = Path(root)
    report = {
        "platform": "metaflow-airflow-training-platform",
        "namespace": "ml-training",
        "default_policy": "deny all ingress and egress, then allow training worker, MLflow, and artifact flows",
        "mtls_mode": "STRICT",
        "gateway_boundary": "training namespace has no public gateway; Airflow is the entrypoint",
        "allowed_flow_count": len(ALLOWED_FLOWS),
        "denied_flow_count": len(DENIED_FLOWS),
        "allowed_flows": ALLOWED_FLOWS,
        "denied_by_default": DENIED_FLOWS,
        "controls": [
            "default deny NetworkPolicy for batch namespace",
            "DNS egress allow for service discovery",
            "training workers may reach MLflow and object storage only",
            "Istio AuthorizationPolicy restricts worker launch to Airflow service account",
        ],
    }
    write_json(root / "reports" / "network_security.json", report)
    return report
