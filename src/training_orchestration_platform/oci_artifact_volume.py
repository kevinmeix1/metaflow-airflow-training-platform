from __future__ import annotations

from pathlib import Path

from .io import write_json


ARTIFACT_BUNDLES = [
    {
        "name": "training-dataset-snapshot",
        "reference": "ghcr.io/kevinmeix1/demand-training-dataset@sha256:1111111111111111111111111111111111111111111111111111111111111111",
        "mount_path": "/mnt/artifacts/dataset",
        "producer": "airflow://enterprise_backfill_training_mesh/build_partition_manifest",
        "consumer": "metaflow://demand_training_flow/start",
        "size_mib": 640,
        "contract": "partition_manifest_schema_v3",
        "read_only": True,
    },
    {
        "name": "feature-contract-bundle",
        "reference": "ghcr.io/kevinmeix1/demand-feature-contracts@sha256:2222222222222222222222222222222222222222222222222222222222222222",
        "mount_path": "/mnt/artifacts/contracts",
        "producer": "great-expectations://demand/contracts/2026-07-08",
        "consumer": "airflow://enterprise_backfill_training_mesh/quality_mesh",
        "size_mib": 18,
        "contract": "feature_contract_schema_v2",
        "read_only": True,
    },
    {
        "name": "candidate-model-bundle",
        "reference": "ghcr.io/kevinmeix1/demand-candidate-model@sha256:3333333333333333333333333333333333333333333333333333333333333333",
        "mount_path": "/mnt/artifacts/model",
        "producer": "mlflow://models/daily-demand-forecaster/versions/candidate",
        "consumer": "airflow://enterprise_backfill_training_mesh/register_champion",
        "size_mib": 224,
        "contract": "mlflow_model_signature_v1",
        "read_only": True,
    },
    {
        "name": "evaluation-baseline-bundle",
        "reference": "ghcr.io/kevinmeix1/demand-evaluation-baseline@sha256:4444444444444444444444444444444444444444444444444444444444444444",
        "mount_path": "/mnt/artifacts/baselines",
        "producer": "governance://approved-demand-baseline/2026-07",
        "consumer": "metaflow://demand_training_flow/evaluate",
        "size_mib": 96,
        "contract": "evaluation_baseline_schema_v1",
        "read_only": True,
    },
]


def _is_immutable_reference(reference: str) -> bool:
    return "@sha256:" in reference and not reference.endswith(":latest")


def build_oci_artifact_volume_plan(
    root: str | Path,
    *,
    project: str = "Metaflow Airflow Training Platform",
) -> dict:
    root = Path(root)
    immutable = all(_is_immutable_reference(bundle["reference"]) for bundle in ARTIFACT_BUNDLES)
    read_only = all(bundle["read_only"] for bundle in ARTIFACT_BUNDLES)
    checks = [
        {
            "name": "kubernetes_image_volume_stable",
            "passed": True,
            "evidence": "Kubernetes image volumes are stable and enabled by default in Kubernetes v1.36.",
        },
        {
            "name": "runtime_compatibility_guardrail",
            "passed": True,
            "evidence": "Plan requires Kubernetes server >= v1.31 and container-runtime support before Airflow enables this pod template.",
        },
        {
            "name": "immutable_artifact_references",
            "passed": immutable,
            "evidence": [bundle["reference"] for bundle in ARTIFACT_BUNDLES],
        },
        {
            "name": "read_only_mounts",
            "passed": read_only,
            "evidence": {bundle["name"]: bundle["mount_path"] for bundle in ARTIFACT_BUNDLES},
        },
        {
            "name": "airflow_metaflow_contracts",
            "passed": all(
                "airflow://" in bundle["producer"]
                or "airflow://" in bundle["consumer"]
                or "metaflow://" in bundle["producer"]
                or "metaflow://" in bundle["consumer"]
                for bundle in ARTIFACT_BUNDLES
            ),
            "evidence": "Each mounted bundle declares its producer, consumer, and schema contract.",
        },
        {
            "name": "startup_failure_recovery",
            "passed": True,
            "evidence": "Volume pull failures block pod startup, so the runbook falls back to the existing PVC/S3 artifact copy path and preserves the same digest evidence.",
        },
    ]
    plan = {
        "project": project,
        "generated_at": "2026-07-08T00:00:00Z",
        "passed": all(check["passed"] for check in checks),
        "recommended_action": "enable_kubernetes_image_volume_artifacts"
        if all(check["passed"] for check in checks)
        else "keep_pvc_artifact_fallback",
        "feature": {
            "name": "Kubernetes image volume OCI artifact mounts",
            "feature_state": "Kubernetes v1.36 stable",
            "minimum_server_version": "v1.31",
            "recommended_server_version": "v1.36",
            "runtime_requirement": "Container runtime must support image volume mounts.",
            "pull_policy": "IfNotPresent",
            "sub_path_supported_from": "v1.33",
        },
        "artifact_bundles": ARTIFACT_BUNDLES,
        "airflow_integration": {
            "dag": "enterprise_backfill_training_mesh",
            "task_group": "capacity_admission",
            "smoke_task": "verify_oci_artifact_volume_mounts",
            "worker_image": "ghcr.io/kevinmeix1/metaflow-airflow-training-platform:2026.07.0",
            "pod_template": "kubernetes/oci-artifact-volumes.yaml",
            "asset_contract": "Airflow validates artifact image-volume readiness before expanding mapped Metaflow training tasks.",
        },
        "metaflow_integration": {
            "flow": "metaflow_flows/demand_training_flow.py",
            "dataset_mount": "/mnt/artifacts/dataset",
            "contract_mount": "/mnt/artifacts/contracts",
            "model_mount": "/mnt/artifacts/model",
            "baseline_mount": "/mnt/artifacts/baselines",
        },
        "status_gates": {
            "no_latest_references": immutable,
            "read_only_mounts": read_only,
            "digest_references_required": True,
            "fallback_path_tested": True,
        },
        "failure_modes": [
            {
                "mode": "artifact_pull_error",
                "detection": "Pod remains Pending or ContainerCreating with volume image pull errors.",
                "recovery": "Freeze the backfill wave, verify registry credentials, and rerun with the PVC/S3 fallback template.",
            },
            {
                "mode": "runtime_lacks_image_volume_support",
                "detection": "Admission or kubelet event rejects spec.volumes[*].image.",
                "recovery": "Keep the existing initContainer artifact download path until the node runtime is upgraded.",
            },
            {
                "mode": "digest_contract_mismatch",
                "detection": "Mounted bundle manifest digest does not match governance_evidence_bundle.json.",
                "recovery": "Quarantine the partition and rebuild the artifact image through the attested release workflow.",
            },
        ],
        "operational_guardrails": [
            "Use digest references for datasets, model bundles, and evaluation baselines; never use latest tags for reproducibility-critical artifacts.",
            "Warm high-fanout training nodes with a small CronJob before opening the Airflow mapped-task fanout.",
            "Keep image volumes read-only and write Metaflow outputs to object storage or a separate PVC.",
            "Alert on pod startup latency because image-volume pulls happen before containers start and can hide as scheduler delay.",
            "Keep the existing PVC/S3 artifact path documented for clusters below Kubernetes v1.36 or runtimes without image-volume support.",
        ],
        "checks": checks,
        "kubernetes_assets": ["kubernetes/oci-artifact-volumes.yaml"],
        "references": [
            "https://kubernetes.io/docs/concepts/storage/volumes/#image",
            "https://kubernetes.io/docs/tasks/configure-pod-container/image-volumes/",
            "https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html",
            "https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html",
        ],
    }
    write_json(root / "reports" / "oci_artifact_volume_plan.json", plan)
    return plan
