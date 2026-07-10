from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from training_orchestration_platform.accelerator_plan import build_accelerator_capacity_plan
from training_orchestration_platform.admin_access_diagnostics import build_admin_access_diagnostic_plan
from training_orchestration_platform.advanced_device_sharing import build_advanced_device_sharing_plan
from training_orchestration_platform.airflow_stateful_orchestration import build_airflow_stateful_orchestration_plan
from training_orchestration_platform.asset_partitioning import build_asset_partitioning_plan
from training_orchestration_platform.capacity_planner import build_backfill_plan, pack_waves
from training_orchestration_platform.chaos import run_chaos_drill
from training_orchestration_platform.cloud_migration import build_cloud_migration_plan
from training_orchestration_platform.cli import demo
from training_orchestration_platform.cohort_fair_sharing import build_cohort_fair_sharing_plan
from training_orchestration_platform.control_plane_diagnostics import build_control_plane_diagnostics_plan
from training_orchestration_platform.constrained_impersonation import build_constrained_impersonation_plan
from training_orchestration_platform.cost_observability import build_cost_observability_report
from training_orchestration_platform.dag_bundle_versioning import build_dag_bundle_versioning_plan
from training_orchestration_platform.data import generate_partition, validate_rows
from training_orchestration_platform.deadline_alerts import build_deadline_alert_plan
from training_orchestration_platform.device_allocation import build_device_allocation_plan
from training_orchestration_platform.disaster_recovery import build_disaster_recovery_plan
from training_orchestration_platform.elastic_workload import build_elastic_workload_plan
from training_orchestration_platform.event_driven_assets import build_event_driven_assets_plan
from training_orchestration_platform.flavor_fungibility import build_flavor_fungibility_plan
from training_orchestration_platform.gitops_release import build_gitops_plan
from training_orchestration_platform.governance import build_governance_bundle
from training_orchestration_platform.hpa_scale_to_zero import build_hpa_scale_to_zero_plan
from training_orchestration_platform.identity import build_identity_access_report
from training_orchestration_platform.indexed_job_resilience import build_indexed_job_resilience_plan
from training_orchestration_platform.inplace_resize import build_inplace_resize_plan
from training_orchestration_platform.inference_gateway import build_inference_gateway_plan
from training_orchestration_platform.io import read_csv, read_json, read_jsonl, write_json
from training_orchestration_platform.kuberay_capacity import build_kuberay_capacity_plan
from training_orchestration_platform.memory_qos import build_memory_qos_plan
from training_orchestration_platform.model import evaluate_gates
from training_orchestration_platform.multi_team_readiness import build_multi_team_readiness_plan
from training_orchestration_platform.multikueue_dispatch import build_multikueue_dispatch_plan
from training_orchestration_platform.network_security import build_network_security_report
from training_orchestration_platform.orchestrator import backfill, run_log_path, run_partition
from training_orchestration_platform.orchestration_scorecard import build_orchestration_scorecard
from training_orchestration_platform.oci_artifact_volume import build_oci_artifact_volume_plan
from training_orchestration_platform.pending_workload_visibility import build_pending_workload_visibility_plan
from training_orchestration_platform.policy_audit import audit_platform_policy
from training_orchestration_platform.performance_budget import build_performance_budget_report
from training_orchestration_platform.pod_resource_envelopes import build_pod_resource_envelope_plan
from training_orchestration_platform.provisioning_admission import build_provisioning_admission_plan
from training_orchestration_platform.queue_simulator import build_queue_simulation
from training_orchestration_platform.release_admission import build_release_admission_decision, evaluate_release_admission
from training_orchestration_platform.resource_health_status import build_resource_health_status_plan
from training_orchestration_platform.resource_optimizer import build_resource_optimization_report
from training_orchestration_platform.runtime_security import build_runtime_security_plan
from training_orchestration_platform.semantic_telemetry import build_semantic_telemetry_plan
from training_orchestration_platform.slo import build_slo_report
from training_orchestration_platform.supply_chain import build_supply_chain_evidence
from training_orchestration_platform.suspended_job_resources import build_suspended_job_resource_plan
from training_orchestration_platform.tenancy import build_tenancy_report
from training_orchestration_platform.topology_placement import build_topology_placement_plan
from training_orchestration_platform.traceability import build_trace_report
from training_orchestration_platform.workload_aware_scheduling import build_workload_aware_scheduling_plan


class TrainingOrchestrationPlatformTest(unittest.TestCase):
    def test_advanced_training_mesh_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        dag = repo / "airflow" / "dags" / "enterprise_backfill_training_mesh_dag.py"
        workloads = repo / "kubernetes" / "training-mesh-workloads.yaml"

        dag_text = dag.read_text(encoding="utf-8")
        workload_text = workloads.read_text(encoding="utf-8")

        for expected in ["KubernetesPodOperator", "task_group", "BranchPythonOperator", "Asset", "expand("]:
            self.assertIn(expected, dag_text)
        for expected in ["deferrable=True", "pod_template_file", "capacity_admission", "reserve_kueue_backfill_quota"]:
            self.assertIn(expected, dag_text)
        for expected in ["TRAINING_IMAGE", "2026.07.0", "image_pull_policy=\"IfNotPresent\"", "verify_oci_artifact_volume_mounts"]:
            self.assertIn(expected, dag_text)
        for expected in ["CronJob", "completionMode: Indexed", "parallelism: 4", "RoleBinding", "ConfigMap", "kueue.x-k8s.io/queue-name"]:
            self.assertIn(expected, workload_text)

    def test_kubernetes_governance_and_airflow_pod_template_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        governance = (repo / "kubernetes" / "platform-governance.yaml").read_text(encoding="utf-8")
        pod_template = (repo / "kubernetes" / "airflow-kubernetes-executor-pod-template.yaml").read_text(encoding="utf-8")

        for expected in ["ResourceQuota", "LimitRange", "PriorityClass", "PodDisruptionBudget"]:
            self.assertIn(expected, governance)
        for expected in ["initContainers", "nodeSelector", "tolerations", "emptyDir", "securityContext"]:
            self.assertIn(expected, pod_template)

    def test_kueue_backfill_admission_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        admission = (repo / "kubernetes" / "kueue-admission-control.yaml").read_text(encoding="utf-8")

        for expected in [
            "ResourceFlavor",
            "ClusterQueue",
            "LocalQueue",
            "WorkloadPriorityClass",
            "demand-training-queue",
            "completionMode: Indexed",
            "backoffLimitPerIndex",
            "borrowingLimit",
            "preemption",
        ]:
            self.assertIn(expected, admission)

    def test_event_driven_autoscaling_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        autoscaling = (repo / "kubernetes" / "event-driven-autoscaling.yaml").read_text(encoding="utf-8")

        for expected in ["ScaledJob", "kafka", "lagThreshold", "limitToPartitionsWithLag", "demand-training-queue"]:
            self.assertIn(expected, autoscaling)

    def test_queue_simulation_models_backfill_recovery_priority(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "queue-simulation-policy.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_queue_simulation(root)

            self.assertTrue(report["passed"])
            self.assertGreaterEqual(report["preempted_count"], 1)
            self.assertTrue(any(item["name"] == "failed-partition-smoke-replay" for item in report["simulation"]["admitted"]))
            self.assertTrue((root / "reports" / "queue_simulation.json").exists())
            self.assertIn("PriorityClass", manifest)
            self.assertIn("DemandTrainingQueuePressureHigh", manifest)

    def test_release_admission_admits_or_throttles_backfills(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "release-admission-policy.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "reports" / "slo_error_budget.json", {"max_burn_rate": 0.2, "release_freeze": False, "recommended_action": "allow_backfill"})
            write_json(root / "reports" / "performance_budget.json", {"passed": True, "checks": []})
            write_json(root / "reports" / "queue_simulation.json", {"passed": True, "pending_count": 0, "simulation": {"pending": []}})
            write_json(root / "reports" / "governance_evidence_bundle.json", {"release": {"decision": "approved_training_artifact"}})
            write_json(root / "reports" / "supply_chain_evidence.json", {"artifact_count": 8, "subject": {"attestation_action": "actions/attest@v4"}})
            write_json(root / "reports" / "backfill_capacity_plan.json", {"wave_count": 2, "workload_count": 6})

            decision = build_release_admission_decision(root)
            throttled = evaluate_release_admission(
                slo={"max_burn_rate": 7.0, "release_freeze": True, "recommended_action": "hold_bulk_backfills"},
                performance={"passed": True, "checks": []},
                queue={"passed": True, "pending_count": 0, "simulation": {"pending": []}},
                governance={"release": {"decision": "approved_training_artifact"}},
                supply_chain={"artifact_count": 8, "subject": {"attestation_action": "actions/attest@v4"}},
                capacity_plan={"wave_count": 2, "workload_count": 6},
            )

            self.assertEqual(decision["decision"]["recommended_action"], "admit_backfill_wave")
            self.assertFalse(decision["decision"]["unsafe_allow"])
            self.assertEqual(throttled["recommended_action"], "throttle_bulk_backfill")
            self.assertTrue((root / "reports" / "release_admission_decision.json").exists())
            self.assertIn("ValidatingAdmissionPolicy", manifest)
            self.assertIn("AnalysisTemplate", manifest)
            self.assertIn("DemandTrainingAdmissionUnsafeAllow", manifest)

    def test_performance_budget_report_and_prometheus_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "performance-budget-policy.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = demo(root)
            report = build_performance_budget_report(root)
            names = {check["name"] for check in report["checks"]}

            self.assertTrue(result["performance_budget"]["passed"])
            self.assertTrue(report["passed"])
            self.assertIn("successful_partitions", names)
            self.assertIn("backfill_wave_count", names)
            self.assertIn("failed_partition_recovery_minutes", names)
            self.assertTrue((root / "reports" / "performance_budget.json").exists())
            self.assertIn("PrometheusRule", manifest)
            self.assertIn("histogram_quantile", manifest)
            self.assertIn("DemandTrainingQueueBudgetExceeded", manifest)

    def test_admission_policies_and_policy_audit_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        admission = (repo / "kubernetes" / "admission-policies.yaml").read_text(encoding="utf-8")

        for expected in ["ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding", "ImageValidatingPolicy", "slsa-provenance"]:
            self.assertIn(expected, admission)
        with tempfile.TemporaryDirectory() as tmp:
            report = audit_platform_policy(repo, output_root=tmp)
            passed = {check["name"] for check in report["checks"] if check["passed"]}
            self.assertIn("indexed_backfill", passed)
            self.assertIn("event_driven_scaling", passed)
            self.assertIn("immutable_image_digest", passed)
            self.assertIn("no_latest_image_tags", passed)

    def test_trace_report_and_otel_collector_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        collector = (repo / "kubernetes" / "opentelemetry-collector.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            trace = build_trace_report(tmp)

            self.assertEqual(trace["span_count"], 5)
            self.assertEqual(trace["root_service"], "airflow")
            self.assertTrue(any(span["service"] == "openlineage" for span in trace["spans"]))
            airflow_attrs = trace["spans"][0]["attributes"]
            self.assertEqual(airflow_attrs["airflow.pool.name"], "metaflow_training_pool")
            self.assertTrue(any(span["attributes"].get("metaflow.run_id") == "demand-2026-06-06-recovery" for span in trace["spans"]))
            self.assertTrue(any(span["attributes"].get("openlineage.dataset.name") == "daily_demand_model" for span in trace["spans"]))
            self.assertTrue((Path(tmp) / "reports" / "trace_report.json").exists())
        for expected in ["kind: ConfigMap", "otlp", "k8sattributes", "memory_limiter", "attributes/semantic_redaction", "training.row_sample", "pii.customer_id", "prometheus", "batch"]:
            self.assertIn(expected, collector)

    def test_chaos_drill_and_chaos_mesh_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        chaos_manifest = (repo / "kubernetes" / "chaos-experiments.yaml").read_text(encoding="utf-8")

        for expected in ["PodChaos", "NetworkChaos", "StressChaos", "Schedule", "concurrencyPolicy: Forbid", "partition-worker-pod-kill"]:
            self.assertIn(expected, chaos_manifest)
        with tempfile.TemporaryDirectory() as tmp:
            report = run_chaos_drill(tmp)

            self.assertTrue(report["passed"])
            self.assertEqual(report["scenario_count"], 3)
            self.assertTrue(any(scenario["fault"] == "StressChaos" for scenario in report["scenarios"]))
            self.assertTrue((Path(tmp) / "reports" / "chaos_drill_report.json").exists())

    def test_resource_optimization_and_autoscaling_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        optimization = (repo / "kubernetes" / "resource-optimization.yaml").read_text(encoding="utf-8")

        for expected in ["VerticalPodAutoscaler", "HorizontalPodAutoscaler", "PrometheusRule", "airflow-capacity-pools", "stabilizationWindowSeconds: 300"]:
            self.assertIn(expected, optimization)
        with tempfile.TemporaryDirectory() as tmp:
            report = build_resource_optimization_report(tmp)

            self.assertEqual(report["summary"]["workload_count"], 3)
            self.assertIn("Kueue nominal quota", " ".join(report["guardrails"]))
            self.assertTrue(any("reduce_wave_width" in item["actions"] for item in report["recommendations"]))
            self.assertTrue((Path(tmp) / "reports" / "resource_optimization.json").exists())

    def test_network_security_topology_and_manifests_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        network_security = (repo / "kubernetes" / "network-security.yaml").read_text(encoding="utf-8")

        for expected in ["kind: NetworkPolicy", "default-deny-all", "PeerAuthentication", "mode: STRICT", "AuthorizationPolicy"]:
            self.assertIn(expected, network_security)
        with tempfile.TemporaryDirectory() as tmp:
            report = build_network_security_report(tmp)

            self.assertEqual(report["mtls_mode"], "STRICT")
            self.assertEqual(report["allowed_flow_count"], 3)
            self.assertTrue(any(flow["destination"] == "object-storage" for flow in report["allowed_flows"]))
            self.assertTrue((Path(tmp) / "reports" / "network_security.json").exists())

    def test_gitops_plan_and_progressive_delivery_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        gitops = (repo / "gitops" / "gitops-promotion.yaml").read_text(encoding="utf-8")

        for expected in ["kind: Application", "kind: AppProject", "AnalysisTemplate", "Rollout", "argocd.argoproj.io/sync-wave"]:
            self.assertIn(expected, gitops)
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_gitops_plan(tmp)

            self.assertEqual(plan["deployment_controller"], "Argo CD")
            self.assertIn("backfill SLO", plan["progressive_delivery"])
            self.assertTrue(any("smoke backfill" in gate for gate in plan["gates"]))
            self.assertTrue((Path(tmp) / "reports" / "gitops_plan.json").exists())

    def test_disaster_recovery_plan_and_backup_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        dr = (repo / "kubernetes" / "disaster-recovery.yaml").read_text(encoding="utf-8")

        for expected in ["kind: Schedule", "BackupStorageLocation", "VolumeSnapshotClass", "restore-order"]:
            self.assertIn(expected, dr)
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_disaster_recovery_plan(tmp)

            self.assertLessEqual(plan["rpo_minutes"], 30)
            self.assertEqual(plan["restore_sequence"][0]["asset"], "namespace and batch CRDs")
            self.assertTrue(any(item["asset"] == "backfill replay" for item in plan["restore_sequence"]))
            self.assertTrue((Path(tmp) / "reports" / "disaster_recovery_plan.json").exists())

    def test_governance_evidence_bundle_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        governance = (repo / "kubernetes" / "governance-evidence.yaml").read_text(encoding="utf-8")

        for expected in ["kind: ConfigMap", "kind: Job", "model-card", "risk-register", "reproducibility-manifest"]:
            self.assertIn(expected, governance)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = demo(root)
            bundle = build_governance_bundle(root)
            data_card = read_json(root / "governance" / "data_card.json")
            manifest = read_json(root / "governance" / "reproducibility_manifest.json")

            self.assertEqual(result["governance_bundle"]["release"]["decision"], "approved_training_artifact")
            self.assertEqual(bundle["release"]["model_name"], "daily-demand-forecaster")
            self.assertEqual(data_card["latest_partition"], "2026-06-06")
            self.assertTrue(any(item["exists"] and len(item["sha256"]) == 64 for item in manifest["artifact_hashes"]))
            self.assertTrue((root / "reports" / "governance_evidence_bundle.json").exists())

    def test_slo_error_budget_report_and_alert_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        alerts = (repo / "kubernetes" / "slo-alerts.yaml").read_text(encoding="utf-8")

        for expected in ["PrometheusRule", "SLOBurnRateHigh", "multiwindow", "error-budget-freeze"]:
            self.assertIn(expected, alerts)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = demo(root)
            report = build_slo_report(root)

            self.assertEqual(result["slo_error_budget"]["recommended_action"], "hold_bulk_backfills")
            self.assertEqual(report["slos"][0]["name"], "partition_training_success")
            self.assertEqual(report["run_counts"]["recovered_failed_dates"], 1)
            self.assertTrue((root / "reports" / "slo_error_budget.json").exists())

    def test_cloud_migration_plan_and_infra_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        nodepools = (repo / "kubernetes" / "cloud-nodepools.yaml").read_text(encoding="utf-8")
        terraform = (repo / "infra" / "terraform" / "aws" / "main.tf").read_text(encoding="utf-8")

        for expected in ["NodePool", "EC2NodeClass", "WhenEmptyOrUnderutilized"]:
            self.assertIn(expected, nodepools)
        for expected in ["cluster_compute_config", "node_pools", "aws_s3_bucket"]:
            self.assertIn(expected, terraform)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = demo(root)
            plan = build_cloud_migration_plan(root)

            self.assertEqual(result["cloud_migration"]["primary_target"], "AWS EKS Auto Mode")
            self.assertEqual(plan["managed_service_mapping"]["queueing"], "Kueue on EKS for batch admission and fair sharing")
            self.assertTrue((root / "reports" / "cloud_migration_plan.json").exists())

    def test_ci_workflow_uploads_artifacts_and_validates_outputs(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        workflow = (repo / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        makefile = (repo / "Makefile").read_text(encoding="utf-8")

        for expected in ["actions/upload-artifact@v6", "actions/attest@v4", "attestations: write", "GITHUB_STEP_SUMMARY", "make ci-verify", "concurrency"]:
            self.assertIn(expected, workflow)
        for expected in ["ci-verify:", "index.html", "tenancy_fairness_report.json", "identity_access_report.json", "pending_workload_visibility_plan.json", "flavor_fungibility_plan.json", "cohort_fair_sharing_plan.json", "pod_resource_envelope_plan.json", "event_driven_assets_plan.json", "multi_team_readiness_plan.json", "asset_partitioning_plan.json", "dag_bundle_versioning_plan.json", "multikueue_dispatch_plan.json", "oci_artifact_volume_plan.json", "provisioning_admission_plan.json", "indexed_job_resilience_plan.json", "elastic_workload_plan.json", "cost_observability_report.json", "deadline_alert_plan.json", "semantic_telemetry_plan.json", "inference_gateway_plan.json", "kuberay_capacity_plan.json", "topology_placement_plan.json", "inplace_resize_plan.json", "admin_access_diagnostics_plan.json", "advanced_device_sharing_plan.json", "resource_health_status_plan.json", "release_admission_decision.json", "runtime_security_plan.json", "control_plane_diagnostics_plan.json", "memory_qos_plan.json", "hpa_scale_to_zero_plan.json", "suspended_job_resources_plan.json", "constrained_impersonation_plan.json", "workload_aware_scheduling_plan.json", "queue_simulation.json", "performance_budget.json", "device_allocation_plan.json", "accelerator_capacity_plan.json", "orchestration_scorecard.json", "supply_chain_evidence.json", "governance_evidence_bundle.json", "cloud_migration_plan.json"]:
            self.assertIn(expected, makefile)

    def test_accelerator_capacity_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "accelerator-scheduling.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = build_accelerator_capacity_plan(root, project="Metaflow Airflow Training Platform", primary_workload="training")

            self.assertEqual(len(plan["profiles"]), 3)
            self.assertIn("gpu-a100-mig", {profile["kueue_flavor"] for profile in plan["profiles"]})
            self.assertTrue((root / "reports" / "accelerator_capacity_plan.json").exists())
            self.assertIn("ResourceFlavor", manifest)
            self.assertIn("ResourceClaimTemplate", manifest)
            self.assertIn("nvidia.com/mig-1g.10gb", manifest)

    def test_device_allocation_plan_and_dra_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "dynamic-resource-allocation.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "dynamic-resource-allocation.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_device_allocation_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "admit_dra_backed_training_wave")
            self.assertTrue(any(workload["resource_claim_template"] == "l4-shared-training" for workload in report["workloads"]))
            self.assertTrue(any(workload["sharing_strategy"] == "mig" for workload in report["workloads"]))
            self.assertTrue((root / "reports" / "device_allocation_plan.json").exists())
            for expected in ["DeviceClass", "ResourceClaimTemplate", "completionMode: Indexed", "kueue.x-k8s.io/queue-name", "kube_resourceclaim_status_phase"]:
                self.assertIn(expected, manifest)
            for expected in ["Dynamic Resource Allocation", "Airflow", "time-slicing", "MIG", "ResourceClaim"]:
                self.assertIn(expected, docs)

    def test_dra_resource_health_status_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "dra-resource-health-status.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "dra-resource-health-status.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_resource_health_status_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_dra_resource_health_runbook")
            self.assertEqual(report["feature"]["state"], "Kubernetes v1.36 beta and enabled by default")
            self.assertEqual(report["unhealthy_or_unknown_count"], 2)
            self.assertTrue(any(event["workload"] == "failed-partition-replay" for event in report["device_health_events"]))
            self.assertTrue((root / "reports" / "resource_health_status_plan.json").exists())
        for expected in ["DeviceTaintRule", "allocatedResourcesStatus", "ResourceHealthStatus", "resourceclaims", "kube_resourceclaim_status_devices"]:
            self.assertIn(expected, manifest)
        for expected in ["Airflow", "ResourceClaim", "PodResourcesLister", "deterministic CPU path"]:
            self.assertIn(expected, docs)

    def test_advanced_device_sharing_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "dra-advanced-device-sharing.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "dra-advanced-device-sharing.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_advanced_device_sharing_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_dra_advanced_device_sharing_policy")
            self.assertEqual(report["features"]["device_binding_conditions"]["default_wait_seconds"], 600)
            self.assertTrue(any(policy["feature"] == "DRAConsumableCapacity" for policy in report["policies"]))
            self.assertTrue((root / "reports" / "advanced_device_sharing_plan.json").exists())
        for expected in ["prioritizedList", "partitionable", "capacity:", "bindingConditions", "TrainingDRADeviceBindingWaitHigh"]:
            self.assertIn(expected, manifest)
        for expected in ["Training", "prioritized", "Partitionable", "binding conditions"]:
            self.assertIn(expected, docs)

    def test_admin_access_diagnostics_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "dra-admin-access-diagnostics.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "dra-admin-access-diagnostics.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_admin_access_diagnostic_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_dra_admin_access_diagnostics")
            self.assertEqual(report["feature"]["feature_gate"], "DRAAdminAccess")
            self.assertTrue(any("metaflow.run_id" in item["evidence"] for item in report["diagnostics"]))
            self.assertTrue((root / "reports" / "admin_access_diagnostics_plan.json").exists())
        for expected in ["resource.kubernetes.io/admin-access", "adminAccess: true", "mlops-training-dra-admin", "TrainingDRAAdminAccessClaimRunningTooLong"]:
            self.assertIn(expected, manifest)
        for expected in ["Training DRA AdminAccess Diagnostics", "AdminAccess", "Least-privilege RBAC", "Metaflow run id"]:
            self.assertIn(expected, docs)

    def test_inplace_resize_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "inplace-pod-resize.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "inplace-pod-resize.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_inplace_resize_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_inplace_resize_controls")
            self.assertEqual(report["features"]["pod_level_resource_resize"]["feature_gate"], "InPlacePodLevelResourcesVerticalScaling")
            self.assertTrue(any(policy["scope"] == "pod" for policy in report["policies"]))
            self.assertTrue((root / "reports" / "inplace_resize_plan.json").exists())
        for expected in ["--subresource resize", "resizePolicy", "InPlaceOrRecreate", "PodResizePending", "TrainingInPlaceResizeInProgressTooLong"]:
            self.assertIn(expected, manifest)
        for expected in ["Training In-Place Pod Resize Controls", "pods/resize", "PodResizePending", "InPlaceOrRecreate"]:
            self.assertIn(expected, docs)

    def test_topology_placement_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "topology-aware-scheduling.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "topology-aware-scheduling.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_topology_placement_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_topology_aware_backfills")
            self.assertTrue((root / "reports" / "topology_placement_plan.json").exists())
            self.assertTrue(any(workload["pod_count"] == 16 for workload in report["workloads"]))
        for expected in ["kind: Topology", "topologyName", "kueue.x-k8s.io/podset-required-topology", "topologySpreadConstraints", "TrainingTopologyAssignmentDelayed"]:
            self.assertIn(expected, manifest)
        for expected in ["Topology-Aware Scheduling", "Airflow", "topology spread constraints", "AdmissionChecks"]:
            self.assertIn(expected, docs)

    def test_kuberay_capacity_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "kuberay-kueue-workloads.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "kuberay-kueue.md").read_text(encoding="utf-8")
        dag = (repo / "airflow" / "dags" / "enterprise_backfill_training_mesh_dag.py").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_kuberay_capacity_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_kuberay_backfill_waves")
            self.assertTrue((root / "reports" / "kuberay_capacity_plan.json").exists())
            self.assertEqual(report["capacity"]["max_gpu_demand"], 24)
        for expected in ["RayJob", "enableInTreeAutoscaling", "kueue.x-k8s.io/elastic-job", "demand-elastic-backfill", "DemandRayBackfillWorkersPending"]:
            self.assertIn(expected, manifest)
        for expected in ["KubeRay", "Kueue", "Metaflow", "backfill"]:
            self.assertIn(expected, docs)
        for expected in ["submit_kuberay_backfill_wave", "wait_for_kuberay_backfill_wave_deferrable", "rayjob/demand-elastic-backfill"]:
            self.assertIn(expected, dag)

    def test_elastic_workload_plan_and_jobset_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "kueue-elastic-workloads.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "kueue-elastic-workloads.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_elastic_workload_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_kueue_elastic_backfill_slices")
            self.assertEqual(report["feature_gate"], "ElasticJobsViaWorkloadSlices")
            self.assertTrue(any(item["replacement_for"] for item in report["workload_slices"]))
            self.assertTrue((root / "reports" / "elastic_workload_plan.json").exists())
        for expected in ["JobSet", "workload-slice-name", "workload-slice-replacement-for", "TrainingElasticWorkloadSlicePending"]:
            self.assertIn(expected, manifest)
        for expected in ["Elastic Workloads", "Workload Slices", "JobSet", "feature-gate"]:
            self.assertIn(expected, docs)

    def test_indexed_job_resilience_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "indexed-job-resilience.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "indexed-job-resilience.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_indexed_job_resilience_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_indexed_job_resilience_controls")
            self.assertEqual(report["kubernetes_job"]["completion_mode"], "Indexed")
            self.assertEqual(report["retry_policy"]["backoff_limit_per_index"], 1)
            self.assertTrue(any(check["name"] == "pod_failure_policy" for check in report["checks"]))
            self.assertTrue((root / "reports" / "indexed_job_resilience_plan.json").exists())
        for expected in ["completionMode: Indexed", "backoffLimitPerIndex", "maxFailedIndexes", "successPolicy", "podFailurePolicy", "JOB_COMPLETION_INDEX", "TrainingIndexedJobFailedIndexesHigh"]:
            self.assertIn(expected, manifest)
        for expected in ["Indexed Job Resilience", "Airflow Backfill Create", "successPolicy", "podFailurePolicy", "backoffLimitPerIndex"]:
            self.assertIn(expected, docs)

    def test_provisioning_admission_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "provisioning-admission-checks.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "provisioning-admission.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_provisioning_admission_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_kueue_provisioning_admission_for_training")
            self.assertEqual(report["kueue_policy"]["controller_name"], "kueue.x-k8s.io/provisioning-request")
            self.assertTrue(any(check["name"] == "quota_before_capacity" for check in report["checks"]))
            self.assertTrue((root / "reports" / "provisioning_admission_plan.json").exists())
        for expected in ["AdmissionCheck", "ProvisioningRequestConfig", "kueue.x-k8s.io/provisioning-request", "admissionChecksStrategy", "check-capacity.autoscaling.x-k8s.io", "podSetUpdates", "TrainingProvisioningAdmissionPendingTooLong"]:
            self.assertIn(expected, manifest)
        for expected in ["Kueue Provisioning Admission", "ProvisioningRequest", "Cluster Autoscaler", "quota reservation"]:
            self.assertIn(expected, docs)

    def test_multikueue_dispatch_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "multikueue-dispatch.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "multikueue-dispatch.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_multikueue_dispatch_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_multikueue_training_dispatch")
            self.assertEqual(report["dispatch_policy"]["controller_name"], "kueue.x-k8s.io/multikueue")
            self.assertIn("status.clusterName", report["dispatch_policy"]["status_fields"])
            self.assertEqual(report["manager_quota"]["nvidia_com_gpu"], 14)
            self.assertTrue(any(check["name"] == "manager_quota_aligned" for check in report["checks"]))
            self.assertTrue((root / "reports" / "multikueue_dispatch_plan.json").exists())
        for expected in ["MultiKueueConfig", "MultiKueueCluster", "kueue.x-k8s.io/multikueue", "admissionChecksStrategy", "kueue.x-k8s.io/prebuilt-workload-name", "TrainingMultiKueueDispatchStalled"]:
            self.assertIn(expected, manifest)
        for expected in ["MultiKueue Dispatch", "manager cluster", "worker clusters", "status.clusterName"]:
            self.assertIn(expected, docs)

    def test_oci_artifact_volume_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "oci-artifact-volumes.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "oci-artifact-volumes.md").read_text(encoding="utf-8")
        dag = (repo / "airflow" / "dags" / "enterprise_backfill_training_mesh_dag.py").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_oci_artifact_volume_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_kubernetes_image_volume_artifacts")
            self.assertEqual(report["feature"]["feature_state"], "Kubernetes v1.36 stable")
            self.assertTrue(all("@sha256:" in bundle["reference"] for bundle in report["artifact_bundles"]))
            self.assertTrue(all(bundle["read_only"] for bundle in report["artifact_bundles"]))
            self.assertTrue(any(check["name"] == "startup_failure_recovery" for check in report["checks"]))
            self.assertTrue((root / "reports" / "oci_artifact_volume_plan.json").exists())
        for expected in ["image:", "reference: ghcr.io/kevinmeix1/demand-training-dataset@sha256", "pullPolicy: IfNotPresent", "readOnly: true", "training-artifact-volume-smoke", "training-artifact-volume-warmup"]:
            self.assertIn(expected, manifest)
        for expected in ["OCI Artifact Volumes", "Kubernetes v1.36", "PVC/S3 fallback", "read-only"]:
            self.assertIn(expected, docs)
        for expected in ["verify_oci_artifact_volume_mounts", "kubernetes/oci-artifact-volumes.yaml", "IfNotPresent"]:
            self.assertIn(expected, dag)

    def test_dag_bundle_versioning_plan_and_airflow_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        config = (repo / "airflow" / "dag-bundle-config.ini").read_text(encoding="utf-8")
        docs = (repo / "docs" / "airflow-dag-bundles.md").read_text(encoding="utf-8")
        dag = (repo / "airflow" / "dags" / "enterprise_backfill_training_mesh_dag.py").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_dag_bundle_versioning_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_airflow3_training_dag_bundle_versioning")
            self.assertFalse(report["rerun_policy"]["core.rerun_with_latest_version"])
            self.assertTrue(report["backfill_policy"]["scheduler_managed_backfills"])
            self.assertIn("metaflow_run_id", report["training_lineage_evidence"])
            self.assertIn("oci_artifact_volume_digest", report["training_lineage_evidence"])
            self.assertTrue((root / "reports" / "dag_bundle_versioning_plan.json").exists())
        for expected in ["GitDagBundle", "dag_bundle_config_list", "git_conn_id", "disable_bundle_versioning = False", "rerun_with_latest_version = False", "sparse_dirs"]:
            self.assertIn(expected, config)
        for expected in ["Airflow DAG Bundles", "GitDagBundle", "Scheduler-managed backfills", "failed-partition replay"]:
            self.assertIn(expected, docs)
        self.assertIn("rerun_with_latest_version=False", dag)

    def test_asset_partitioning_plan_and_airflow_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "airflow-asset-partitioning.md").read_text(encoding="utf-8")
        dag = (repo / "airflow" / "dags" / "enterprise_backfill_training_mesh_dag.py").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_asset_partitioning_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_airflow_asset_partitioning_for_training_backfills")
            self.assertIn("PartitionedAssetTimetable", report["features"]["timetables"])
            self.assertTrue(any(flow["partition_key"] == "ds:domain:model_family" for flow in report["flows"]))
            self.assertTrue((root / "reports" / "asset_partitioning_plan.json").exists())
        for expected in ["Airflow Asset Partitioning", "PartitionedAssetTimetable", "dag_run.partition_key", "scheduler-managed partition backfills"]:
            self.assertIn(expected, docs)
        for expected in ["CronPartitionTimetable", "PartitionedAssetTimetable", "StartOfHourMapper", "dag_run.partition_key", "partitioned_model_registration_gate"]:
            self.assertIn(expected, dag)

    def test_airflow33_stateful_orchestration_contract(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        dag = (repo / "airflow" / "dags" / "airflow33_stateful_training_dag.py").read_text(encoding="utf-8")
        docs = (repo / "docs" / "airflow-stateful-orchestration.md").read_text(encoding="utf-8")
        ci = (repo / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        validator = (repo / "tools" / "validate_airflow33_dag.py").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            report = build_airflow_stateful_orchestration_plan(tmp, repo_root=repo)

        self.assertTrue(report["passed"])
        self.assertEqual(report["recommended_action"], "adopt_airflow_33_stateful_training_contract")
        self.assertIn("real_airflow_parse_gate", {check["name"] for check in report["checks"] if check["passed"]})
        for expected in ["task_state_store", "asset_state_store", "NEVER_EXPIRE", "ExceptionRetryPolicy", "RollupMapper", "FanOutMapper", "PartitionedAtRuntime"]:
            self.assertIn(expected, dag)
        self.assertIn("dag.validate()", validator)
        self.assertIn("apache-airflow==3.3.0", ci)
        self.assertIn("Production Boundary", docs)

    def test_multi_team_readiness_plan_and_airflow_config_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "airflow-multi-team-readiness.md").read_text(encoding="utf-8")
        config = (repo / "airflow" / "dag-bundle-config.ini").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_multi_team_readiness_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "prepare_airflow_multi_team_training_isolation")
            self.assertEqual(report["team"]["team_name"], "ml-training")
            self.assertEqual(report["configuration"]["AIRFLOW__CORE__MULTI_TEAM"], "True")
            self.assertEqual(report["asset_filtering_contract"]["class"], "AssetAccessControl")
            self.assertTrue((root / "reports" / "multi_team_readiness_plan.json").exists())
        for expected in [
            "Airflow Multi-Team Readiness",
            "team_name",
            "AssetAccessControl",
            "airflow triggerer --team-name",
            "AIRFLOW_VAR__ML_TRAINING",
        ]:
            self.assertIn(expected, docs)
        for expected in ["team_name", "ml-training", "multi_team = True", "LocalExecutor;ml-training=KubernetesExecutor"]:
            self.assertIn(expected, config)

    def test_event_driven_assets_plan_and_docs_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "event-driven-assets.md").read_text(encoding="utf-8")
        dag = (repo / "airflow" / "dags" / "enterprise_backfill_training_mesh_dag.py").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_event_driven_assets_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_airflow3_training_event_assets")
            self.assertEqual(report["asset_expression"], "(RAW_SALES & PARTITION_MANIFESTS) | FAILED_PARTITION_REPLAY")
            self.assertTrue(all(asset["trigger_base_class"] == "BaseEventTrigger" for asset in report["event_assets"]))
            self.assertTrue((root / "reports" / "event_driven_assets_plan.json").exists())
        for expected in ["AssetWatcher", "BaseEventTrigger", "shared_stream_key", "AssetAlias", "conditional asset expression"]:
            self.assertIn(expected, docs)
        for expected in ["EVENT_DRIVEN_ASSET_EXPRESSION", "AssetWatcher", "BaseEventTrigger", "shared_stream_key", "AssetAlias"]:
            self.assertIn(expected, dag)

    def test_pod_resource_envelope_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "pod-resource-envelopes.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "pod-resource-envelopes.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_pod_resource_envelope_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_pod_resource_envelopes_and_scheduling_gates")
            self.assertEqual(report["feature_gates"]["PodSchedulingReadiness"], "stable since Kubernetes 1.30")
            self.assertTrue(all(workload["scheduling_gates"] for workload in report["workloads"]))
            self.assertTrue((root / "reports" / "pod_resource_envelope_plan.json").exists())
        for expected in ["PodLevelResources", "schedulingGates", "scheduler_pending_pods", "PodLevelResourceManagers"]:
            self.assertIn(expected, docs)
        for expected in ["schedulingGates", "resources:", "partition-manifest-builder", "metaflow-training-worker", "failed-partition-replay"]:
            self.assertIn(expected, manifest)

    def test_cohort_fair_sharing_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "kueue-cohort-fair-sharing.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "kueue-cohort-fair-sharing.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_cohort_fair_sharing_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_kueue_cohort_fair_sharing")
            self.assertEqual(report["feature_gates"]["AdmissionFairSharing"], "beta since Kueue v0.15 and enabled by default")
            self.assertGreater(report["cluster_queues"][0]["weight"], report["cluster_queues"][-1]["weight"])
            self.assertTrue((root / "reports" / "cohort_fair_sharing_plan.json").exists())
        for expected in ["Fair Sharing", "Admission Fair Sharing", "borrowingLimit", "lendingLimit", "preemptionStrategies"]:
            self.assertIn(expected, docs)
        for expected in ["AdmissionFairSharing", "LessThanInitialShare", "fairSharing", "borrowingLimit", "lendingLimit", "LocalQueue"]:
            self.assertIn(expected, manifest)

    def test_flavor_fungibility_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "kueue-flavor-fungibility.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "kueue-flavor-fungibility.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_flavor_fungibility_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_kueue_flavor_fungibility")
            self.assertTrue(all(policy["when_can_borrow"] == "TryNextFlavor" for policy in report["flavor_policies"]))
            self.assertTrue(any(policy["name"] == "hpo-sweeps-gpu" for policy in report["flavor_policies"]))
            self.assertTrue((root / "reports" / "flavor_fungibility_plan.json").exists())
        for expected in ["ResourceFlavor", "flavorFungibility", "TryNextFlavor", "BorrowingOverPreemption", "gpu-l4-spot"]:
            self.assertIn(expected, manifest)
        for expected in ["Kueue Flavor Fungibility", "ResourceFlavor", "TryNextFlavor", "BorrowingOverPreemption"]:
            self.assertIn(expected, docs)

    def test_pending_workload_visibility_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "kueue-pending-workload-visibility.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "kueue-pending-workload-visibility.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_pending_workload_visibility_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_kueue_pending_workload_visibility")
            self.assertEqual(report["feature"]["api_group"], "visibility.kueue.x-k8s.io/v1beta2")
            self.assertTrue(any(item["local_queue"] == "daily-backfill" for item in report["pending_workloads"]))
            self.assertTrue((root / "reports" / "pending_workload_visibility_plan.json").exists())
        for expected in ["visibility.kueue.x-k8s.io", "clusterqueues/pendingworkloads", "localqueues/pendingworkloads", "kueue_admission_wait_time_seconds", "kueue_cluster_queue_resource_pending"]:
            self.assertIn(expected, manifest)
        for expected in ["Kueue Pending Workload Visibility", "VisibilityOnDemand", "failed-partition recovery", "pending workload"]:
            self.assertIn(expected, docs)

    def test_workload_aware_scheduling_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "workload-aware-scheduling.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "workload-aware-scheduling.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_workload_aware_scheduling_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "prepare_workload_aware_scheduling_for_training_backfills")
            self.assertEqual(report["api_contract"]["api_group"], "scheduling.k8s.io/v1alpha2")
            self.assertIn("WorkloadWithJob", report["feature_gates"])
            self.assertTrue(any(item["pod_group"] == "metaflow-backfill-pg" for item in report["workloads"]))
            self.assertTrue((root / "reports" / "workload_aware_scheduling_plan.json").exists())
        for expected in ["Kubernetes Workload-Aware Scheduling", "PodGroup", "WorkloadWithJob", "ResourceClaim", "Metaflow"]:
            self.assertIn(expected, docs + manifest)
        for expected in ["scheduling.k8s.io/v1alpha2", "kind: PodGroup", "completionMode: Indexed", "parallelism: 8", "ResourceClaimTemplate", "disruptionMode: PodGroup"]:
            self.assertIn(expected, manifest)

    def test_runtime_security_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "runtime-security.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "runtime-security.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_runtime_security_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_userns_and_kubelet_fine_grained_authz_for_training_workloads")
            self.assertEqual(report["feature_status"]["user_namespaces"], "Kubernetes v1.36 stable and enabled by default; pods opt in with hostUsers=false")
            self.assertIn("nodes/log", report["kubelet_rbac"]["allowed_subresources"])
            self.assertIn("nodes/proxy", report["kubelet_rbac"]["forbidden_for_monitoring"])
            self.assertTrue(all(workload["host_users"] is False for workload in report["workloads"]))
            self.assertTrue((root / "reports" / "runtime_security_plan.json").exists())
        for expected in ["Runtime Security", "hostUsers: false", "KubeletFineGrainedAuthz", "nodes/metrics", "nodes/stats"]:
            self.assertIn(expected, docs + manifest)
        for expected in ["ValidatingAdmissionPolicy", "nodes/log", "allowPrivilegeEscalation: false", "seccompProfile", "RuntimeDefault"]:
            self.assertIn(expected, manifest)

    def test_control_plane_diagnostics_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "control-plane-diagnostics.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "control-plane-diagnostics.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_control_plane_diagnostics_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_control_plane_freshness_diagnostics")
            self.assertTrue(report["feature_status"]["component_statusz"].startswith("Kubernetes v1.36"))
            self.assertTrue(any(component["flagz"] == "/flagz" for component in report["components"]))
            self.assertTrue(any(controller["name"] == "metaflow-backfill-controller" for controller in report["controllers"]))
            self.assertTrue((root / "reports" / "control_plane_diagnostics_plan.json").exists())
        for expected in ["Control Plane Diagnostics", "/statusz", "/flagz", "PSI", "native histogram"]:
            self.assertIn(expected, docs + manifest)
        for expected in ["ServiceMonitor", "PrometheusRule", "TrainingBackfillControllerCacheStale", "KubeletPSIMemoryStallHigh", "NativeHistogramStorageBudgetHigh"]:
            self.assertIn(expected, manifest)

    def test_memory_qos_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "memory-qos.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "memory-qos.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_memory_qos_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_memory_qos_tiered_protection")
            self.assertEqual(report["kubelet_config"]["memoryReservationPolicy"], "TieredReservation")
            self.assertTrue(any(workload["name"] == "airflow-scheduler" and workload["qos_class"] == "Guaranteed" for workload in report["workloads"]))
            self.assertTrue((root / "reports" / "memory_qos_plan.json").exists())
        for expected in ["Memory QoS", "TieredReservation", "memory.min", "memory.low", "memory.high", "cgroup v2"]:
            self.assertIn(expected, docs + manifest)
        for expected in ["KubeletConfiguration", "MemoryQoS", "PrometheusRule", "TrainingMemoryQoSThrottlingHigh", "TrainingMemoryQoSPSIPressureHigh"]:
            self.assertIn(expected, manifest)

    def test_hpa_scale_to_zero_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "hpa-scale-to-zero.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "hpa-scale-to-zero.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_hpa_scale_to_zero_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_hpa_scale_to_zero_for_elastic_training_workers")
            self.assertEqual(report["feature_gate"]["name"], "HPAScaleToZero")
            self.assertTrue(all(workload["min_replicas"] == 0 for workload in report["scale_to_zero_workloads"]))
            self.assertTrue(all(workload["metric_type"] in {"External", "Object"} for workload in report["scale_to_zero_workloads"]))
            self.assertFalse({"airflow-scheduler", "mlflow-registration-gate"} & {workload["name"] for workload in report["scale_to_zero_workloads"]})
            self.assertTrue((root / "reports" / "hpa_scale_to_zero_plan.json").exists())
        for expected in ["HPA Scale To Zero", "HPAScaleToZero", "minReplicas: 0", "External", "cold-start"]:
            self.assertIn(expected, docs + manifest)
        for expected in ["HorizontalPodAutoscaler", "autoscaling/v2", "type: Object", "TrainingScaleToZeroWakeupFailed", "TrainingScaleToZeroColdStartBudgetExceeded"]:
            self.assertIn(expected, manifest)

    def test_suspended_job_resource_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "suspended-job-resources.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "suspended-job-resources.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_suspended_job_resource_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_suspended_job_resource_mutation_for_queued_training_jobs")
            self.assertEqual(report["feature"]["name"], "MutablePodResourcesForSuspendedJobs")
            self.assertEqual(report["feature"]["state"], "Kubernetes v1.36 beta and enabled by default")
            self.assertTrue(all(item["suspended"] for item in report["resource_mutations"]))
            self.assertTrue(all(not item["suspended"] for item in report["protected_jobs"]))
            self.assertTrue((root / "reports" / "suspended_job_resources_plan.json").exists())
        for expected in ["Suspended Job Resource Mutation", "MutablePodResourcesForSuspendedJobs", "spec.suspend", "CPU", "GPU", "Metaflow"]:
            self.assertIn(expected, docs + manifest)
        for expected in ["ValidatingAdmissionPolicy", "suspend: true", "TrainingSuspendedJobResizeStale", "TrainingSuspendedJobUnsuspendWithoutQuotaFit"]:
            self.assertIn(expected, manifest)

    def test_constrained_impersonation_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "constrained-impersonation.md").read_text(encoding="utf-8")
        manifest = (repo / "kubernetes" / "constrained-impersonation.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_constrained_impersonation_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_constrained_impersonation_for_training_operations")
            self.assertEqual(report["feature"]["name"], "ConstrainedImpersonation")
            self.assertTrue(all(item["identity_permission"] == "impersonate:serviceaccount" for item in report["delegations"]))
            self.assertTrue((root / "reports" / "constrained_impersonation_plan.json").exists())
        for expected in ["Constrained Impersonation", "ConstrainedImpersonation", "impersonate:serviceaccount", "impersonate-on:serviceaccount:get", "impersonate-on:serviceaccount:patch", "authentication.k8s.io", "audit"]:
            self.assertIn(expected, docs + manifest)
        for expected in ["RoleBinding", "backfill-controller-impersonate-actions", "partition-status-writer", "ConfigMap", "TrainingLegacyImpersonateVerbDetected", "TrainingConstrainedImpersonationAuditMissing", "authenticationMetadata.impersonationConstraint"]:
            self.assertIn(expected, manifest)

    def test_inference_gateway_plan_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "inference-gateway-routing.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "inference-gateway.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_inference_gateway_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "publish_champion_inference_pool")
            self.assertEqual(report["pool"]["api_version"], "inference.networking.k8s.io/v1")
            self.assertTrue((root / "reports" / "inference_gateway_plan.json").exists())
        for expected in ["InferencePool", "InferenceObjective", "endpointPickerRef", "FailOpen", "HTTPRoute", "DemandEndpointPickerUnavailable"]:
            self.assertIn(expected, manifest)
        for expected in ["Gateway API Inference Extension", "InferencePool", "Endpoint Picker", "training"]:
            self.assertIn(expected, docs)

    def test_semantic_telemetry_plan_and_collector_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        collector = (repo / "kubernetes" / "opentelemetry-collector.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "semantic-telemetry.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_semantic_telemetry_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enforce_training_lineage_telemetry_contract")
            self.assertIn("metaflow.run_id", report["schema"]["required_attributes"])
            self.assertIn("training.row_sample", report["schema"]["redacted_attributes"])
            self.assertTrue((root / "reports" / "semantic_telemetry_plan.json").exists())
        for expected in ["attributes/semantic_redaction", "telemetry.contract.name", "feature.row", "pii.customer_id"]:
            self.assertIn(expected, collector)
        for expected in ["Semantic Telemetry", "Metaflow", "OpenLineage", "row"]:
            self.assertIn(expected, docs)

    def test_airflow_deadline_alert_plan_and_docs_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        docs = (repo / "docs" / "airflow-deadline-alerts.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_deadline_alert_plan(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_airflow3_deadline_alerts")
            self.assertEqual(report["runtime_config"]["AIRFLOW__CALLBACKS__CALLBACK_EXECUTION_TIMEOUT"], "300")
            self.assertTrue(any(policy["name"] == "failed_partition_recovery" for policy in report["deadline_policies"]))
            self.assertTrue((root / "reports" / "deadline_alert_plan.json").exists())
        for expected in ["Deadline Alerts", "legacy Airflow 2 SLA", "Kueue", "failed partition"]:
            self.assertIn(expected, docs)

    def test_cost_observability_report_and_opencost_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "opencost-finops.yaml").read_text(encoding="utf-8")
        docs = (repo / "docs" / "cost-observability.md").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_cost_observability_report(root)

            self.assertTrue(report["passed"])
            self.assertEqual(report["recommended_action"], "enable_training_opencost_guardrails")
            self.assertIn("cost_per_successful_partition", report["unit_economics"]["primary_kpi"])
            self.assertTrue(any(item["training_wave"] == "backfill" for item in report["training_budgets"]))
            self.assertTrue((root / "reports" / "cost_observability_report.json").exists())
        for expected in ["PrometheusRule", "opencost", "TrainingBackfillCostBudgetExceeded", "TrainingRetryStormCost", "label_training_wave"]:
            self.assertIn(expected, manifest)
        for expected in ["OpenCost", "Metaflow", "ResourceQuota", "GPU"]:
            self.assertIn(expected, docs)

    def test_tenancy_fairness_report_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "multitenancy-fairness.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_tenancy_report(root)
            tenant_names = {tenant["name"] for tenant in report["tenants"]}

            self.assertTrue(report["passed"])
            self.assertIn("forecasting-prod", tenant_names)
            self.assertIn("ml-training-cohort", report["fairness"]["cohort"])
            self.assertTrue((root / "reports" / "tenancy_fairness_report.json").exists())
            for expected in ["ResourceQuota", "LimitRange", "RoleBinding", "NetworkPolicy", "Cohort", "ClusterQueue", "airflow-tenant-pools"]:
                self.assertIn(expected, manifest)

    def test_identity_access_report_and_kubernetes_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        manifest = (repo / "kubernetes" / "workload-identity.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = build_identity_access_report(root)
            service_accounts = {identity["service_account"] for identity in report["identities"]}

            self.assertTrue(report["passed"])
            self.assertIn("metaflow-partition-worker", service_accounts)
            self.assertTrue((root / "reports" / "identity_access_report.json").exists())
            for expected in ["ServiceAccount", "automountServiceAccountToken: false", "SecretStore", "ExternalSecret", "refreshInterval: 30m", "eks.amazonaws.com/role-arn", "spiffe.io/spiffe-id", "airflow-workload-identity-policy"]:
                self.assertIn(expected, manifest)

    def test_orchestration_scorecard_covers_advanced_controls(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scorecard = build_orchestration_scorecard(root, repo_root=repo, project="Metaflow Airflow Training Platform")
            names = {check["name"] for check in scorecard["checks"] if check["passed"]}

            self.assertTrue(scorecard["passed"])
            self.assertGreaterEqual(scorecard["score"], 90.0)
            self.assertIn("dynamic_task_mapping", names)
            self.assertIn("kueue_admission", names)
            self.assertIn("semantic_telemetry_contract", names)
            self.assertIn("airflow_deadline_alerts", names)
            self.assertIn("opencost_finops", names)
            self.assertIn("kueue_elastic_workloads", names)
            self.assertIn("indexed_job_resilience", names)
            self.assertIn("provisioning_admission_checks", names)
            self.assertIn("multikueue_dispatch", names)
            self.assertIn("oci_image_volume_artifacts", names)
            self.assertIn("airflow_dag_bundle_versioning", names)
            self.assertIn("airflow_asset_partitioning", names)
            self.assertIn("airflow_stateful_orchestration", names)
            self.assertIn("airflow_multi_team_readiness", names)
            self.assertIn("airflow_event_driven_assets", names)
            self.assertIn("pod_resource_envelopes", names)
            self.assertIn("kueue_cohort_fair_sharing", names)
            self.assertIn("kueue_flavor_fungibility", names)
            self.assertIn("kueue_pending_workload_visibility", names)
            self.assertIn("dra_resource_health_status", names)
            self.assertIn("dra_advanced_device_sharing", names)
            self.assertIn("dra_admin_access_diagnostics", names)
            self.assertIn("kubernetes_inplace_resize", names)
            self.assertIn("kubernetes_workload_aware_scheduling", names)
            self.assertIn("runtime_security_userns_kubelet_authz", names)
            self.assertIn("control_plane_freshness_diagnostics", names)
            self.assertIn("memory_qos_tiered_protection", names)
            self.assertIn("hpa_scale_to_zero_external_metrics", names)
            self.assertIn("suspended_job_resource_mutation", names)
            self.assertIn("constrained_impersonation_least_privilege", names)
            self.assertIn("supply_chain_provenance", names)
            self.assertTrue((root / "reports" / "orchestration_scorecard.json").exists())

    def test_supply_chain_evidence_and_policy_assets_exist(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        policy = (repo / "kubernetes" / "supply-chain-policy.yaml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "reports" / "demo.json", {"status": "ok"})
            evidence = build_supply_chain_evidence(
                root,
                project="Metaflow Airflow Training Platform",
                artifact_name="training-orchestration-demo-artifacts",
                workflow="Training Orchestration CI",
                namespace="mlops-training",
            )

            self.assertEqual(evidence["artifact_count"], 1)
            self.assertEqual(len(evidence["artifacts"][0]["sha256"]), 64)
            self.assertEqual(evidence["subject"]["attestation_action"], "actions/attest@v4")
            self.assertTrue((root / "supply-chain" / "subject.checksums.txt").exists())
            self.assertIn("ClusterImagePolicy", policy)
            self.assertIn("predicateType: https://slsa.dev/provenance/v1", policy)
            self.assertIn("policy.sigstore.dev/include", policy)

    def test_artifact_index_links_key_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = demo(root)
            index = (root / "reports" / "index.html").read_text(encoding="utf-8")

            self.assertTrue(result["artifact_index"].endswith("index.html"))
            for expected in [
                "training_orchestration_dashboard.html",
                "backfill_summary.json",
                "traceability_report.json",
                "governance_evidence_bundle.json",
                "slo_error_budget.json",
                "accelerator_capacity_plan.json",
                "device_allocation_plan.json",
                "resource_health_status_plan.json",
                "advanced_device_sharing_plan.json",
                "admin_access_diagnostics_plan.json",
                "inplace_resize_plan.json",
                "topology_placement_plan.json",
                "kuberay_capacity_plan.json",
                "inference_gateway_plan.json",
                "semantic_telemetry_plan.json",
                "deadline_alert_plan.json",
                "cost_observability_report.json",
                "elastic_workload_plan.json",
                "indexed_job_resilience_plan.json",
                "provisioning_admission_plan.json",
                "multikueue_dispatch_plan.json",
                "oci_artifact_volume_plan.json",
                "dag_bundle_versioning_plan.json",
                "asset_partitioning_plan.json",
                "airflow_stateful_orchestration_plan.json",
                "multi_team_readiness_plan.json",
                "event_driven_assets_plan.json",
                "pod_resource_envelope_plan.json",
                "cohort_fair_sharing_plan.json",
                "flavor_fungibility_plan.json",
                "pending_workload_visibility_plan.json",
                "tenancy_fairness_report.json",
                "identity_access_report.json",
                "performance_budget.json",
                "queue_simulation.json",
                "workload_aware_scheduling_plan.json",
                "runtime_security_plan.json",
                "control_plane_diagnostics_plan.json",
                "memory_qos_plan.json",
                "hpa_scale_to_zero_plan.json",
                "suspended_job_resources_plan.json",
                "constrained_impersonation_plan.json",
                "release_admission_decision.json",
                "resource_optimization.json",
                "network_security.json",
                "chaos_drill_report.json",
                "gitops_plan.json",
                "orchestration_scorecard.json",
                "supply_chain_evidence.json",
                "cloud_migration_plan.json",
            ]:
                self.assertIn(expected, index)

    def test_backfill_capacity_planner_packs_waves_within_limits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = build_backfill_plan(root, "2026-06-01", "2026-06-03")
            oversized = pack_waves(
                [
                    {"partition": "2026-06-01", "model_family": "inventory_capped", "cpu": 2.0, "memory_gib": 3.0, "priority": "backfill-critical"},
                    {"partition": "2026-06-01", "model_family": "promo_lift", "cpu": 1.5, "memory_gib": 2.0, "priority": "backfill-critical"},
                    {"partition": "2026-06-02", "model_family": "inventory_capped", "cpu": 2.0, "memory_gib": 3.0, "priority": "backfill-critical"},
                ],
                max_cpu=3.0,
                max_memory_gib=4.0,
                max_parallelism=2,
            )

            self.assertEqual(plan["workload_count"], 9)
            self.assertGreaterEqual(plan["wave_count"], 3)
            self.assertTrue(all(wave["cpu"] <= 6.0 and wave["memory_gib"] <= 10.0 for wave in plan["waves"]))
            self.assertEqual(len(oversized), 3)

    def test_demo_runs_backfill_failure_recovery_and_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = demo(root)

            self.assertEqual(result["initial_backfill"]["success_count"], 5)
            self.assertEqual(result["idempotent_backfill"]["skipped_count"], 3)
            self.assertEqual(result["failure_drill"]["failed_count"], 1)
            self.assertEqual(result["recovery"]["status"], "success")
            self.assertTrue((root / "reports" / "training_orchestration_dashboard.html").exists())
            self.assertTrue((root / "reports" / "index.html").exists())
            self.assertTrue((root / "reports" / "accelerator_capacity_plan.json").exists())
            self.assertTrue((root / "reports" / "device_allocation_plan.json").exists())
            self.assertTrue((root / "reports" / "resource_health_status_plan.json").exists())
            self.assertTrue((root / "reports" / "advanced_device_sharing_plan.json").exists())
            self.assertTrue((root / "reports" / "admin_access_diagnostics_plan.json").exists())
            self.assertTrue((root / "reports" / "inplace_resize_plan.json").exists())
            self.assertTrue((root / "reports" / "topology_placement_plan.json").exists())
            self.assertTrue((root / "reports" / "kuberay_capacity_plan.json").exists())
            self.assertTrue((root / "reports" / "inference_gateway_plan.json").exists())
            self.assertTrue((root / "reports" / "semantic_telemetry_plan.json").exists())
            self.assertTrue((root / "reports" / "deadline_alert_plan.json").exists())
            self.assertTrue((root / "reports" / "cost_observability_report.json").exists())
            self.assertTrue((root / "reports" / "elastic_workload_plan.json").exists())
            self.assertTrue((root / "reports" / "indexed_job_resilience_plan.json").exists())
            self.assertTrue((root / "reports" / "multikueue_dispatch_plan.json").exists())
            self.assertTrue((root / "reports" / "oci_artifact_volume_plan.json").exists())
            self.assertTrue((root / "reports" / "dag_bundle_versioning_plan.json").exists())
            self.assertTrue((root / "reports" / "asset_partitioning_plan.json").exists())
            self.assertTrue((root / "reports" / "multi_team_readiness_plan.json").exists())
            self.assertTrue((root / "reports" / "event_driven_assets_plan.json").exists())
            self.assertTrue((root / "reports" / "pod_resource_envelope_plan.json").exists())
            self.assertTrue((root / "reports" / "cohort_fair_sharing_plan.json").exists())
            self.assertTrue((root / "reports" / "flavor_fungibility_plan.json").exists())
            self.assertTrue((root / "reports" / "pending_workload_visibility_plan.json").exists())
            self.assertTrue((root / "reports" / "tenancy_fairness_report.json").exists())
            self.assertTrue((root / "reports" / "identity_access_report.json").exists())
            self.assertTrue((root / "reports" / "performance_budget.json").exists())
            self.assertTrue((root / "reports" / "queue_simulation.json").exists())
            self.assertTrue((root / "reports" / "workload_aware_scheduling_plan.json").exists())
            self.assertTrue((root / "reports" / "runtime_security_plan.json").exists())
            self.assertTrue((root / "reports" / "control_plane_diagnostics_plan.json").exists())
            self.assertTrue((root / "reports" / "memory_qos_plan.json").exists())
            self.assertTrue((root / "reports" / "hpa_scale_to_zero_plan.json").exists())
            self.assertTrue((root / "reports" / "suspended_job_resources_plan.json").exists())
            self.assertTrue((root / "reports" / "constrained_impersonation_plan.json").exists())
            self.assertTrue((root / "reports" / "release_admission_decision.json").exists())
            self.assertTrue((root / "reports" / "orchestration_scorecard.json").exists())
            self.assertTrue((root / "reports" / "supply_chain_evidence.json").exists())

    def test_backfill_is_idempotent_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = backfill(root, "2026-06-01", "2026-06-02")
            second = backfill(root, "2026-06-01", "2026-06-02")

            self.assertEqual(first["success_count"], 2)
            self.assertEqual(second["skipped_count"], 2)

    def test_failed_partition_can_be_recovered_with_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            failed = run_partition(root, "2026-06-03", fail_task="metaflow_train")
            recovered = run_partition(root, "2026-06-03", force=True)
            runs = read_jsonl(run_log_path(root))

            self.assertEqual(failed["status"], "failed")
            self.assertEqual(recovered["status"], "success")
            self.assertTrue(any(row["status"] == "failed" for row in runs if row["task"] == "pipeline"))
            self.assertTrue(any(row["status"] == "success" for row in runs if row["task"] == "pipeline"))

    def test_data_validation_and_gate_failure_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = generate_partition(Path(tmp) / "sales.csv", "2026-06-04")
            validation = validate_rows(read_csv(dataset))
            gate_report = evaluate_gates({"rmse": 99.0, "mape": 1.0, "max_sku_mae": 80.0}, validation)

            self.assertTrue(validation["passed"])
            self.assertFalse(gate_report["passed"])
            self.assertEqual(
                {check["name"] for check in gate_report["checks"] if not check["passed"]},
                {"rmse", "mape", "max_sku_mae"},
            )

    def test_lineage_and_asset_catalog_are_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_partition(root, "2026-06-05")

            assets = read_json(root / "orchestration" / "asset_catalog.json")
            lineage = read_json(root / "orchestration" / "lineage.json")

            self.assertIn("daily_demand_model", assets)
            self.assertIn("metaflow_training_flow", lineage)
            self.assertIn("daily_demand_model", lineage["metaflow_training_flow"])

    def test_partition_manifest_contains_content_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_partition(root, "2026-06-08")
            manifest = read_json(root / "data" / "manifests" / "ds=2026-06-08.json")

            self.assertEqual(manifest["partition"], "2026-06-08")
            self.assertEqual(len(manifest["content_sha256"]), 64)
            self.assertEqual(manifest["idempotency_key"], "daily-demand:2026-06-08")


if __name__ == "__main__":
    unittest.main()
