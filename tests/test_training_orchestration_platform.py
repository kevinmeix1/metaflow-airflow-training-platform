from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from training_orchestration_platform.capacity_planner import build_backfill_plan, pack_waves
from training_orchestration_platform.cli import demo
from training_orchestration_platform.data import generate_partition, validate_rows
from training_orchestration_platform.io import read_csv, read_json, read_jsonl
from training_orchestration_platform.model import evaluate_gates
from training_orchestration_platform.orchestrator import backfill, run_log_path, run_partition


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
