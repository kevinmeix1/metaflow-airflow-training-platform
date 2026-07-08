from __future__ import annotations

from datetime import datetime, timedelta

AIRFLOW_AVAILABLE = True

try:
    from airflow.decorators import dag, task, task_group
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import BranchPythonOperator
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from airflow.sdk import Asset
    from airflow.utils.trigger_rule import TriggerRule
except Exception:
    AIRFLOW_AVAILABLE = False


DOMAINS = ["north", "south", "enterprise", "digital"]
MODEL_FAMILIES = ["baseline", "promo_lift", "inventory_capped"]
VALIDATION_SUITES = ["contract", "freshness", "volume", "distribution"]
TRAINING_IMAGE = "ghcr.io/kevinmeix1/metaflow-airflow-training-platform:2026.07.0"


def pod(task_id: str, command: str, *, pool: str = "training_pool", priority_weight: int = 1):
    return KubernetesPodOperator(
        task_id=task_id,
        namespace="ml-training",
        image=TRAINING_IMAGE,
        image_pull_policy="IfNotPresent",
        cmds=["bash", "-lc"],
        arguments=[command],
        service_account_name="demand-training-runner",
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        deferrable=True,
        logging_interval=60,
        reattach_on_restart=True,
        on_finish_action="delete_pod",
        on_kill_action="delete_pod",
        startup_timeout_seconds=300,
        execution_timeout=timedelta(hours=4),
        pod_template_file="/opt/airflow/dags/repo/kubernetes/airflow-kubernetes-executor-pod-template.yaml",
        pool=pool,
        priority_weight=priority_weight,
        retries=2,
        retry_delay=timedelta(minutes=5),
        labels={"platform": "metaflow-airflow-training", "task": task_id, "artifact-locality": "oci-image-volume"},
    )


if AIRFLOW_AVAILABLE:
    RAW_SALES = Asset("lakehouse://retail/raw_sales")
    PARTITION_MANIFESTS = Asset("lakehouse://retail/manifests/daily_sales")
    MODEL_REGISTRY = Asset("mlflow://models/daily-demand-forecaster")
    LINEAGE = Asset("openlineage://retail/daily-demand")

    @dag(
        dag_id="enterprise_backfill_training_mesh",
        start_date=datetime(2026, 1, 1),
        schedule=[RAW_SALES],
        catchup=True,
        max_active_runs=2,
        default_args={
            "owner": "ml-platform",
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["airflow", "metaflow", "dynamic-mapping", "backfill", "lineage"],
    )
    def enterprise_backfill_training_mesh():
        start = EmptyOperator(task_id="start_training_mesh")

        @task(outlets=[PARTITION_MANIFESTS])
        def build_partition_manifest(data_interval_start=None, data_interval_end=None) -> dict:
            ds = data_interval_start.strftime("%Y-%m-%d") if data_interval_start else "manual"
            return {"ds": ds, "idempotency_key": f"daily-demand:{ds}", "domains": DOMAINS, "families": MODEL_FAMILIES}

        @task
        def expand_domain_family_grid(manifest: dict) -> list[dict]:
            return [
                {"ds": manifest["ds"], "domain": domain, "model_family": family}
                for domain in manifest["domains"]
                for family in manifest["families"]
            ]

        @task_group(group_id="quality_mesh")
        def quality_mesh_group():
            contract = pod("contract_validation", "make backfill", priority_weight=5)
            freshness = pod("freshness_validation", "make backfill", priority_weight=4)
            volume = pod("volume_anomaly_check", "make backfill", priority_weight=3)
            distribution = pod("distribution_shift_check", "make backfill", priority_weight=3)
            contract >> [freshness, volume, distribution]
            return distribution

        @task_group(group_id="capacity_admission")
        def capacity_admission_group():
            reserve_backfill_quota = pod(
                "reserve_kueue_backfill_quota",
                "kubectl get localqueue demand-training-queue -n ml-training",
                priority_weight=5,
            )
            inspect_cluster_queue = pod(
                "inspect_cluster_queue_headroom",
                "kubectl get clusterqueue demand-training-cluster-queue -o yaml",
                priority_weight=4,
            )
            verify_artifact_volumes = pod(
                "verify_oci_artifact_volume_mounts",
                "kubectl apply -f kubernetes/oci-artifact-volumes.yaml && kubectl wait --for=condition=Complete job/training-artifact-volume-smoke -n ml-training --timeout=10m",
                priority_weight=6,
            )
            submit_ray_backfill_wave = pod(
                "submit_kuberay_backfill_wave",
                "kubectl apply -f kubernetes/kuberay-kueue-workloads.yaml",
                priority_weight=5,
            )
            wait_for_ray_backfill_wave = pod(
                "wait_for_kuberay_backfill_wave_deferrable",
                "kubectl wait --for=condition=Complete rayjob/demand-elastic-backfill -n mlops-training --timeout=45m",
                priority_weight=5,
            )
            wait_for_partition_workers = pod(
                "wait_for_partition_workers_deferrable",
                "kubectl wait --for=condition=Complete job/demand-training-admission-smoke -n ml-training --timeout=15m",
                priority_weight=4,
            )
            reserve_backfill_quota >> inspect_cluster_queue >> verify_artifact_volumes >> submit_ray_backfill_wave >> wait_for_ray_backfill_wave >> wait_for_partition_workers
            return wait_for_partition_workers

        @task_group(group_id="metaflow_training_grid")
        def training_grid_group(grid: list[dict]):
            @task(pool="metaflow_training_pool", retries=2)
            def launch_metaflow_child_flow(spec: dict) -> dict:
                return {
                    **spec,
                    "run_command": f"python metaflow_flows/demand_training_flow.py run --ds {spec['ds']}",
                    "artifact": f"mlflow://runs/{spec['domain']}/{spec['model_family']}",
                }

            @task(pool="metaflow_training_pool")
            def evaluate_child_flow(result: dict) -> dict:
                return {**result, "rmse": 4.2, "mape": 0.14, "passed": True}

            runs = launch_metaflow_child_flow.expand(spec=grid)
            return evaluate_child_flow.expand(result=runs)

        @task
        def choose_champion(candidate_reports: list[dict]) -> str:
            return "register_champion"

        select_champion = BranchPythonOperator(
            task_id="branch_on_champion_selection",
            python_callable=lambda: "register_champion",
        )
        register = pod("register_champion", "make run", priority_weight=10)
        quarantine = pod("quarantine_failed_partition", "make dashboard", priority_weight=8)
        quarantine.trigger_rule = TriggerRule.ONE_FAILED
        publish_dashboard = pod("publish_training_dashboard", "make dashboard", priority_weight=2)
        publish_lineage = EmptyOperator(task_id="publish_openlineage", outlets=[MODEL_REGISTRY, LINEAGE], trigger_rule=TriggerRule.ALL_DONE)
        end = EmptyOperator(task_id="training_mesh_complete")

        manifest = build_partition_manifest()
        grid = expand_domain_family_grid(manifest)
        start >> manifest >> capacity_admission_group() >> quality_mesh_group() >> training_grid_group(grid) >> choose_champion(grid) >> select_champion
        select_champion >> register >> publish_dashboard >> publish_lineage >> end
        select_champion >> quarantine >> publish_lineage

    enterprise_backfill_training_mesh()
