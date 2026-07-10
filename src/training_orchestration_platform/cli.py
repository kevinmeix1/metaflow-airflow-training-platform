from __future__ import annotations

import argparse
import json
from pathlib import Path

from .accelerator_plan import build_accelerator_capacity_plan
from .admin_access_diagnostics import build_admin_access_diagnostic_plan
from .advanced_device_sharing import build_advanced_device_sharing_plan
from .airflow_stateful_orchestration import build_airflow_stateful_orchestration_plan
from .artifact_index import render_artifact_index
from .asset_partitioning import build_asset_partitioning_plan
from .capacity_planner import build_backfill_plan
from .checkpoint_training_readiness import build_checkpoint_training_readiness_plan
from .chaos import run_chaos_drill
from .cloud_migration import build_cloud_migration_plan
from .cohort_fair_sharing import build_cohort_fair_sharing_plan
from .control_plane_diagnostics import build_control_plane_diagnostics_plan
from .constrained_impersonation import build_constrained_impersonation_plan
from .cost_observability import build_cost_observability_report
from .dag_bundle_versioning import build_dag_bundle_versioning_plan
from .dashboard import render_dashboard
from .deadline_alerts import build_deadline_alert_plan
from .device_allocation import build_device_allocation_plan
from .disaster_recovery import build_disaster_recovery_plan
from .elastic_workload import build_elastic_workload_plan
from .event_driven_assets import build_event_driven_assets_plan
from .flavor_fungibility import build_flavor_fungibility_plan
from .gitops_release import build_gitops_plan
from .governance import build_governance_bundle
from .hpa_scale_to_zero import build_hpa_scale_to_zero_plan
from .identity import build_identity_access_report
from .indexed_job_resilience import build_indexed_job_resilience_plan
from .inplace_resize import build_inplace_resize_plan
from .inference_gateway import build_inference_gateway_plan
from .kuberay_capacity import build_kuberay_capacity_plan
from .memory_qos import build_memory_qos_plan
from .multi_team_readiness import build_multi_team_readiness_plan
from .multikueue_dispatch import build_multikueue_dispatch_plan
from .network_security import build_network_security_report
from .io import read_jsonl
from .orchestrator import backfill, run_log_path, run_partition
from .orchestration_scorecard import build_orchestration_scorecard
from .oci_artifact_volume import build_oci_artifact_volume_plan
from .pending_workload_visibility import build_pending_workload_visibility_plan
from .policy_audit import audit_platform_policy
from .performance_budget import build_performance_budget_report
from .pod_resource_envelopes import build_pod_resource_envelope_plan
from .provisioning_admission import build_provisioning_admission_plan
from .queue_simulator import build_queue_simulation
from .release_admission import build_release_admission_decision
from .resource_health_status import build_resource_health_status_plan
from .resource_optimizer import build_resource_optimization_report
from .runtime_security import build_runtime_security_plan
from .semantic_telemetry import build_semantic_telemetry_plan
from .slo import build_slo_report
from .supply_chain import build_supply_chain_evidence
from .suspended_job_resources import build_suspended_job_resource_plan
from .tenancy import build_tenancy_report
from .topology_placement import build_topology_placement_plan
from .traceability import build_trace_report
from .workload_aware_scheduling import build_workload_aware_scheduling_plan


def demo(output: str | Path) -> dict:
    root = Path(output)
    first = backfill(root, "2026-06-01", "2026-06-05")
    skipped = backfill(root, "2026-06-03", "2026-06-05")
    failure = backfill(root, "2026-06-06", "2026-06-06", fail_date="2026-06-06")
    recovery = run_partition(root, "2026-06-06", force=True)
    capacity_plan = build_backfill_plan(root, "2026-06-01", "2026-06-07")
    policy_audit = audit_platform_policy(Path.cwd(), output_root=root)
    trace_report = build_trace_report(root)
    chaos_drill = run_chaos_drill(root)
    resource_optimization = build_resource_optimization_report(root)
    network_security = build_network_security_report(root)
    gitops_plan = build_gitops_plan(root)
    disaster_recovery = build_disaster_recovery_plan(root)
    governance_bundle = build_governance_bundle(root)
    slo_error_budget = build_slo_report(root)
    cloud_migration = build_cloud_migration_plan(root)
    accelerator_capacity = build_accelerator_capacity_plan(
        root,
        project="Metaflow Airflow Training Platform",
        primary_workload="partitioned training backfills and feature-heavy model families",
    )
    device_allocation = build_device_allocation_plan(root)
    resource_health_status = build_resource_health_status_plan(root)
    advanced_device_sharing = build_advanced_device_sharing_plan(root)
    admin_access_diagnostics = build_admin_access_diagnostic_plan(root)
    inplace_resize = build_inplace_resize_plan(root)
    topology_placement = build_topology_placement_plan(root)
    kuberay_capacity = build_kuberay_capacity_plan(root)
    inference_gateway = build_inference_gateway_plan(root)
    semantic_telemetry = build_semantic_telemetry_plan(root)
    deadline_alerts = build_deadline_alert_plan(root)
    cost_observability = build_cost_observability_report(root)
    elastic_workload = build_elastic_workload_plan(root)
    indexed_job_resilience = build_indexed_job_resilience_plan(root)
    provisioning_admission = build_provisioning_admission_plan(root)
    multikueue_dispatch = build_multikueue_dispatch_plan(root)
    dag_bundle_versioning = build_dag_bundle_versioning_plan(root)
    asset_partitioning = build_asset_partitioning_plan(root)
    airflow_stateful_orchestration = build_airflow_stateful_orchestration_plan(root)
    multi_team_readiness = build_multi_team_readiness_plan(root)
    event_driven_assets = build_event_driven_assets_plan(root)
    pod_resource_envelopes = build_pod_resource_envelope_plan(root)
    cohort_fair_sharing = build_cohort_fair_sharing_plan(root)
    flavor_fungibility = build_flavor_fungibility_plan(root)
    pending_workload_visibility = build_pending_workload_visibility_plan(root)
    tenancy = build_tenancy_report(root)
    identity_access = build_identity_access_report(root)
    performance_budget = build_performance_budget_report(root)
    queue_simulation = build_queue_simulation(root)
    workload_aware_scheduling = build_workload_aware_scheduling_plan(root)
    runtime_security = build_runtime_security_plan(root)
    control_plane_diagnostics = build_control_plane_diagnostics_plan(root)
    memory_qos = build_memory_qos_plan(root)
    hpa_scale_to_zero = build_hpa_scale_to_zero_plan(root)
    suspended_job_resources = build_suspended_job_resource_plan(root)
    constrained_impersonation = build_constrained_impersonation_plan(root)
    oci_artifact_volume = build_oci_artifact_volume_plan(root)
    checkpoint_training = build_checkpoint_training_readiness_plan(root)
    dashboard = render_dashboard(root, root / "reports" / "training_orchestration_dashboard.html")
    supply_chain = build_supply_chain_evidence(
        root,
        project="Metaflow Airflow Training Platform",
        artifact_name="training-orchestration-demo-artifacts",
        workflow="Training Orchestration CI",
        namespace="mlops-training",
    )
    release_admission = build_release_admission_decision(root)
    artifact_index = render_artifact_index(
        root,
        title="Metaflow Airflow Training Platform",
        description="Reviewer landing page for generated training dashboard, lineage, backfill evidence, SLOs, and migration artifacts.",
        dashboard="training_orchestration_dashboard.html",
    )
    orchestration_scorecard = build_orchestration_scorecard(root, project="Metaflow Airflow Training Platform")
    return {
        "initial_backfill": first,
        "idempotent_backfill": skipped,
        "failure_drill": failure,
        "recovery": recovery,
        "capacity_plan": capacity_plan,
        "policy_audit": policy_audit,
        "trace_report": trace_report,
        "chaos_drill": chaos_drill,
        "resource_optimization": resource_optimization,
        "network_security": network_security,
        "gitops_plan": gitops_plan,
        "disaster_recovery": disaster_recovery,
        "governance_bundle": governance_bundle,
        "slo_error_budget": slo_error_budget,
        "cloud_migration": cloud_migration,
        "accelerator_capacity": accelerator_capacity,
        "device_allocation": device_allocation,
        "resource_health_status": resource_health_status,
        "advanced_device_sharing": advanced_device_sharing,
        "admin_access_diagnostics": admin_access_diagnostics,
        "inplace_resize": inplace_resize,
        "topology_placement": topology_placement,
        "kuberay_capacity": kuberay_capacity,
        "inference_gateway": inference_gateway,
        "semantic_telemetry": semantic_telemetry,
        "deadline_alerts": deadline_alerts,
        "cost_observability": cost_observability,
        "elastic_workload": elastic_workload,
        "indexed_job_resilience": indexed_job_resilience,
        "provisioning_admission": provisioning_admission,
        "multikueue_dispatch": multikueue_dispatch,
        "dag_bundle_versioning": dag_bundle_versioning,
        "asset_partitioning": asset_partitioning,
        "airflow_stateful_orchestration": airflow_stateful_orchestration,
        "multi_team_readiness": multi_team_readiness,
        "event_driven_assets": event_driven_assets,
        "pod_resource_envelopes": pod_resource_envelopes,
        "cohort_fair_sharing": cohort_fair_sharing,
        "flavor_fungibility": flavor_fungibility,
        "pending_workload_visibility": pending_workload_visibility,
        "tenancy": tenancy,
        "identity_access": identity_access,
        "performance_budget": performance_budget,
        "queue_simulation": queue_simulation,
        "workload_aware_scheduling": workload_aware_scheduling,
        "runtime_security": runtime_security,
        "control_plane_diagnostics": control_plane_diagnostics,
        "memory_qos": memory_qos,
        "hpa_scale_to_zero": hpa_scale_to_zero,
        "suspended_job_resources": suspended_job_resources,
        "constrained_impersonation": constrained_impersonation,
        "oci_artifact_volume": oci_artifact_volume,
        "checkpoint_training": checkpoint_training,
        "release_admission": release_admission,
        "dashboard": str(dashboard),
        "artifact_index": str(artifact_index),
        "orchestration_scorecard": orchestration_scorecard,
        "supply_chain": supply_chain,
    }


def demo_summary(output: str | Path, result: dict) -> dict:
    root = Path(output)
    pipeline_events = [
        item
        for item in read_jsonl(run_log_path(root))
        if item.get("task") == "pipeline"
    ]
    return {
        "status": "completed",
        "pipeline": {
            "successful_runs": sum(
                item.get("status") == "success" for item in pipeline_events
            ),
            "failed_runs": sum(
                item.get("status") == "failed" for item in pipeline_events
            ),
            "recovered_partition": result["recovery"]["ds"],
        },
        "control_plane": {
            "orchestration_score": result["orchestration_scorecard"]["score"],
            "release_action": result["release_admission"]["decision"][
                "recommended_action"
            ],
            "release_freeze": result["slo_error_budget"]["release_freeze"],
        },
        "artifacts": {
            "report_count": len(list((root / "reports").glob("*"))),
            "dashboard": result["dashboard"],
            "index": result["artifact_index"],
        },
        "next_command": "make metaflow-runtime-contract",
    }


def governance(output: str | Path) -> dict:
    root = Path(output)
    if not (root / "orchestration" / "asset_catalog.json").exists():
        backfill(root, "2026-06-01", "2026-06-03")
    return build_governance_bundle(root)


def slo_report(output: str | Path) -> dict:
    root = Path(output)
    if not (root / "orchestration" / "run_history.jsonl").exists():
        backfill(root, "2026-06-01", "2026-06-03")
    return build_slo_report(root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Metaflow and Airflow training orchestration platform")
    sub = parser.add_subparsers(dest="command", required=True)
    demo_parser = sub.add_parser("demo")
    demo_parser.add_argument("--output", default=".local")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--output", default=".local")
    run_parser.add_argument("--date", required=True)
    run_parser.add_argument("--force", action="store_true")
    backfill_parser = sub.add_parser("backfill")
    backfill_parser.add_argument("--output", default=".local")
    backfill_parser.add_argument("--start", required=True)
    backfill_parser.add_argument("--end", required=True)
    backfill_parser.add_argument("--force", action="store_true")
    plan_parser = sub.add_parser("plan-backfill")
    plan_parser.add_argument("--output", default=".local")
    plan_parser.add_argument("--start", required=True)
    plan_parser.add_argument("--end", required=True)
    plan_parser.add_argument("--force", action="store_true")
    dashboard_parser = sub.add_parser("dashboard")
    dashboard_parser.add_argument("--output", default=".local")
    audit_parser = sub.add_parser("policy-audit")
    audit_parser.add_argument("--output", default=".local")
    trace_parser = sub.add_parser("trace-report")
    trace_parser.add_argument("--output", default=".local")
    chaos_parser = sub.add_parser("chaos-drill")
    chaos_parser.add_argument("--output", default=".local")
    optimize_parser = sub.add_parser("optimize-resources")
    optimize_parser.add_argument("--output", default=".local")
    network_parser = sub.add_parser("network-security")
    network_parser.add_argument("--output", default=".local")
    gitops_parser = sub.add_parser("gitops-plan")
    gitops_parser.add_argument("--output", default=".local")
    dr_parser = sub.add_parser("dr-plan")
    dr_parser.add_argument("--output", default=".local")
    governance_parser = sub.add_parser("governance-bundle")
    governance_parser.add_argument("--output", default=".local")
    slo_parser = sub.add_parser("slo-report")
    slo_parser.add_argument("--output", default=".local")
    cloud_parser = sub.add_parser("cloud-plan")
    cloud_parser.add_argument("--output", default=".local")
    supply_chain_parser = sub.add_parser("supply-chain")
    supply_chain_parser.add_argument("--output", default=".local")
    scorecard_parser = sub.add_parser("orchestration-scorecard")
    scorecard_parser.add_argument("--output", default=".local")
    accelerator_parser = sub.add_parser("accelerator-plan")
    accelerator_parser.add_argument("--output", default=".local")
    device_parser = sub.add_parser("device-plan")
    device_parser.add_argument("--output", default=".local")
    resource_health_parser = sub.add_parser("resource-health-status")
    resource_health_parser.add_argument("--output", default=".local")
    advanced_sharing_parser = sub.add_parser("advanced-device-sharing")
    advanced_sharing_parser.add_argument("--output", default=".local")
    admin_access_parser = sub.add_parser("admin-access-diagnostics")
    admin_access_parser.add_argument("--output", default=".local")
    inplace_resize_parser = sub.add_parser("inplace-resize-plan")
    inplace_resize_parser.add_argument("--output", default=".local")
    topology_parser = sub.add_parser("topology-plan")
    topology_parser.add_argument("--output", default=".local")
    kuberay_parser = sub.add_parser("kuberay-plan")
    kuberay_parser.add_argument("--output", default=".local")
    inference_gateway_parser = sub.add_parser("inference-gateway-plan")
    inference_gateway_parser.add_argument("--output", default=".local")
    semantic_parser = sub.add_parser("semantic-telemetry-plan")
    semantic_parser.add_argument("--output", default=".local")
    deadline_parser = sub.add_parser("deadline-alerts-plan")
    deadline_parser.add_argument("--output", default=".local")
    cost_parser = sub.add_parser("cost-observability")
    cost_parser.add_argument("--output", default=".local")
    elastic_parser = sub.add_parser("elastic-workload-plan")
    elastic_parser.add_argument("--output", default=".local")
    indexed_job_parser = sub.add_parser("indexed-job-resilience")
    indexed_job_parser.add_argument("--output", default=".local")
    provisioning_parser = sub.add_parser("provisioning-admission")
    provisioning_parser.add_argument("--output", default=".local")
    multikueue_parser = sub.add_parser("multikueue-dispatch")
    multikueue_parser.add_argument("--output", default=".local")
    dag_bundle_parser = sub.add_parser("dag-bundle-plan")
    dag_bundle_parser.add_argument("--output", default=".local")
    asset_partitioning_parser = sub.add_parser("asset-partitioning-plan")
    asset_partitioning_parser.add_argument("--output", default=".local")
    airflow_stateful_parser = sub.add_parser("airflow-stateful-orchestration")
    airflow_stateful_parser.add_argument("--output", default=".local")
    multi_team_parser = sub.add_parser("multi-team-readiness")
    multi_team_parser.add_argument("--output", default=".local")
    event_assets_parser = sub.add_parser("event-driven-assets")
    event_assets_parser.add_argument("--output", default=".local")
    pod_resource_parser = sub.add_parser("pod-resource-envelopes")
    pod_resource_parser.add_argument("--output", default=".local")
    cohort_parser = sub.add_parser("cohort-fair-sharing")
    cohort_parser.add_argument("--output", default=".local")
    flavor_parser = sub.add_parser("flavor-fungibility")
    flavor_parser.add_argument("--output", default=".local")
    pending_visibility_parser = sub.add_parser("pending-workload-visibility")
    pending_visibility_parser.add_argument("--output", default=".local")
    tenancy_parser = sub.add_parser("tenancy-report")
    tenancy_parser.add_argument("--output", default=".local")
    identity_parser = sub.add_parser("identity-report")
    identity_parser.add_argument("--output", default=".local")
    performance_parser = sub.add_parser("performance-budget")
    performance_parser.add_argument("--output", default=".local")
    queue_parser = sub.add_parser("queue-simulation")
    queue_parser.add_argument("--output", default=".local")
    workload_parser = sub.add_parser("workload-aware-scheduling")
    workload_parser.add_argument("--output", default=".local")
    runtime_security_parser = sub.add_parser("runtime-security")
    runtime_security_parser.add_argument("--output", default=".local")
    control_plane_parser = sub.add_parser("control-plane-diagnostics")
    control_plane_parser.add_argument("--output", default=".local")
    memory_qos_parser = sub.add_parser("memory-qos")
    memory_qos_parser.add_argument("--output", default=".local")
    hpa_parser = sub.add_parser("hpa-scale-zero")
    hpa_parser.add_argument("--output", default=".local")
    suspended_job_parser = sub.add_parser("suspended-job-resources")
    suspended_job_parser.add_argument("--output", default=".local")
    constrained_impersonation_parser = sub.add_parser("constrained-impersonation")
    constrained_impersonation_parser.add_argument("--output", default=".local")
    artifact_volume_parser = sub.add_parser("oci-artifact-volumes")
    artifact_volume_parser.add_argument("--output", default=".local")
    checkpoint_training_parser = sub.add_parser("checkpoint-training-readiness")
    checkpoint_training_parser.add_argument("--output", default=".local")
    admission_parser = sub.add_parser("release-admission")
    admission_parser.add_argument("--output", default=".local")
    args = parser.parse_args(argv)
    if args.command == "demo":
        result = demo(args.output)
        print(json.dumps(demo_summary(args.output, result), indent=2, sort_keys=True))
    elif args.command == "run":
        print(json.dumps(run_partition(args.output, args.date, force=args.force), indent=2, sort_keys=True))
    elif args.command == "backfill":
        print(json.dumps(backfill(args.output, args.start, args.end, force=args.force), indent=2, sort_keys=True))
    elif args.command == "plan-backfill":
        print(json.dumps(build_backfill_plan(args.output, args.start, args.end, force=args.force), indent=2, sort_keys=True))
    elif args.command == "dashboard":
        print(json.dumps({"dashboard": str(render_dashboard(args.output, Path(args.output) / "reports" / "training_orchestration_dashboard.html"))}, indent=2, sort_keys=True))
    elif args.command == "policy-audit":
        print(json.dumps(audit_platform_policy(Path.cwd(), output_root=args.output), indent=2, sort_keys=True))
    elif args.command == "trace-report":
        print(json.dumps(build_trace_report(args.output), indent=2, sort_keys=True))
    elif args.command == "chaos-drill":
        print(json.dumps(run_chaos_drill(args.output), indent=2, sort_keys=True))
    elif args.command == "optimize-resources":
        print(json.dumps(build_resource_optimization_report(args.output), indent=2, sort_keys=True))
    elif args.command == "network-security":
        print(json.dumps(build_network_security_report(args.output), indent=2, sort_keys=True))
    elif args.command == "gitops-plan":
        print(json.dumps(build_gitops_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "dr-plan":
        print(json.dumps(build_disaster_recovery_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "governance-bundle":
        print(json.dumps(governance(args.output), indent=2, sort_keys=True))
    elif args.command == "slo-report":
        print(json.dumps(slo_report(args.output), indent=2, sort_keys=True))
    elif args.command == "cloud-plan":
        print(json.dumps(build_cloud_migration_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "supply-chain":
        print(json.dumps(build_supply_chain_evidence(args.output, project="Metaflow Airflow Training Platform", artifact_name="training-orchestration-demo-artifacts", workflow="Training Orchestration CI", namespace="mlops-training"), indent=2, sort_keys=True))
    elif args.command == "orchestration-scorecard":
        print(json.dumps(build_orchestration_scorecard(args.output, project="Metaflow Airflow Training Platform"), indent=2, sort_keys=True))
    elif args.command == "accelerator-plan":
        print(json.dumps(build_accelerator_capacity_plan(args.output, project="Metaflow Airflow Training Platform", primary_workload="partitioned training backfills and feature-heavy model families"), indent=2, sort_keys=True))
    elif args.command == "device-plan":
        print(json.dumps(build_device_allocation_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "resource-health-status":
        print(json.dumps(build_resource_health_status_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "advanced-device-sharing":
        print(json.dumps(build_advanced_device_sharing_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "admin-access-diagnostics":
        print(json.dumps(build_admin_access_diagnostic_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "inplace-resize-plan":
        print(json.dumps(build_inplace_resize_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "topology-plan":
        print(json.dumps(build_topology_placement_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "kuberay-plan":
        print(json.dumps(build_kuberay_capacity_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "inference-gateway-plan":
        print(json.dumps(build_inference_gateway_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "semantic-telemetry-plan":
        print(json.dumps(build_semantic_telemetry_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "deadline-alerts-plan":
        print(json.dumps(build_deadline_alert_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "cost-observability":
        print(json.dumps(build_cost_observability_report(args.output), indent=2, sort_keys=True))
    elif args.command == "elastic-workload-plan":
        print(json.dumps(build_elastic_workload_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "indexed-job-resilience":
        print(json.dumps(build_indexed_job_resilience_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "provisioning-admission":
        print(json.dumps(build_provisioning_admission_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "multikueue-dispatch":
        print(json.dumps(build_multikueue_dispatch_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "dag-bundle-plan":
        print(json.dumps(build_dag_bundle_versioning_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "asset-partitioning-plan":
        print(json.dumps(build_asset_partitioning_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "airflow-stateful-orchestration":
        print(json.dumps(build_airflow_stateful_orchestration_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "multi-team-readiness":
        print(json.dumps(build_multi_team_readiness_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "event-driven-assets":
        print(json.dumps(build_event_driven_assets_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "pod-resource-envelopes":
        print(json.dumps(build_pod_resource_envelope_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "cohort-fair-sharing":
        print(json.dumps(build_cohort_fair_sharing_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "flavor-fungibility":
        print(json.dumps(build_flavor_fungibility_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "pending-workload-visibility":
        print(json.dumps(build_pending_workload_visibility_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "tenancy-report":
        print(json.dumps(build_tenancy_report(args.output), indent=2, sort_keys=True))
    elif args.command == "identity-report":
        print(json.dumps(build_identity_access_report(args.output), indent=2, sort_keys=True))
    elif args.command == "performance-budget":
        print(json.dumps(build_performance_budget_report(args.output), indent=2, sort_keys=True))
    elif args.command == "queue-simulation":
        print(json.dumps(build_queue_simulation(args.output), indent=2, sort_keys=True))
    elif args.command == "workload-aware-scheduling":
        print(json.dumps(build_workload_aware_scheduling_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "runtime-security":
        print(json.dumps(build_runtime_security_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "control-plane-diagnostics":
        print(json.dumps(build_control_plane_diagnostics_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "memory-qos":
        print(json.dumps(build_memory_qos_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "hpa-scale-zero":
        print(json.dumps(build_hpa_scale_to_zero_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "suspended-job-resources":
        print(json.dumps(build_suspended_job_resource_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "constrained-impersonation":
        print(json.dumps(build_constrained_impersonation_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "oci-artifact-volumes":
        print(json.dumps(build_oci_artifact_volume_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "checkpoint-training-readiness":
        print(json.dumps(build_checkpoint_training_readiness_plan(args.output), indent=2, sort_keys=True))
    elif args.command == "release-admission":
        print(json.dumps(build_release_admission_decision(args.output), indent=2, sort_keys=True))
    return 0
