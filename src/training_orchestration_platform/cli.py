from __future__ import annotations

import argparse
import json
from pathlib import Path

from .artifact_index import render_artifact_index
from .capacity_planner import build_backfill_plan
from .chaos import run_chaos_drill
from .cloud_migration import build_cloud_migration_plan
from .dashboard import render_dashboard
from .disaster_recovery import build_disaster_recovery_plan
from .gitops_release import build_gitops_plan
from .governance import build_governance_bundle
from .network_security import build_network_security_report
from .orchestrator import backfill, run_partition
from .orchestration_scorecard import build_orchestration_scorecard
from .policy_audit import audit_platform_policy
from .resource_optimizer import build_resource_optimization_report
from .slo import build_slo_report
from .supply_chain import build_supply_chain_evidence
from .traceability import build_trace_report


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
    dashboard = render_dashboard(root, root / "reports" / "training_orchestration_dashboard.html")
    artifact_index = render_artifact_index(
        root,
        title="Metaflow Airflow Training Platform",
        description="Reviewer landing page for generated training dashboard, lineage, backfill evidence, SLOs, and migration artifacts.",
        dashboard="training_orchestration_dashboard.html",
    )
    orchestration_scorecard = build_orchestration_scorecard(root, project="Metaflow Airflow Training Platform")
    supply_chain = build_supply_chain_evidence(
        root,
        project="Metaflow Airflow Training Platform",
        artifact_name="training-orchestration-demo-artifacts",
        workflow="Training Orchestration CI",
        namespace="mlops-training",
    )
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
        "dashboard": str(dashboard),
        "artifact_index": str(artifact_index),
        "orchestration_scorecard": orchestration_scorecard,
        "supply_chain": supply_chain,
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
    args = parser.parse_args(argv)
    if args.command == "demo":
        print(json.dumps(demo(args.output), indent=2, sort_keys=True))
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
    return 0
