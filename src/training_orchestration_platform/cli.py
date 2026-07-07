from __future__ import annotations

import argparse
import json
from pathlib import Path

from .dashboard import render_dashboard
from .orchestrator import backfill, run_partition


def demo(output: str | Path) -> dict:
    root = Path(output)
    first = backfill(root, "2026-06-01", "2026-06-05")
    skipped = backfill(root, "2026-06-03", "2026-06-05")
    failure = backfill(root, "2026-06-06", "2026-06-06", fail_date="2026-06-06")
    recovery = run_partition(root, "2026-06-06", force=True)
    dashboard = render_dashboard(root, root / "reports" / "training_orchestration_dashboard.html")
    return {
        "initial_backfill": first,
        "idempotent_backfill": skipped,
        "failure_drill": failure,
        "recovery": recovery,
        "dashboard": str(dashboard),
    }


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
    dashboard_parser = sub.add_parser("dashboard")
    dashboard_parser.add_argument("--output", default=".local")
    args = parser.parse_args(argv)
    if args.command == "demo":
        print(json.dumps(demo(args.output), indent=2, sort_keys=True))
    elif args.command == "run":
        print(json.dumps(run_partition(args.output, args.date, force=args.force), indent=2, sort_keys=True))
    elif args.command == "backfill":
        print(json.dumps(backfill(args.output, args.start, args.end, force=args.force), indent=2, sort_keys=True))
    elif args.command == "dashboard":
        print(json.dumps({"dashboard": str(render_dashboard(args.output, Path(args.output) / "reports" / "training_orchestration_dashboard.html"))}, indent=2, sort_keys=True))
    return 0
