from __future__ import annotations

import html
from pathlib import Path


def _escape(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _breakable(value: object) -> str:
    escaped = _escape(value)
    return escaped.replace("/", "/<wbr>").replace("_", "_<wbr>").replace("-", "-<wbr>")


def render_artifact_index(root: str | Path, *, title: str, description: str, dashboard: str) -> Path:
    root = Path(root)
    cards = [
        ("Training Dashboard", dashboard, "HTML operations view for partition state, retries, failures, and recovery evidence."),
        ("Backfill Summary", "backfill_summary.json", "Partition-level backfill result with idempotency, failure, and recovery outcomes."),
        ("Lineage Trace", "traceability_report.json", "Asset, run, artifact, and Kubernetes workload lineage for reproducibility review."),
        ("Governance Evidence", "governance_evidence_bundle.json", "Dataset card, training approval record, risk register, and reproducibility hashes."),
        ("SLO Error Budget", "slo_error_budget.json", "Training freshness, success-rate, and queue-health SLO burn-rate evidence."),
        ("Supply Chain Evidence", "supply_chain_evidence.json", "Artifact hashes, GitHub attestations, SLSA provenance, and Sigstore policy controls."),
        ("Cloud Migration Plan", "cloud_migration_plan.json", "Migration notes for MWAA, EKS, Karpenter, MLflow, and managed data platforms."),
        ("Accelerator Plan", "accelerator_capacity_plan.json", "GPU, DRA, Kueue, MIG, and time-slicing plan for accelerator-aware training."),
        ("Device Allocation", "device_allocation_plan.json", "DRA ResourceClaim templates, Kueue coupling, partition fallbacks, and device-health guardrails."),
        ("DRA Resource Health", "resource_health_status_plan.json", "Kubernetes v1.36 ResourceHealthStatus, ResourceClaim device status, device quarantine, and partition replay policy."),
        ("Advanced Device Sharing", "advanced_device_sharing_plan.json", "DRA prioritized alternatives, partitionable devices, consumable capacity, and binding-condition readiness for training."),
        ("AdminAccess Diagnostics", "admin_access_diagnostics_plan.json", "Kubernetes v1.36 DRA AdminAccess diagnostics for backfills, HPO, Airflow map indexes, Metaflow runs, and MLflow lineage."),
        ("In-Place Resize", "inplace_resize_plan.json", "Kubernetes in-place Pod resize, pod-level resource resizing, partition lineage preservation, VPA InPlaceOrRecreate, and resize-status alerts."),
        ("Topology Placement", "topology_placement_plan.json", "Kueue TAS, rack-aware distributed backfills, Airflow HA spread, and wave-splitting fallbacks."),
        ("KubeRay Capacity", "kuberay_capacity_plan.json", "Elastic Ray backfill waves, Kueue queueing, GPU worker bounds, and Metaflow recovery fallbacks."),
        ("Inference Gateway", "inference_gateway_plan.json", "Promoted champion InferencePool, endpoint picker fallback, route priorities, and serving handoff evidence."),
        ("Semantic Telemetry", "semantic_telemetry_plan.json", "Airflow, Kueue, Metaflow, MLflow, OpenLineage, partition, and Kubernetes attributes with row redaction."),
        ("Deadline Alerts", "deadline_alert_plan.json", "Airflow 3 queue, runtime, and failed-partition deadline policies with bounded callbacks."),
        ("Cost Observability", "cost_observability_report.json", "OpenCost partition cost, backfill budgets, GPU training spend, retry-storm cost, and PVC artifact allocation."),
        ("Elastic Workloads", "elastic_workload_plan.json", "Kueue Workload Slices, JobSet elastic backfills, replacement slices, and quota-safe recovery."),
        ("Indexed Job Resilience", "indexed_job_resilience_plan.json", "Kubernetes Indexed Jobs, per-index retries, success policy, pod failure policy, and bounded Airflow backfills."),
        ("Provisioning Admission", "provisioning_admission_plan.json", "Kueue ProvisioningRequest capacity checks, autoscaler retry strategy, node targeting, and fallback queueing."),
        ("MultiKueue Dispatch", "multikueue_dispatch_plan.json", "Kueue MultiKueue manager and worker dispatch, quota alignment, status sync, and worker failover evidence."),
        ("OCI Artifact Volumes", "oci_artifact_volume_plan.json", "Kubernetes image-volume training artifacts, digest-pinned bundles, read-only mounts, warmups, and fallback controls."),
        ("DAG Bundle Versioning", "dag_bundle_versioning_plan.json", "Airflow 3 GitDagBundle versioning for partition replay, scheduler-managed backfills, and Metaflow lineage."),
        ("Asset Partitioning", "asset_partitioning_plan.json", "Airflow 3.2 partitioned assets for dataset snapshots, Metaflow runs, evaluation gates, and model-registration lineage."),
        ("Multi-Team Readiness", "multi_team_readiness_plan.json", "Airflow multi-team preview readiness for training-owned DAG Bundles, pools, triggerers, secrets, executors, and asset filtering."),
        ("Event-Driven Assets", "event_driven_assets_plan.json", "Airflow 3 AssetWatchers for raw sales arrival, partition manifests, failed replay, and candidate model events."),
        ("Pod Resource Envelopes", "pod_resource_envelope_plan.json", "Kubernetes pod-level resources, scheduling gates, manifest readiness, artifact-volume readiness, and scheduler-churn observability."),
        ("Cohort Fair Sharing", "cohort_fair_sharing_plan.json", "Kueue Fair Sharing, Admission Fair Sharing, training queue weights, borrowing/lending limits, and preemption guardrails."),
        ("Flavor Fungibility", "flavor_fungibility_plan.json", "Kueue ResourceFlavor fallback, TryNextFlavor policies, explicit borrowing/preemption preference, and training pool trade-offs."),
        ("Pending Workload Visibility", "pending_workload_visibility_plan.json", "Kueue VisibilityOnDemand, raw pendingworkloads API paths, backfill queue triage, and admission-wait alerts."),
        ("Performance Budget", "performance_budget.json", "Backfill throughput, wave packing, queue wait, and recovery gates with owner actions."),
        ("Queue Simulation", "queue_simulation.json", "Kueue quota, indexed backfill priority, GPU, Airflow pool, and preemption simulation."),
        ("Release Admission", "release_admission_decision.json", "Fail-closed backfill admission record combining SLOs, capacity, queues, governance, and provenance."),
        ("Tenant Fairness", "tenancy_fairness_report.json", "Training tenant quotas, Kueue cohorts, Airflow pools, recovery reservations, and chargeback labels."),
        ("Workload Identity", "identity_access_report.json", "Keyless identities for Airflow schedulers, Metaflow partition workers, and MLflow registration."),
        ("Resource Optimization", "resource_optimization.json", "Requests, limits, VPA, HPA, and Kueue recommendations for partitioned training."),
        ("Network Security", "network_security.json", "mTLS, network policy, and Airflow-to-storage access topology for training workloads."),
        ("Chaos Drill", "chaos_drill_report.json", "Backfill failure-injection scenarios with blast radius and recovery objectives."),
        ("GitOps Plan", "gitops_plan.json", "Promotion waves, smoke backfill gates, rollback commands, and GitOps controls."),
        ("Orchestration Scorecard", "orchestration_scorecard.json", "Automated scan of advanced Airflow, Kubernetes, lineage, and security controls."),
    ]
    card_html = "\n".join(
        f"""
        <a class="card" href="{_escape(href)}">
          <span class="label">{_escape(label)}</span>
          <strong>{_breakable(href)}</strong>
          <small>{_escape(summary)}</small>
        </a>"""
        for label, href, summary in cards
    )
    body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_escape(title)} Evidence Index</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fb;
      --panel: #ffffff;
      --ink: #182031;
      --muted: #5e6877;
      --line: #dce2ea;
      --accent: #6f4a12;
      --accent-soft: #f5eddc;
      --shadow: 0 18px 45px rgba(37, 45, 63, 0.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
      line-height: 1.55;
    }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 48px 24px 56px; }}
    header {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 28px;
      align-items: end;
      padding-bottom: 28px;
      border-bottom: 1px solid var(--line);
    }}
    h1 {{ margin: 0 0 10px; font-size: clamp(2rem, 4vw, 4rem); line-height: 1; letter-spacing: 0; }}
    p {{ margin: 0; color: var(--muted); max-width: 760px; }}
    .badge {{
      display: inline-flex;
      align-items: center;
      min-height: 36px;
      padding: 0 14px;
      border: 1px solid #dcc58e;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 0.82rem;
      font-weight: 800;
      text-transform: uppercase;
    }}
    .grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin-top: 28px; }}
    .card {{
      display: flex;
      min-height: 178px;
      flex-direction: column;
      justify-content: space-between;
      gap: 18px;
      padding: 22px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: var(--shadow);
      color: inherit;
      text-decoration: none;
    }}
    .card:hover {{ border-color: #c8a75b; transform: translateY(-1px); }}
    .label {{ color: var(--accent); font-size: 0.78rem; font-weight: 800; text-transform: uppercase; }}
    strong {{ font-size: 0.96rem; line-height: 1.3; overflow-wrap: break-word; }}
    small {{ color: var(--muted); font-size: 0.9rem; }}
    footer {{ margin-top: 28px; color: var(--muted); font-size: 0.9rem; }}
    @media (max-width: 880px) {{
      header {{ grid-template-columns: 1fr; }}
      .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>{_escape(title)}</h1>
        <p>{_escape(description)}</p>
      </div>
      <span class="badge">Demo Evidence</span>
    </header>
    <section class="grid" aria-label="Generated artifacts">
      {card_html}
    </section>
    <footer>Generated by the local demo command. Open the dashboard first, then inspect the JSON evidence behind orchestration and backfill decisions.</footer>
  </main>
</body>
</html>
"""
    output = root / "reports" / "index.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(body, encoding="utf-8")
    return output
