from __future__ import annotations

import html
import json
from pathlib import Path

from .io import read_json, read_jsonl
from .orchestrator import run_log_path


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value))


def badge(value: bool) -> str:
    return f'<span class="badge {"pass" if value else "fail"}">{"PASS" if value else "FAIL"}</span>'


def status_badge(status: str) -> str:
    if status == "success":
        return '<span class="badge pass">PASS</span>'
    if status == "failed":
        return '<span class="badge fail">FAIL</span>'
    return f'<span class="badge neutral">{esc(status.upper())}</span>'


def partition_chips(value: object) -> str:
    if not isinstance(value, list):
        return esc(value)
    visible = value[-3:]
    chips = [f'<span class="chip">{esc(item)}</span>' for item in visible]
    if len(value) > len(visible):
        chips.insert(0, f'<span class="chip muted">+{len(value) - len(visible)} earlier</span>')
    return "".join(chips) or '<span class="chip muted">none</span>'


TASK_LABELS = {
    "extract_partition": "extract",
    "validate_partition": "validate",
    "metaflow_train": "train",
    "pipeline": "pipeline",
}


def compact_label(value: object) -> str:
    text = "" if value is None else str(value)
    if text in TASK_LABELS:
        display = TASK_LABELS[text]
    elif text.startswith("demand-2026-"):
        display = f"demand v{text[-5:]}"
    elif text.endswith("-queue"):
        display = text[:-6].replace("-", " ")
    else:
        display = text.replace("_", " ")
    return f'<span class="nowrap" title="{esc(text)}">{esc(display)}</span>'


def identifier_label(value: object, *, width: int = 12) -> str:
    text = "" if value is None else str(value)
    display = text if len(text) <= width else f"{text[:width]}..."
    return f'<span class="nowrap" title="{esc(text)}">{esc(display)}</span>'


def asset_label(value: object) -> str:
    text = "" if value is None else str(value)
    display = text.replace("_", " ").replace("-", " ")
    return f'<span class="asset-label" title="{esc(text)}">{esc(display)}</span>'


def time_label(value: object) -> str:
    text = "" if value is None else str(value)
    display = text[11:16] if len(text) >= 16 else text
    return f'<span class="nowrap" title="{esc(text)}">{esc(display)}</span>'


def rows(items: list[dict], columns: list[str]) -> str:
    if not items:
        return f"<tr><td colspan='{len(columns)}'>No records</td></tr>"
    rendered = []
    for item in items:
        cells = []
        for column in columns:
            value = item.get(column, "")
            if isinstance(value, str) and value.startswith("<span class="):
                cells.append(f"<td>{value}</td>")
            else:
                cells.append(f"<td>{esc(value)}</td>")
        rendered.append("<tr>" + "".join(cells) + "</tr>")
    return "\n".join(rendered)


def render_dashboard(root: str | Path, output_path: str | Path) -> Path:
    root = Path(root)
    runs = read_jsonl(run_log_path(root))
    pipelines = [row for row in runs if row.get("task") == "pipeline"]
    successes = [row for row in pipelines if row.get("status") == "success"]
    failures = [row for row in pipelines if row.get("status") == "failed"]
    latest_healthy = bool(successes) and (not failures or successes[-1].get("timestamp", "") > failures[-1].get("timestamp", ""))
    assets = read_json(root / "orchestration" / "asset_catalog.json") if (root / "orchestration" / "asset_catalog.json").exists() else {}
    lineage = read_json(root / "orchestration" / "lineage.json") if (root / "orchestration" / "lineage.json").exists() else {}
    capacity_plan = read_json(root / "reports" / "backfill_capacity_plan.json") if (root / "reports" / "backfill_capacity_plan.json").exists() else {}
    runtime_contract = read_json(root / "metaflow" / "latest.json") if (root / "metaflow" / "latest.json").exists() else {}
    runtime_verification = read_json(root / "metaflow" / "verification.json") if (root / "metaflow" / "verification.json").exists() else {}
    resume_verification = read_json(root / "metaflow" / "resume_verification.json") if (root / "metaflow" / "resume_verification.json").exists() else {}
    event_assets = read_json(root / "reports" / "event_driven_assets_plan.json") if (root / "reports" / "event_driven_assets_plan.json").exists() else {}
    checkpoint_training = read_json(root / "reports" / "checkpoint_training_readiness_plan.json") if (root / "reports" / "checkpoint_training_readiness_plan.json").exists() else {}
    watcher_dedupe = event_assets.get("ha_watcher_dedupe_simulation", {})
    checkpoint_capacity = checkpoint_training.get("capacity", {})
    checkpoint_windows = checkpoint_training.get("resume_windows", [])
    checkpoint_observability = checkpoint_training.get("observability", {})
    checkpoint_jobs = checkpoint_training.get("training_jobs", [])
    checkpoint_payload = json.dumps(
        {"jobs": checkpoint_jobs, "windows": checkpoint_windows},
        separators=(",", ":"),
    ).replace("</", "<\\/")
    runtime_verified = bool(runtime_verification.get("passed")) and runtime_verification.get("run_id") == runtime_contract.get("metaflow_run_id")
    resume_status = "success" if resume_verification.get("passed") else ("failed" if resume_verification else "not run")
    planner_workloads = [
        workload
        for wave in capacity_plan.get("waves", [])
        for workload in wave.get("workloads", [])
    ]
    planner_payload = json.dumps(planner_workloads, separators=(",", ":")).replace("</", "<\\/")
    recent = [
        {
                "date": row.get("ds"),
                "task": compact_label(row.get("task")),
                "status": status_badge(str(row.get("status"))),
                "run": compact_label(row.get("run_id", "")[:6]),
                "time": time_label(row.get("timestamp", "")),
        }
        for row in runs[-18:]
    ]
    model_rows = []
    for row in successes[-8:]:
        details = row.get("details", {})
        metrics = details.get("metrics", {})
        gates = details.get("gates", {})
        model_rows.append(
            {
                "date": row.get("ds"),
                "model": compact_label(details.get("model_version")),
                "engine": compact_label(details.get("engine", "local")),
                "gates": badge(gates.get("passed", False)),
                "rmse": metrics.get("rmse"),
                "mape": metrics.get("mape"),
            }
        )
    body = f"""
    <!doctype html>
    <html lang="en">
    <head>
      <title>Training Orchestration Platform</title>
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <style>
        * {{ box-sizing: border-box; }}
        body {{ margin: 0; background: #f5f7fa; color: #1c2733; font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
        header {{ background: #172026; color: white; padding: 28px 36px; border-bottom: 5px solid #22c55e; }}
        main {{ max-width: 1460px; margin: 0 auto; padding: 24px 36px 42px; }}
        h1 {{ margin: 0; font-size: 28px; line-height: 1.2; }}
        h2 {{ margin: 0 0 14px; font-size: 17px; }}
        header p {{ margin: 8px 0 0; color: #cbd5df; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 14px; margin-bottom: 18px; }}
        .metric, .panel {{ background: #fff; border: 1px solid #d7dee7; border-radius: 8px; box-shadow: 0 1px 2px rgba(23,32,38,.04); }}
        .metric {{ min-height: 112px; padding: 16px; }}
        .metric span {{ display:block; color:#5b6b7d; font-size:13px; margin-bottom:10px; }}
        .metric strong {{ display:block; font-size:24px; line-height:1.2; overflow-wrap:anywhere; }}
        .layout {{ display:grid; grid-template-columns:minmax(0,1fr) minmax(360px,.42fr); gap:16px; align-items:start; }}
        .layout > div, .panel {{ min-width:0; }}
        .panel {{ padding:16px; margin-top:16px; }}
        table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
        th, td {{ border-bottom:1px solid #e8edf3; padding:11px 12px; text-align:left; font-size:14px; overflow-wrap:anywhere; vertical-align:top; }}
        th {{ background:#f8fafc; color:#334155; }}
        tr:last-child td {{ border-bottom:0; }}
        .table-wrap {{ width:100%; max-width:100%; overflow-x:auto; border:1px solid #e8edf3; }}
        table {{ min-width:620px; }}
        .model-runs {{ min-width:760px; }}
        .model-runs col:nth-child(1) {{ width:17%; }}
        .model-runs col:nth-child(2) {{ width:25%; }}
        .model-runs col:nth-child(3) {{ width:16%; }}
        .model-runs col:nth-child(4) {{ width:14%; }}
        .events col:nth-child(1) {{ width:20%; }}
        .events col:nth-child(2) {{ width:28%; }}
        .events col:nth-child(3) {{ width:20%; }}
        .events col:nth-child(4) {{ width:16%; }}
        .events col:nth-child(5) {{ width:16%; }}
        .badge {{ display:inline-block; border-radius:999px; padding:4px 10px; font-size:12px; font-weight:800; }}
        .metric .badge {{ width:auto; max-width:max-content; }}
        .pass {{ color:#166534; background:#dcfce7; }}
        .fail {{ color:#991b1b; background:#fee2e2; }}
        .neutral {{ color:#334155; background:#e2e8f0; }}
        .chip {{ display:inline-block; margin:0 5px 5px 0; padding:4px 8px; border-radius:999px; background:#ecfdf5; color:#166534; font-size:12px; font-weight:800; white-space:nowrap; }}
        .chip.muted {{ background:#f1f5f9; color:#475569; }}
        .nowrap {{ display:inline-block; max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; vertical-align:bottom; }}
        .facts {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); min-width:0; border-top:1px solid #e3e9f0; }}
        .fact {{ min-width:0; padding:13px 10px 13px 0; min-height:72px; border-bottom:1px solid #e3e9f0; }}
        .fact:nth-child(even) {{ padding-left:14px; border-left:1px solid #e3e9f0; }}
        .fact > span {{ display:block; color:#64748b; font-size:12px; margin-bottom:7px; }}
        .fact strong {{ display:block; min-width:0; font-size:17px; overflow-wrap:anywhere; }}
        .fact .nowrap {{ display:block; width:100%; }}
        .planner {{ border-left:4px solid #15803d; margin-bottom:18px; }}
        .planner-heading {{ display:flex; align-items:flex-start; justify-content:space-between; gap:18px; margin-bottom:16px; }}
        .planner-heading p {{ margin:5px 0 0; color:#64748b; font-size:13px; }}
        .planner-grid {{ display:grid; grid-template-columns:minmax(0,.75fr) minmax(0,1.25fr); gap:20px; align-items:start; }}
        .controls {{ display:grid; gap:14px; }}
        .control-row {{ display:grid; grid-template-columns:150px minmax(0,1fr) 72px; gap:10px; align-items:center; }}
        .control-row label {{ color:#475569; font-size:12px; font-weight:700; }}
        .control-row input {{ width:100%; accent-color:#15803d; }}
        .control-value {{ color:#0f172a; background:#f1f5f9; border-radius:5px; padding:6px 8px; font-size:12px; font-weight:800; text-align:center; }}
        .planner-kpis {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); border:1px solid #e3e9f0; border-radius:6px; overflow:hidden; margin-bottom:14px; }}
        .planner-kpis div {{ padding:11px; min-height:66px; background:#f8fafc; border-right:1px solid #e3e9f0; }}
        .planner-kpis div:last-child {{ border-right:0; }}
        .planner-kpis span {{ display:block; color:#64748b; font-size:11px; margin-bottom:7px; }}
        .planner-kpis strong {{ display:block; font-size:16px; }}
        .wave-list {{ display:grid; gap:8px; max-height:268px; overflow:auto; padding-right:4px; }}
        .wave-row {{ display:grid; grid-template-columns:64px minmax(0,1fr) 150px; gap:10px; align-items:center; padding:9px 10px; border:1px solid #e3e9f0; border-radius:6px; background:white; }}
        .wave-name {{ font-size:12px; font-weight:800; color:#166534; }}
        .wave-workloads {{ display:flex; flex-wrap:wrap; gap:5px; }}
        .wave-chip {{ padding:3px 6px; border-radius:4px; background:#ecfdf5; color:#166534; font-size:11px; white-space:nowrap; }}
        .wave-resources {{ color:#475569; font-size:11px; text-align:right; }}
        .checkpoint-lab {{ border-left:4px solid #2563eb; margin-bottom:18px; }}
        .checkpoint-top {{ display:grid; grid-template-columns:minmax(0,1fr) 260px; gap:18px; align-items:start; margin-bottom:16px; }}
        .checkpoint-top p {{ margin:5px 0 0; color:#64748b; font-size:13px; }}
        .select-wrap label {{ display:block; color:#475569; font-size:12px; font-weight:800; margin-bottom:6px; }}
        .select-wrap select {{ width:100%; min-height:38px; border:1px solid #cbd5e1; border-radius:6px; padding:8px 10px; background:#fff; color:#0f172a; font:inherit; }}
        .checkpoint-grid {{ display:grid; grid-template-columns:minmax(0,.92fr) minmax(0,1.08fr); gap:18px; align-items:start; }}
        .checkpoint-facts {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); border:1px solid #e3e9f0; border-radius:6px; overflow:hidden; }}
        .checkpoint-facts div {{ padding:12px; min-height:74px; background:#f8fafc; border-right:1px solid #e3e9f0; border-bottom:1px solid #e3e9f0; }}
        .checkpoint-facts div:nth-child(2n) {{ border-right:0; }}
        .checkpoint-facts div:nth-last-child(-n+2) {{ border-bottom:0; }}
        .checkpoint-facts span {{ display:block; color:#64748b; font-size:11px; margin-bottom:7px; }}
        .checkpoint-facts strong {{ display:block; font-size:16px; overflow-wrap:anywhere; }}
        .timeline {{ display:grid; gap:12px; }}
        .timeline-row {{ display:grid; grid-template-columns:150px minmax(0,1fr) 74px; gap:12px; align-items:center; }}
        .timeline-label {{ color:#475569; font-size:12px; font-weight:800; }}
        .timeline-track {{ height:13px; border-radius:999px; background:#e2e8f0; overflow:hidden; }}
        .timeline-fill {{ height:100%; border-radius:999px; background:#2563eb; }}
        .timeline-fill.sla {{ background:#22c55e; }}
        .timeline-value {{ color:#0f172a; font-size:12px; font-weight:800; text-align:right; }}
        .timeline-note {{ margin:12px 0 0; color:#64748b; font-size:12px; }}
        @media (max-width:900px) {{ header {{ padding:22px 18px; }} main {{ padding:18px; }} .layout,.planner-grid {{ grid-template-columns:1fr; }} }}
        @media (max-width:760px) {{ .checkpoint-top,.checkpoint-grid {{ grid-template-columns:1fr; }} }}
        @media (max-width:540px) {{ .facts,.checkpoint-facts {{ grid-template-columns:1fr; }} .fact:nth-child(even) {{ padding-left:0; border-left:0; }} .checkpoint-facts div {{ border-right:0; }} .checkpoint-facts div:nth-last-child(-n+2) {{ border-bottom:1px solid #e3e9f0; }} .checkpoint-facts div:last-child {{ border-bottom:0; }} .timeline-row {{ grid-template-columns:1fr; gap:6px; }} .timeline-value {{ text-align:left; }} }}
        @media (max-width:620px) {{ .planner-heading {{ flex-direction:column; }} .planner-kpis {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .planner-kpis div:nth-child(2) {{ border-right:0; }} .planner-kpis div:nth-child(-n+2) {{ border-bottom:1px solid #e3e9f0; }} .control-row {{ grid-template-columns:110px minmax(0,1fr) 60px; }} .wave-row {{ grid-template-columns:52px minmax(0,1fr); }} .wave-resources {{ grid-column:2; text-align:left; }} }}
      </style>
    </head>
    <body>
      <header>
        <h1>Metaflow + Airflow Training Platform</h1>
        <p>Partitioned training, bounded candidate fan-out, evaluation gates, recovery evidence, and orchestration health.</p>
      </header>
      <main>
        <section class="grid">
          <div class="metric"><span>Successful partitions</span><strong>{len(successes)}</strong></div>
          <div class="metric"><span>Failed runs</span><strong>{len(failures)}</strong></div>
          <div class="metric"><span>Latest partition</span><strong>{esc(successes[-1]['ds'] if successes else 'none')}</strong></div>
          <div class="metric"><span>Latest health</span><strong>{badge(latest_healthy)}</strong></div>
          <div class="metric"><span>Metaflow runtime</span><strong>{badge(runtime_verified)}</strong></div>
          <div class="metric"><span>Recovery drill</span><strong>{status_badge(resume_status)}</strong></div>
        </section>
        <section class="panel planner" data-testid="backfill-capacity-lab">
          <div class="planner-heading">
            <div><h2>Backfill Capacity Lab</h2><p>Repack the generated partition workloads under different queue budgets.</p></div>
            <span class="badge neutral">first-fit, priority ordered</span>
          </div>
          <div class="planner-grid">
            <div class="controls">
              <div class="control-row"><label for="maxCpu">CPU per wave</label><input id="maxCpu" type="range" min="2" max="12" step="0.5" value="6"><output id="maxCpuValue" class="control-value">6 CPU</output></div>
              <div class="control-row"><label for="maxMemory">Memory per wave</label><input id="maxMemory" type="range" min="4" max="24" step="1" value="10"><output id="maxMemoryValue" class="control-value">10 GiB</output></div>
              <div class="control-row"><label for="maxParallelism">Parallel workloads</label><input id="maxParallelism" type="range" min="1" max="8" step="1" value="4"><output id="maxParallelismValue" class="control-value">4 pods</output></div>
              <p class="planner-note">Critical model families stay ahead of baseline workloads. Completed partitions remain excluded unless the backfill is forced.</p>
            </div>
            <div>
              <div class="planner-kpis" aria-live="polite">
                <div><span>Workloads</span><strong id="plannerWorkloads">{len(planner_workloads)}</strong></div>
                <div><span>Waves</span><strong id="plannerWaves">{esc(capacity_plan.get('wave_count', 0))}</strong></div>
                <div><span>Peak CPU</span><strong id="plannerCpu">n/a</strong></div>
                <div><span>Peak memory</span><strong id="plannerMemory">n/a</strong></div>
              </div>
              <div id="waveList" class="wave-list"></div>
            </div>
          </div>
        </section>
        <section class="panel checkpoint-lab" data-testid="checkpoint-recovery-timeline">
          <div class="checkpoint-top">
            <div>
              <h2>Checkpoint Recovery Timeline</h2>
              <p>Inspect restore time, checkpoint write volume, queue priority, and SLA margin for each distributed training profile.</p>
            </div>
            <div class="select-wrap">
              <label for="checkpointJob">Training profile</label>
              <select id="checkpointJob"></select>
            </div>
          </div>
          <div class="checkpoint-grid">
            <div class="checkpoint-facts" aria-live="polite">
              <div><span>Framework</span><strong id="checkpointFramework">n/a</strong></div>
              <div><span>Queue</span><strong id="checkpointQueue">n/a</strong></div>
              <div><span>Replica groups</span><strong id="checkpointReplicas">n/a</strong></div>
              <div><span>Priority</span><strong id="checkpointPriority">n/a</strong></div>
              <div><span>Checkpoint scope</span><strong id="checkpointScope">n/a</strong></div>
              <div><span>Recovery decision</span><strong id="checkpointDecision">n/a</strong></div>
            </div>
            <div>
              <div class="timeline">
                <div class="timeline-row"><span class="timeline-label">Restore estimate</span><div class="timeline-track"><div id="restoreFill" class="timeline-fill"></div></div><span id="restoreValue" class="timeline-value">n/a</span></div>
                <div class="timeline-row"><span class="timeline-label">Resume SLA</span><div class="timeline-track"><div id="slaFill" class="timeline-fill sla"></div></div><span id="slaValue" class="timeline-value">n/a</span></div>
                <div class="timeline-row"><span class="timeline-label">Checkpoint write</span><div class="timeline-track"><div id="writeFill" class="timeline-fill"></div></div><span id="writeValue" class="timeline-value">n/a</span></div>
              </div>
              <p id="checkpointNote" class="timeline-note">Select a training profile to inspect checkpoint recovery evidence.</p>
            </div>
          </div>
        </section>
        <section class="layout">
          <div>
            <div class="panel">
              <h2>Model Runs</h2>
              <div class="table-wrap"><table class="model-runs"><colgroup><col><col><col><col><col><col></colgroup><tr><th>Date</th><th>Model</th><th>Engine</th><th>Gates</th><th>RMSE</th><th>MAPE</th></tr>{rows(model_rows, ['date', 'model', 'engine', 'gates', 'rmse', 'mape'])}</table></div>
            </div>
            <div class="panel">
              <h2>Recent Orchestration Events</h2>
              <div class="table-wrap"><table class="events"><colgroup><col><col><col><col><col></colgroup><tr><th>Date</th><th>Task</th><th>Status</th><th>Run</th><th>Time</th></tr>{rows(recent, ['date', 'task', 'status', 'run', 'time'])}</table></div>
            </div>
          </div>
          <div>
            <div class="panel">
              <h2>Executable Metaflow Run</h2>
              <div class="facts">
                <div class="fact"><span>Runtime</span><strong>{esc(runtime_verification.get('metaflow_version', 'not run'))}</strong></div>
                <div class="fact"><span>Run contract</span><strong>{badge(runtime_verified)}</strong></div>
                <div class="fact"><span>Candidate fan-out</span><strong>{esc(runtime_contract.get('candidate_count', 0))} / {esc(runtime_contract.get('passing_candidate_count', 0))} passing</strong></div>
                <div class="fact"><span>Rendered cards</span><strong>{esc(runtime_verification.get('card_count', 0))}</strong></div>
                <div class="fact"><span>Selected candidate</span><strong>{compact_label(runtime_contract.get('selected_candidate', 'none'))}</strong></div>
                <div class="fact"><span>Registration key</span><strong>{compact_label(runtime_contract.get('registration_idempotency_key', 'none'))}</strong></div>
              </div>
            </div>
            <div class="panel">
              <h2>Failure Recovery Drill</h2>
              <div class="facts">
                <div class="fact"><span>Status</span><strong>{status_badge(resume_status)}</strong></div>
                <div class="fact"><span>Failed boundary</span><strong>{compact_label(resume_verification.get('failed_step', 'not run'))}</strong></div>
                <div class="fact"><span>Publish attempts</span><strong>{partition_chips(resume_verification.get('failed_publish_attempts', []))}</strong></div>
                <div class="fact"><span>Cloned upstream tasks</span><strong>{esc(resume_verification.get('cloned_task_count', 'n/a'))}</strong></div>
                <div class="fact"><span>Origin run</span><strong>{identifier_label(resume_verification.get('failed_run_id', 'not run'))}</strong></div>
                <div class="fact"><span>Resumed run</span><strong>{identifier_label(resume_verification.get('resumed_run_id', 'not run'))}</strong></div>
                <div class="fact"><span>Fresh steps</span><strong>{partition_chips(resume_verification.get('fresh_steps', []))}</strong></div>
                <div class="fact"><span>Candidate</span><strong>{compact_label(resume_verification.get('selected_candidate', 'not run'))}</strong></div>
              </div>
            </div>
            <div class="panel">
              <h2>Checkpointed Distributed Training</h2>
              <div class="facts">
                <div class="fact"><span>Readiness</span><strong>{badge(bool(checkpoint_training.get('passed')))}</strong></div>
                <div class="fact"><span>Resume SLA pass</span><strong>{esc(sum(1 for item in checkpoint_windows if item.get('passed')))}/{esc(len(checkpoint_windows))}</strong></div>
                <div class="fact"><span>GPU equivalent</span><strong>{esc(checkpoint_capacity.get('total_gpu_equivalent', 'n/a'))}</strong></div>
                <div class="fact"><span>Protected queue</span><strong>{compact_label(checkpoint_capacity.get('protected_queue', 'not planned'))}</strong></div>
                <div class="fact"><span>Preemptible queue</span><strong>{compact_label(checkpoint_capacity.get('preemptible_queue', 'not planned'))}</strong></div>
                <div class="fact"><span>Metaflow fields</span><strong>{partition_chips(checkpoint_observability.get('metaflow', []))}</strong></div>
              </div>
            </div>
            <div class="panel">
              <h2>Backfill Capacity Plan</h2>
              <div class="facts">
                <div class="fact"><span>Workloads</span><strong>{esc(capacity_plan.get('workload_count', 'n/a'))}</strong></div>
                <div class="fact"><span>Waves</span><strong>{esc(capacity_plan.get('wave_count', 'n/a'))}</strong></div>
                <div class="fact"><span>Queue</span><strong>{compact_label(capacity_plan.get('queue', 'n/a'))}</strong></div>
                <div class="fact"><span>Skipped partitions</span><strong>{partition_chips(capacity_plan.get('skipped_partitions', []))}</strong></div>
              </div>
            </div>
            <div class="panel">
              <h2>Asset Catalog</h2>
              <div class="facts">
                <div class="fact"><span>Assets</span><strong>{len(assets)}</strong></div>
                <div class="fact"><span>Lineage edges</span><strong>{sum(len(v) for v in lineage.values())}</strong></div>
                <div class="fact"><span>Latest model partition</span><strong>{esc(assets.get('daily_demand_model', {}).get('latest_successful_partition'))}</strong></div>
                <div class="fact"><span>Failed DAG runs</span><strong>{esc(assets.get('airflow_training_dag', {}).get('failed_runs', 0))}</strong></div>
              </div>
            </div>
            <div class="panel">
              <h2>Event Watcher Dedupe</h2>
              <div class="facts">
                <div class="fact"><span>Triggerers</span><strong>{esc(watcher_dedupe.get('triggerer_count', 0))}</strong></div>
                <div class="fact"><span>Input events</span><strong>{esc(watcher_dedupe.get('input_events', 0))}</strong></div>
                <div class="fact"><span>Accepted events</span><strong>{esc(watcher_dedupe.get('accepted_events', 0))}</strong></div>
                <div class="fact"><span>Suppressed duplicates</span><strong>{esc(watcher_dedupe.get('suppressed_duplicates', 0))}</strong></div>
                <div class="fact"><span>Dedupe store</span><strong>{compact_label(watcher_dedupe.get('dedupe_store', 'not planned'))}</strong></div>
                <div class="fact"><span>Contract</span><strong>{badge(bool(watcher_dedupe.get('passed')))}</strong></div>
              </div>
            </div>
            <div class="panel">
              <h2>Lineage</h2>
              <div class="table-wrap"><table><tr><th>Asset</th><th>Downstream</th></tr>{rows([{'asset': asset_label(k), 'downstream': asset_label(', '.join(v))} for k, v in lineage.items()], ['asset', 'downstream'])}</table></div>
            </div>
          </div>
        </section>
      </main>
      <script>
        const plannerWorkloads = {planner_payload};
        const checkpointData = {checkpoint_payload};
        const plannerById = (id) => document.getElementById(id);

        function packWaves(workloads, maxCpu, maxMemory, maxParallelism) {{
          const waves = [];
          let current = [];
          let cpu = 0;
          let memory = 0;
          workloads.forEach((workload) => {{
            const exceeds = current.length >= maxParallelism || cpu + workload.cpu > maxCpu || memory + workload.memory_gib > maxMemory;
            if (current.length && exceeds) {{
              waves.push({{workloads: current, cpu, memory}});
              current = [];
              cpu = 0;
              memory = 0;
            }}
            current.push(workload);
            cpu += workload.cpu;
            memory += workload.memory_gib;
          }});
          if (current.length) waves.push({{workloads: current, cpu, memory}});
          return waves;
        }}

        function renderPlanner() {{
          const maxCpu = Number(plannerById("maxCpu").value);
          const maxMemory = Number(plannerById("maxMemory").value);
          const maxParallelism = Number(plannerById("maxParallelism").value);
          plannerById("maxCpuValue").textContent = maxCpu + " CPU";
          plannerById("maxMemoryValue").textContent = maxMemory + " GiB";
          plannerById("maxParallelismValue").textContent = maxParallelism + " pods";
          const waves = packWaves(plannerWorkloads, maxCpu, maxMemory, maxParallelism);
          plannerById("plannerWaves").textContent = waves.length;
          plannerById("plannerCpu").textContent = Math.max(0, ...waves.map((wave) => wave.cpu)).toFixed(1);
          plannerById("plannerMemory").textContent = Math.max(0, ...waves.map((wave) => wave.memory)).toFixed(1) + " GiB";
          const list = plannerById("waveList");
          list.replaceChildren();
          waves.forEach((wave, index) => {{
            const row = document.createElement("div");
            row.className = "wave-row";
            const name = document.createElement("span");
            name.className = "wave-name";
            name.textContent = "Wave " + (index + 1);
            const items = document.createElement("div");
            items.className = "wave-workloads";
            wave.workloads.forEach((workload) => {{
              const chip = document.createElement("span");
              chip.className = "wave-chip";
              chip.textContent = workload.partition.slice(5) + " / " + workload.model_family.replaceAll("_", " ");
              items.appendChild(chip);
            }});
            const resources = document.createElement("span");
            resources.className = "wave-resources";
            resources.textContent = wave.cpu.toFixed(1) + " CPU / " + wave.memory.toFixed(1) + " GiB";
            row.append(name, items, resources);
            list.appendChild(row);
          }});
        }}

        ["maxCpu", "maxMemory", "maxParallelism"].forEach((id) => plannerById(id).addEventListener("input", renderPlanner));
        renderPlanner();

        function formatReplicaGroups(groups) {{
          return Object.entries(groups || {{}}).map(([name, count]) => name + " " + count).join(" / ") || "n/a";
        }}

        function selectedCheckpointWindow(jobName) {{
          return checkpointData.windows.find((item) => item.job === jobName) || {{}};
        }}

        function setWidth(id, value, max) {{
          const percent = max > 0 ? Math.max(4, Math.min(100, (value / max) * 100)) : 0;
          plannerById(id).style.width = percent + "%";
        }}

        function renderCheckpointTimeline() {{
          const select = plannerById("checkpointJob");
          const job = checkpointData.jobs.find((item) => item.name === select.value) || checkpointData.jobs[0] || {{}};
          const window = selectedCheckpointWindow(job.name);
          const restore = Number(window.estimated_restore_minutes || 0);
          const sla = Number(window.resume_sla_minutes || 0);
          const write = Number(window.checkpoint_write_gib || 0);
          const maxTime = Math.max(restore, sla, 1);
          plannerById("checkpointFramework").textContent = job.framework || "n/a";
          plannerById("checkpointQueue").textContent = job.queue || "n/a";
          plannerById("checkpointReplicas").textContent = formatReplicaGroups(job.replica_groups);
          plannerById("checkpointPriority").textContent = job.priority || "n/a";
          plannerById("checkpointScope").textContent = job.checkpoint_scope || "n/a";
          plannerById("checkpointDecision").textContent = window.passed ? "Resume inside SLA" : "Hold registration";
          setWidth("restoreFill", restore, maxTime);
          setWidth("slaFill", sla, maxTime);
          setWidth("writeFill", write, 6);
          plannerById("restoreValue").textContent = restore.toFixed(1) + " min";
          plannerById("slaValue").textContent = sla.toFixed(1) + " min";
          plannerById("writeValue").textContent = write.toFixed(2) + " GiB";
          plannerById("checkpointNote").textContent = window.passed
            ? "Safe to resume: Airflow partition identity and Metaflow checkpoint scope stay stable."
            : "Hold model registration until restore time and checkpoint integrity return inside policy.";
        }}

        function initializeCheckpointTimeline() {{
          const select = plannerById("checkpointJob");
          checkpointData.jobs.forEach((job) => {{
            const option = document.createElement("option");
            option.value = job.name;
            option.textContent = job.name.replaceAll("-", " ");
            select.appendChild(option);
          }});
          select.addEventListener("change", renderCheckpointTimeline);
          renderCheckpointTimeline();
        }}

        initializeCheckpointTimeline();
      </script>
    </body>
    </html>
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(body, encoding="utf-8")
    return output_path
