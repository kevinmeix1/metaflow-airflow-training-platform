from __future__ import annotations

import html
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
    recent = [
        {
            "date": row.get("ds"),
            "task": row.get("task"),
            "status": status_badge(str(row.get("status"))),
            "run": row.get("run_id", "")[:8],
            "time": row.get("timestamp", "")[:19],
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
                "model": details.get("model_version"),
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
        .panel {{ padding:16px; margin-top:16px; }}
        table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
        th, td {{ border-bottom:1px solid #e8edf3; padding:11px 12px; text-align:left; font-size:14px; overflow-wrap:anywhere; vertical-align:top; }}
        th {{ background:#f8fafc; color:#334155; }}
        tr:last-child td {{ border-bottom:0; }}
        .badge {{ display:inline-block; border-radius:999px; padding:4px 10px; font-size:12px; font-weight:800; }}
        .metric .badge {{ width:auto; max-width:max-content; }}
        .pass {{ color:#166534; background:#dcfce7; }}
        .fail {{ color:#991b1b; background:#fee2e2; }}
        .neutral {{ color:#334155; background:#e2e8f0; }}
        .summary {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }}
        .summary div {{ border:1px solid #e3e9f0; border-radius:6px; padding:12px; min-height:74px; }}
        .summary span {{ display:block; color:#64748b; font-size:12px; margin-bottom:8px; }}
        .summary strong {{ display:block; font-size:18px; overflow-wrap:anywhere; }}
        @media (max-width:900px) {{ header {{ padding:22px 18px; }} main {{ padding:18px; }} .layout {{ grid-template-columns:1fr; }} }}
      </style>
    </head>
    <body>
      <header>
        <h1>Metaflow + Airflow Training Platform</h1>
        <p>Partitioned backfills, retryable task runs, MLflow-style tracking, asset lineage, and training health observability.</p>
      </header>
      <main>
        <section class="grid">
          <div class="metric"><span>Successful partitions</span><strong>{len(successes)}</strong></div>
          <div class="metric"><span>Failed runs</span><strong>{len(failures)}</strong></div>
          <div class="metric"><span>Latest partition</span><strong>{esc(successes[-1]['ds'] if successes else 'none')}</strong></div>
          <div class="metric"><span>Latest health</span><strong>{badge(latest_healthy)}</strong></div>
        </section>
        <section class="layout">
          <div>
            <div class="panel">
              <h2>Model Runs</h2>
              <table><tr><th>Date</th><th>Model</th><th>Gates</th><th>RMSE</th><th>MAPE</th></tr>{rows(model_rows, ['date', 'model', 'gates', 'rmse', 'mape'])}</table>
            </div>
            <div class="panel">
              <h2>Recent Orchestration Events</h2>
              <table><tr><th>Date</th><th>Task</th><th>Status</th><th>Run</th><th>Time</th></tr>{rows(recent, ['date', 'task', 'status', 'run', 'time'])}</table>
            </div>
          </div>
          <div>
            <div class="panel">
              <h2>Asset Catalog</h2>
              <div class="summary">
                <div><span>Assets</span><strong>{len(assets)}</strong></div>
                <div><span>Lineage edges</span><strong>{sum(len(v) for v in lineage.values())}</strong></div>
                <div><span>Latest model partition</span><strong>{esc(assets.get('daily_demand_model', {}).get('latest_successful_partition'))}</strong></div>
                <div><span>Failed DAG runs</span><strong>{esc(assets.get('airflow_training_dag', {}).get('failed_runs', 0))}</strong></div>
              </div>
            </div>
            <div class="panel">
              <h2>Lineage</h2>
              <table><tr><th>Asset</th><th>Downstream</th></tr>{rows([{'asset': k, 'downstream': ', '.join(v)} for k, v in lineage.items()], ['asset', 'downstream'])}</table>
            </div>
          </div>
        </section>
      </main>
    </body>
    </html>
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(body, encoding="utf-8")
    return output_path
