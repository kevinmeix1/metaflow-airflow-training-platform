# Operations Review Runbook

## Start the console

```bash
make clean
make demo
python3 -m http.server 8093 --bind 127.0.0.1 --directory .local/reports
```

Open `http://127.0.0.1:8093/training_orchestration_dashboard.html`.

The dashboard includes a **Run Review** panel organized around the ownership
boundary, backfill capacity, failed-partition
recovery, and Kubernetes migration.

## Five-minute story

1. Establish the Airflow and Metaflow ownership boundary and point to the generated health evidence.
2. Lower CPU per wave from 6 to 2 in the Backfill Capacity Lab and show one wave becoming three.
3. Explain priority ordering, skipped completed partitions, and why the browser planner mirrors the Python policy.
4. Inspect the executable Metaflow run: version, four-way fan-out, cards, winning candidate, and registration key.
5. Walk through the forced publish failure, bounded attempts, eight cloned upstream tasks, and two fresh resume steps.
6. Connect the local proof to Airflow 3.3 assets, Kubernetes Jobs, Kueue, remote artifacts, MLflow, and telemetry.

## Generate narration and video

```bash
python3.11 -m venv .demo-venv
.demo-venv/bin/pip install -e '.[demo]'
make demo-voice PYTHON=.demo-venv/bin/python
make demo-video
```

The natural neural voice is generated with `edge-tts`. The resulting video is
`docs/demo/training-judge-demo.mp4`. Keep this media environment separate from
the pinned Metaflow runtime so `make verify-metaflow-lock` remains exact.
Offline voice engines such as Piper or Kokoro can be substituted, but
`edge-tts` gives a natural operations-review voice without committing model weights.
