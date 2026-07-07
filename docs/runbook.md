# Training Orchestration Runbook

## Backfill Skips A Partition

Expected when the partition already has a successful pipeline run. Use `--force` only when you intentionally want to replace artifacts.

```bash
PYTHONPATH=src python3 -m training_orchestration_platform run --output .local --date 2026-06-03 --force
```

## Training Task Fails

Symptoms:

- run history contains `task = pipeline` and `status = failed`
- dashboard failed run count increases

Actions:

1. Inspect `.local/orchestration/run_history.jsonl`.
2. Identify the task that failed before the pipeline failure.
3. Fix the underlying issue.
4. Rerun the partition with `--force`.
5. Confirm the dashboard latest health is passing.

## Data Validation Fails

Actions:

1. Inspect `.local/reports/validation_<date>.json`.
2. Check required columns, numeric parsing, and SKU domain.
3. Correct the upstream partition or quarantine it.
4. Rerun only the affected date.

## Model Gate Fails

Actions:

1. Inspect `.local/reports/metrics_<date>.json`.
2. Check RMSE, MAPE, and SKU-level MAE.
3. Compare with prior successful partitions.
4. Decide whether the issue is data drift, demand shock, or model weakness.
5. Avoid promotion or downstream publish until gates pass.

