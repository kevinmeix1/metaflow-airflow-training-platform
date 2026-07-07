# SLO And Error Budget Automation

The training platform writes `reports/slo_error_budget.json` from partition run history.

It tracks:

- partition training success
- failed partition recovery
- lineage catalog freshness
- backfill capacity admission
- multi-window burn-rate policy
- bulk-backfill freeze recommendations

Run it locally:

```bash
make demo
make slo-report
```

`kubernetes/slo-alerts.yaml` contains PrometheusRule examples for training failure burn and unrecovered partitions, plus a scheduled freeze-sync job for Airflow backfill policy.
