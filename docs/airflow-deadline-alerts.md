# Airflow Deadline Alerts

`make deadline-alerts-plan` writes `.local/reports/deadline_alert_plan.json`.

## What It Shows

- Airflow 3-style Deadline Alert policies for Dag queue time, backfill runtime, and failed partition recovery.
- A migration stance away from legacy Airflow 2 SLA callbacks.
- Callback execution timeout configuration so stuck notifiers cannot block alert handling.
- Queue-to-start remediation that checks scheduler capacity, Airflow pools, and Kueue pending workloads.
- Failed-partition remediation that forces replay and attaches lineage context.

## Production Notes

Deadline Alerts should guard the time thresholds an operator actually cares about: "did this Dag start after it was queued?", "is this backfill wave taking too long?", and "did the failed partition recover quickly?". Those alerts should route to concrete capacity and recovery actions rather than generic task failure emails.
