# Airflow Deadline Alerts

`make deadline-alerts-plan` writes `.local/reports/deadline_alert_plan.json`.

## What It Shows

- Airflow 3-style Deadline Alert policies for Dag queue time, backfill runtime, and failed partition recovery.
- A migration stance away from legacy Airflow 2 SLA callbacks.
- Callback execution timeout configuration so stuck notifiers cannot block alert handling.
- Explicit callback contracts with dedupe keys, bounded payload fields, owners, and allowed side effects.
- Queue-to-start remediation that checks scheduler capacity, Airflow pools, and Kueue pending workloads.
- Failed-partition remediation that forces replay and attaches lineage context.

## Callback Contract

Deadline callbacks are not hidden training launchers. The generated plan records the receiver, owner, retry policy, dedupe key, payload fields, and allowed side effect for each callback. A callback can notify, page, open or update an incident, or request a guarded replay task. It must not directly start training, widen a backfill, or publish a model.

## Production Notes

Deadline Alerts should guard the time thresholds an operator actually cares about: "did this Dag start after it was queued?", "is this backfill wave taking too long?", and "did the failed partition recover quickly?". Those alerts should route to concrete capacity and recovery actions rather than generic task failure emails.
